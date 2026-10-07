package io.github.ariellaub.protobus;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.internal.Headers;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The caller's side of RPC: publishes requests and routes their replies, unary
 * and streaming, back to the call that is waiting for them.
 */
public class MessageDispatcher {
    /** AMQP carries {@code message-id} as a shortstr: one length byte, so 255 at most. */
    static final int MAX_MESSAGE_ID_BYTES = 255;

    private final Connection connection;
    private final CallbackListener callbackListener;
    private final Map<String, Pending> callbacks = new ConcurrentHashMap<>();
    /**
     * correlationId → in-flight streaming reply. Distinct from {@code callbacks},
     * so a streaming reply cannot resolve an unrelated unary call.
     */
    private final Map<String, StreamCall> pendingStreams = new ConcurrentHashMap<>();
    /** Bytes buffered across every pending stream: a bound on the process, not one call. */
    private final AtomicLong totalBufferedBytes = new AtomicLong();
    private final Object channelLock = new Object();
    private volatile AmqpChannel channel;
    private volatile boolean initialized;
    private final Runnable detachRestorer;
    private final Runnable detachDisconnected;

    private static final class Pending {
        final CompletableFuture<byte[]> reply = new CompletableFuture<>();
        volatile ScheduledFuture<?> timer;
    }

    public MessageDispatcher(Connection connection) {
        this.connection = connection;
        this.callbackListener = new CallbackListener(connection);
        this.detachDisconnected = connection.onDisconnected(this::onDisconnected);
        this.detachRestorer = connection.registerRestorer(gen -> restore());
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void init() {
        if (initialized) return;
        openPublishChannel();
        callbackListener.init(this::onResult, null);
        // A reply queue rebuilt on a live connection is a new, server-named queue:
        // requests in flight name the old one, so their replies will never come.
        callbackListener.onQueueReplaced(() -> failPending(new DisconnectedError(
                "the reply queue was replaced while the call was pending")));
        callbackListener.start();
        initialized = true;
    }

    /**
     * Open the publishing channel and declare the exchanges this dispatcher
     * publishes to, so a client works against a broker no service has touched yet.
     */
    private AmqpChannel openPublishChannel() {
        AmqpChannel ch = connection.openChannel();
        connection.declareExchange(ch, Config.busExchangeName(), "topic");
        connection.declareExchange(ch, Config.cancelExchangeName(), "fanout");
        synchronized (channelLock) {
            channel = ch;
        }
        return ch;
    }

    /**
     * The publishing channel, reopened when it was lost on a live connection (a
     * channel error closes only that channel).
     */
    private AmqpChannel publishChannel() {
        AmqpChannel ch = channel;
        if (ch != null && ch.isOpen()) return ch;
        synchronized (channelLock) {
            ch = channel;
            if (ch != null && ch.isOpen()) return ch;
            if (!connection.isReady()) return ch;
            Logger.warn("MessageDispatcher: publishing channel lost on a live connection; reopening it");
            return openPublishChannel();
        }
    }

    private void restore() {
        if (!initialized) return;
        Logger.info("MessageDispatcher: reconnected, re-initializing channel");
        openPublishChannel();
        Logger.info("MessageDispatcher: successfully re-initialized after reconnection");
    }

    /** Fail every pending call and stream: their replies went with the connection. */
    private void onDisconnected() {
        Logger.debug("MessageDispatcher: connection lost, rejecting pending callbacks");
        synchronized (channelLock) {
            channel = null;
        }
        failPending(new DisconnectedError());
    }

    private void failPending(DisconnectedError error) {
        for (Map.Entry<String, Pending> e : new ArrayList<>(callbacks.entrySet())) {
            if (callbacks.remove(e.getKey(), e.getValue())) {
                ScheduledFuture<?> t = e.getValue().timer;
                if (t != null) t.cancel(false);
                completeOff(e.getValue().reply, null, error);
            }
        }
        for (StreamCall s : new ArrayList<>(pendingStreams.values())) s.fail(error);
    }

    /** Off the transport's thread, so a caller's continuation cannot stall reply delivery. */
    private <T> void completeOff(CompletableFuture<T> f, T value, Throwable error) {
        try {
            connection.internalExecutor().execute(() -> {
                if (error == null) f.complete(value);
                else f.completeExceptionally(error);
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            if (error == null) f.complete(value);
            else f.completeExceptionally(error);
        }
    }

    /** A reply, on the reply queue's delivery thread. */
    private Connection.HandlerResult onResult(byte[] content, String id, Connection.MessageHandlerContext context) {
        StreamCall stream = pendingStreams.get(id);
        if (stream != null) {
            stream.onChunk(content, context.headers);
            return Connection.HandlerResult.none();
        }
        Pending pending = callbacks.remove(id);
        // The deadline stays armed: it belongs to the caller's result, which may
        // still be waiting for the request's confirm.
        if (pending != null) completeOff(pending.reply, content, null);
        return Connection.HandlerResult.none();
    }

    static Integer validatePriority(CallOptions options) {
        return Priority.validatePriority(options.priority());
    }

    /**
     * A caller-supplied messageId, or null to have one minted. Blank is refused
     * rather than treated as absent: an id derived from a field that came out empty
     * would give every attempt a different identity, and no deduplication at all.
     */
    static String validateMessageId(String messageId) {
        if (messageId == null) return null;
        if (messageId.trim().isEmpty()) {
            throw new InvalidMessageIdError("messageId must be a non-empty string, got \"" + messageId
                    + "\". Leave it unset to have one generated.");
        }
        int bytes = messageId.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_MESSAGE_ID_BYTES) {
            throw new InvalidMessageIdError("messageId is " + bytes + " bytes; AMQP carries message-id as a "
                    + "shortstr, so it must be at most " + MAX_MESSAGE_ID_BYTES
                    + ". Hash a long key rather than concatenating it.");
        }
        return messageId;
    }

    /**
     * Publish a request and wait for its reply (or, with {@code rpc} false, for the
     * broker's confirm). The deadline, {@link CallOptions#timeoutMs()} or
     * {@link Config#rpcCallTimeoutMs()}, starts once the connection is ready and
     * bounds the confirm as well as the reply. When the publish fails, its error
     * wins over the deadline: "the request never left" is the more specific answer.
     *
     * @return the raw reply, or null when {@code rpc} is false
     */
    public CompletableFuture<byte[]> publishAsync(byte[] content, String routingKey, CallOptions options) {
        CallOptions o = options == null ? CallOptions.DEFAULT : options;
        Integer priority;
        String callerMessageId;
        try {
            priority = validatePriority(o);
            callerMessageId = validateMessageId(o.messageId());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        // A reconnection is waited through; anything else with no connection is a
        // caller error, reported at once.
        if (!connection.isConnected() && !connection.isReconnecting()) {
            return CompletableFuture.failedFuture(new NotConnectedError());
        }
        return connection.whenReadyAsync().thenCompose(v -> send(content, routingKey, o, priority, callerMessageId));
    }

    public byte[] publish(byte[] content, String routingKey, CallOptions options) {
        return Connection.join(publishAsync(content, routingKey, options));
    }

    private CompletableFuture<byte[]> send(byte[] content, String routingKey, CallOptions o, Integer priority,
                                           String callerMessageId) {
        boolean rpc = o.rpc();
        String id = UUID.randomUUID().toString();
        BasicProperties.Builder props = new BasicProperties.Builder()
                .contentType("application/octet-stream")
                .correlationId(id)
                .deliveryMode(2);
        if (rpc) props.replyTo(callbackListener.callbackQueue());
        // Set only when asked for, so a publish with no priority carries none.
        if (priority != null) props.priority(priority);
        if (callerMessageId != null) props.messageId(callerMessageId);
        AmqpChannel ch;
        try {
            ch = publishChannel();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        // An RPC request that routes nowhere means no service is bound to the key:
        // worth learning now, as UnroutableError, rather than after the timeout.
        // Not for one-way publishes, where no consumer may be normal.
        Connection.PublishOptions publish = new Connection.PublishOptions(props.build(), rpc);
        if (!rpc) {
            CompletableFuture<byte[]> done = new CompletableFuture<>();
            connection.publishAsync(ch, Config.busExchangeName(), routingKey, content, publish)
                    .whenComplete((mid, err) -> completeOff(done, null, err));
            return done;
        }

        long limit = o.timeoutMs() != null ? o.timeoutMs() : Config.rpcCallTimeoutMs();
        // Armed BEFORE publishing: a fast service can reply while the confirm is
        // still in flight.
        Pending pending = new Pending();
        callbacks.put(id, pending);
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        // The deadline is the caller's: it ends the call whatever is still
        // outstanding, the reply or the request's own confirm.
        pending.timer = connection.scheduler().schedule(() -> {
            callbacks.remove(id, pending);
            completeOff(result, null, new RpcTimeoutError(
                    "no reply for " + routingKey + " (correlationId " + id + ") within " + limit + "ms"));
        }, limit, TimeUnit.MILLISECONDS);
        result.whenComplete((r, e) -> {
            pending.timer.cancel(false);
            callbacks.remove(id, pending);
        });

        java.util.concurrent.atomic.AtomicBoolean confirmed = new java.util.concurrent.atomic.AtomicBoolean();
        // The deadline bounds the confirm as well as the reply: a timeout or a
        // disconnect settles the call at once, without waiting for the confirm.
        // A reply, though, is only taken once the request is confirmed, because a
        // failed publish is the more specific answer.
        pending.reply.whenComplete((reply, replyErr) -> {
            if (replyErr != null) result.completeExceptionally(replyErr);
            else if (confirmed.get()) result.complete(reply);
        });
        connection.publishAsync(ch, Config.busExchangeName(), routingKey, content, publish).whenComplete((mid, err) -> {
            if (err != null) {
                // The request never made it, so no reply is coming: release the
                // slot now and surface the publish failure.
                completeOff(result, null, err);
                return;
            }
            confirmed.set(true);
            // Off the transport's thread: the reply may already be here, and the
            // caller's continuations must never run on the connection's I/O thread.
            if (pending.reply.isDone()) {
                pending.reply.whenComplete((reply, replyErr) -> {
                    if (replyErr == null) completeOff(result, reply, null);
                });
            }
        });
        return result;
    }

    /**
     * Publish a request expecting a streaming reply. Returns at once; the request
     * is published as soon as the connection is ready, and a failure to publish
     * surfaces from the stream's first {@code next()}.
     *
     * Closing the stream early releases its buffer and tells the producer to stop.
     * That notice is best effort and cooperative: a producer that ignores its signal
     * runs to completion, and its chunks are dropped here.
     */
    public ChunkStream publishStreaming(byte[] content, String routingKey, StreamOptions options) {
        StreamOptions o = options == null ? StreamOptions.DEFAULT : options;
        if (!connection.isConnected() && !connection.isReconnecting()) throw new NotConnectedError();
        String id = UUID.randomUUID().toString();
        long idleMs = o.idleTimeoutMs() != null ? o.idleTimeoutMs() : Config.streamIdleTimeoutMs();
        StreamCall stream = new StreamCall(id, idleMs);
        AbortSignal signal = o.signal();
        if (signal != null && signal.aborted()) {
            // Aborted before it began: nothing to send and nothing to wait for.
            stream.endQuietly();
            stream.published = CompletableFuture.completedFuture(null);
            return new ChunkStream(stream);
        }
        pendingStreams.put(id, stream);
        if (signal != null) {
            long listener = signal.addListener(() -> stream.cancel(false));
            stream.releaseSignal = () -> signal.removeListener(listener);
        }
        stream.armIdle();
        BasicProperties props = new BasicProperties.Builder()
                .contentType("application/octet-stream")
                .correlationId(id)
                .replyTo(callbackListener.callbackQueue())
                .deliveryMode(2)
                .build();
        // The channel is read after readiness: a reconnection replaces it.
        stream.published = connection.whenReadyAsync()
                .thenCompose(v -> connection.publishAsync(publishChannel(), Config.busExchangeName(), routingKey,
                        content, Connection.PublishOptions.of(props)))
                .handle((mid, err) -> {
                    if (err != null) {
                        stream.fail(err instanceof java.util.concurrent.CompletionException && err.getCause() != null
                                ? err.getCause() : err);
                    }
                    return null;
                });
        return new ChunkStream(stream);
    }

    int pendingStreamCount() {
        return pendingStreams.size();
    }

    public void close() {
        detachDisconnected.run();
        detachRestorer.run();
        if (callbackListener.isInitialized()) callbackListener.close();
        AmqpChannel ch = channel;
        if (ch != null && ch.isOpen()) ch.close();
    }

    // ---- streams -----------------------------------------------------------------------

    /** Read x-protobus-seq; absent or unparseable disables validation rather than inventing a violation. */
    static Long parseSeq(Map<String, Object> headers) {
        Long n = Headers.integer(Headers.get(headers, Config.HEADER_SEQ));
        return n != null && n >= 0 ? n : null;
    }

    static boolean parseFinal(Map<String, Object> headers) {
        return Headers.bool(Headers.get(headers, Config.HEADER_FINAL));
    }

    /** One pending streaming call: chunks buffered until the caller takes them. */
    final class StreamCall {
        final String id;
        final long idleMs;
        final ArrayDeque<byte[]> chunks = new ArrayDeque<>();
        long bufferedBytes;
        /** Highest sequence accepted, or null before the first and for peers that send none. */
        Long lastSeq;
        boolean ended;
        Throwable error;
        boolean cancelled;
        ScheduledFuture<?> idle;
        Runnable releaseSignal = () -> {};
        volatile CompletableFuture<Void> published;

        StreamCall(String id, long idleMs) {
            this.id = id;
            this.idleMs = idleMs;
        }

        /**
         * Restart the idle deadline, on progress from either side: a chunk arriving,
         * or a chunk handed to the caller. Armed at call time, so a caller that never
         * iterates still releases what the call holds.
         */
        synchronized void armIdle() {
            if (idle != null) idle.cancel(false);
            if (ended) return;
            idle = connection.scheduler().schedule(this::onIdle, idleMs, TimeUnit.MILLISECONDS);
        }

        private void onIdle() {
            synchronized (this) {
                if (ended) return;
                error = new StreamTimeoutError("No streaming chunk received within " + idleMs + "ms");
                ended = true;
                notifyAll();
            }
            // The producer is still working for a caller that stopped listening.
            cancel(true);
        }

        synchronized void endQuietly() {
            ended = true;
            notifyAll();
        }

        void fail(Throwable err) {
            boolean completed;
            synchronized (this) {
                // A stream whose final chunk arrived has succeeded: what it buffered
                // is the caller's, whatever happens to the connection afterwards.
                completed = ended && error == null && !cancelled;
                if (!completed) {
                    if (error == null) error = err;
                    ended = true;
                }
                if (idle != null) idle.cancel(false);
                notifyAll();
            }
            if (completed) {
                // No more replies are coming for it; only the registration goes.
                releaseSignal.run();
                pendingStreams.remove(id, this);
                return;
            }
            // Released now, keeping the error for the caller: one that never reads
            // the stream again must not hold its entry or its signal listener.
            release();
        }

        private void releaseBuffer() {
            long bytes;
            synchronized (this) {
                bytes = bufferedBytes;
                bufferedBytes = 0;
                chunks.clear();
            }
            if (bytes > 0) totalBufferedBytes.updateAndGet(t -> Math.max(0, t - bytes));
        }

        /** Everything the call holds, released on any terminal outcome. */
        void release() {
            synchronized (this) {
                if (idle != null) idle.cancel(false);
            }
            releaseSignal.run();
            pendingStreams.remove(id, this);
            releaseBuffer();
        }

        /**
         * Stop the producer and release the call. {@code notifyOnly} is for the
         * failure paths, which have already recorded the error the caller is about
         * to see.
         */
        void cancel(boolean notifyOnly) {
            synchronized (this) {
                if (cancelled) return;
                cancelled = true;
                if (!notifyOnly) {
                    ended = true;
                    notifyAll();
                }
            }
            release();
            Logger.debug("cancelling stream " + id);
            BasicProperties props = new BasicProperties.Builder()
                    .correlationId(id)
                    .contentType("application/octet-stream")
                    .build();
            // Fire and forget: the caller has stopped waiting.
            try {
                connection.publishAsync(publishChannel(), Config.cancelExchangeName(), "", new byte[0],
                        Connection.PublishOptions.of(props)).whenComplete((mid, err) -> {
                            if (err != null) Logger.debug("failed to publish cancel for stream " + id + ": " + err);
                        });
            } catch (RuntimeException e) {
                Logger.debug("failed to publish cancel for stream " + id + ": " + e.getMessage());
            }
        }

        /** A chunk, on the reply queue's delivery thread. */
        void onChunk(byte[] body, Map<String, Object> headers) {
            boolean isFinal = parseFinal(headers);
            Long seq = parseSeq(headers);
            boolean overflow = false;
            synchronized (this) {
                if (ended) return;
                if (seq != null) {
                    long expected = lastSeq == null ? 0 : lastSeq + 1;
                    if (seq < expected) {
                        // Already seen: a redelivery, not new data.
                        Logger.debug("stream " + id + ": dropping duplicate chunk seq=" + seq);
                        if (isFinal) {
                            ended = true;
                            notifyAll();
                        }
                        return;
                    }
                    if (seq > expected) {
                        error = new StreamSequenceError("stream " + id + " lost at least one chunk: got seq=" + seq
                                + ", expected " + expected);
                        ended = true;
                        notifyAll();
                        overflow = true;
                    } else {
                        lastSeq = seq;
                    }
                }
                if (!overflow && body.length > 0) {
                    long maxChunks = Config.streamMaxBufferedChunks();
                    long maxBytes = Config.streamMaxBufferedBytes();
                    long maxTotal = Config.streamMaxTotalBufferedBytes();
                    long wouldBe = bufferedBytes + body.length;
                    long wouldBeTotal = totalBufferedBytes.get() + body.length;
                    if (chunks.size() + 1 > maxChunks || wouldBe > maxBytes || wouldBeTotal > maxTotal) {
                        error = new StreamBackpressureError("stream " + id + " exceeded a buffer limit ("
                                + (chunks.size() + 1) + " chunks / " + wouldBe + " bytes for this call, "
                                + wouldBeTotal + " bytes across all calls; limits are " + maxChunks + " chunks / "
                                + maxBytes + " bytes / " + maxTotal + " bytes total): the consumer is not keeping "
                                + "up with the producer");
                        ended = true;
                        notifyAll();
                        overflow = true;
                    } else {
                        chunks.add(body);
                        bufferedBytes = wouldBe;
                        totalBufferedBytes.addAndGet(body.length);
                    }
                }
                if (!overflow) {
                    if (isFinal) ended = true;
                    notifyAll();
                }
            }
            if (overflow) {
                releaseBuffer();
                // The producer would keep going for a stream that has failed.
                cancel(true);
            } else if (!isFinal) {
                armIdle();
            }
        }

        /**
         * The next chunk, or null at the end. Blocks; throws the stream's failure.
         * A failure to publish the request is one of those outcomes, so a reader
         * waits on all of them at once: an idle timeout or a cancel wakes it while
         * the request's confirm is still outstanding.
         */
        byte[] next() {
            Throwable failure = null;
            byte[] chunk = null;
            boolean done = false;
            synchronized (this) {
                while (true) {
                    if (error != null) {
                        failure = error;
                        done = true;
                        break;
                    }
                    chunk = chunks.poll();
                    if (chunk != null) {
                        bufferedBytes -= chunk.length;
                        long n = chunk.length;
                        totalBufferedBytes.updateAndGet(t -> Math.max(0, t - n));
                        if (!ended) armIdle();
                        break;
                    }
                    if (ended) {
                        done = true;
                        break;
                    }
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ProtobusException("interrupted while waiting for a stream chunk", null, e);
                    }
                }
            }
            // On the reader's own thread, outside the monitor: no executor is
            // involved, so a context already closed cannot get in the way.
            if (done) release();
            if (failure != null) throw rethrow(failure);
            return chunk;
        }

        synchronized boolean finished() {
            return ended && chunks.isEmpty();
        }
    }

    private static RuntimeException rethrow(Throwable e) {
        if (e instanceof RuntimeException) return (RuntimeException) e;
        if (e instanceof Error) throw (Error) e;
        return new ProtobusException(Errors.messageOf(e), null, e);
    }

    /**
     * The raw reply bodies of one streaming call. {@link #next()} blocks until a
     * chunk arrives, the stream ends (null) or it fails. Closing it before the end
     * cancels the call.
     */
    public static final class ChunkStream implements AutoCloseable {
        private final StreamCall call;

        ChunkStream(StreamCall call) {
            this.call = call;
        }

        /** The next chunk, or null once the stream has ended. */
        public byte[] next() {
            return call.next();
        }

        /** Cancel: the producer's signal fires and it stops being published. Idempotent. */
        public void cancel() {
            boolean done;
            synchronized (call) {
                done = call.ended && call.chunks.isEmpty();
            }
            if (!done) call.cancel(false);
            else call.release();
        }

        public boolean finished() {
            return call.finished();
        }

        /** Whether the stream's final chunk (or its failure) has arrived, read or not. */
        boolean ended() {
            synchronized (call) {
                return call.ended;
            }
        }

        @Override
        public void close() {
            cancel();
        }

        List<byte[]> drainForTest() {
            List<byte[]> out = new ArrayList<>();
            for (byte[] c = next(); c != null; c = next()) out.add(c);
            return out;
        }
    }
}
