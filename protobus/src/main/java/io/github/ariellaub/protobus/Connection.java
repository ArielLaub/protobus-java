package io.github.ariellaub.protobus;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.AmqpConnection;
import io.github.ariellaub.protobus.amqp.AmqpException;
import io.github.ariellaub.protobus.amqp.Delivery;
import io.github.ariellaub.protobus.amqp.RabbitTransport;
import io.github.ariellaub.protobus.amqp.Transport;
import io.github.ariellaub.protobus.internal.Headers;
import io.github.ariellaub.protobus.internal.Threads;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The broker connection every protobus component of a context shares.
 *
 * It owns the AMQP connection and its lifecycle: automatic reconnection with
 * exponential backoff, coordinated restoration of every component's topology
 * before the connection reports itself ready again, confirmed publishing, and the
 * consume loop that runs a handler and settles its delivery (reply, ack, retry,
 * dead-letter).
 *
 * Every blocking method here may be called from any thread except a transport
 * callback. Handlers run on the handler executor (see {@link ContextOptions}), up
 * to each consumer's prefetch in parallel.
 */
public final class Connection {

    // ---- public types ------------------------------------------------------------------

    /** Reconnection backoff. */
    public static final class ReconnectionOptions {
        /** Consecutive failed attempts before giving up; 0 retries forever. */
        public final int maxRetries;
        public final long initialDelayMs;
        public final long maxDelayMs;
        public final double backoffMultiplier;

        public ReconnectionOptions(int maxRetries, long initialDelayMs, long maxDelayMs, double backoffMultiplier) {
            this.maxRetries = maxRetries;
            this.initialDelayMs = initialDelayMs;
            this.maxDelayMs = maxDelayMs;
            this.backoffMultiplier = backoffMultiplier;
        }

        /** 10 attempts, from 1 s doubling up to 30 s, each with up to 30% jitter. */
        public static ReconnectionOptions defaults() {
            return new ReconnectionOptions(10, 1000, 30000, 2);
        }

        public ReconnectionOptions withMaxRetries(int value) {
            return new ReconnectionOptions(value, initialDelayMs, maxDelayMs, backoffMultiplier);
        }

        public ReconnectionOptions withInitialDelayMs(long value) {
            return new ReconnectionOptions(maxRetries, value, maxDelayMs, backoffMultiplier);
        }

        public ReconnectionOptions withMaxDelayMs(long value) {
            return new ReconnectionOptions(maxRetries, initialDelayMs, value, backoffMultiplier);
        }

        public ReconnectionOptions withBackoffMultiplier(double value) {
            return new ReconnectionOptions(maxRetries, initialDelayMs, maxDelayMs, value);
        }
    }

    /**
     * Restores one component's topology after the socket comes back: its channel,
     * queues, bindings and consumers. Receives the generation it is restoring.
     * Throws to fail the reconnection attempt.
     */
    @FunctionalInterface
    public interface Restorer {
        void restore(long generation) throws Exception;
    }

    /** Extra context handed to a message handler. */
    public static final class MessageHandlerContext {
        /**
         * Fires when the processing timeout elapses or, for a streaming reply, when
         * the caller cancels. protobus never interrupts the handler's thread, so a
         * handler doing long work should watch it.
         */
        public final AbortSignal signal;
        /** The routing key the broker actually delivered on. */
        public final String routingKey;
        /**
         * Stable across every redelivery and retry hop of one logical message, which
         * is what makes deduplication possible. Null only for a message published by
         * something that did not set it.
         */
        public final String messageId;
        /** The broker has delivered this message before. */
        public final boolean redelivered;
        /** The delivery's headers; never null. */
        public final Map<String, Object> headers;

        public MessageHandlerContext(AbortSignal signal, String routingKey, String messageId, boolean redelivered,
                                     Map<String, Object> headers) {
            this.signal = signal;
            this.routingKey = routingKey;
            this.messageId = messageId;
            this.redelivered = redelivered;
            this.headers = headers == null ? Map.of() : headers;
        }
    }

    /** Receives the chunks of a streaming reply. */
    public interface ChunkSink {
        /**
         * Publish a chunk. The previous chunk is published here, and this one is
         * held until the next arrives or the producer returns, so that the last chunk
         * can carry {@code x-protobus-final=true}. Throws
         * {@link StreamCancelledException} once the caller has cancelled.
         */
        void write(byte[] chunk);

        /** Whether the caller has cancelled. */
        boolean cancelled();
    }

    /** Produces a streaming reply into a sink. Runs on a handler thread. */
    @FunctionalInterface
    public interface StreamProducer {
        void produce(ChunkSink sink) throws Exception;
    }

    /** Thrown by {@link ChunkSink#write} after the caller cancelled, to unwind the producer. */
    public static final class StreamCancelledException extends ProtobusException {
        private static final long serialVersionUID = 1L;

        public StreamCancelledException() {
            super("the stream was cancelled by its caller", null);
        }
    }

    /**
     * What a handler returns: nothing (no reply), one encoded reply, or a stream of
     * encoded chunks, each published with {@code x-protobus-final=false} and the
     * last with {@code x-protobus-final=true}.
     */
    public static final class HandlerResult {
        final byte[] reply;
        final StreamProducer stream;

        private HandlerResult(byte[] reply, StreamProducer stream) {
            this.reply = reply;
            this.stream = stream;
        }

        private static final HandlerResult NONE = new HandlerResult(null, null);

        public static HandlerResult none() {
            return NONE;
        }

        public static HandlerResult reply(byte[] reply) {
            return new HandlerResult(reply, null);
        }

        public static HandlerResult stream(StreamProducer producer) {
            return new HandlerResult(null, producer);
        }
    }

    @FunctionalInterface
    public interface MessageHandler {
        HandlerResult handle(byte[] content, String correlationId, MessageHandlerContext context) throws Exception;
    }

    /**
     * An error carrying the reply its caller should receive on a terminal path
     * (dead-lettered, or rejected without retry). MessageService throws one for an
     * unhandled handler error, pre-encoded and sanitised; the connection settles on
     * {@link #getCause()} and replies with {@link #reply()}.
     */
    public static final class ErrorWithReply extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final transient byte[] reply;

        public ErrorWithReply(Throwable cause, byte[] reply) {
            super("protobus: handler failed", cause);
            this.reply = reply;
        }

        public byte[] reply() {
            return reply;
        }
    }

    public static final class ConsumeOptions {
        public String consumerTag = "";
        public boolean noAck;
        public boolean exclusive;
        /**
         * Handle deliveries one at a time, in arrival order, on the transport's
         * thread rather than in parallel on the handler executor. Replies use it: a
         * stream's chunks must be seen in the order the broker delivered them. The
         * handler must not block.
         */
        public boolean ordered;
        /**
         * Builds the caller's reply for a failure that carries none, such as a
         * processing timeout. Returning null leaves the caller to its own timeout.
         */
        public BiFunction<byte[], Throwable, byte[]> buildErrorReply;
        /** Called when the broker cancels the consumer. Runs on the transport's thread: must not block. */
        public Runnable onCancelled;
        /**
         * Handlers this consumer runs at once; further deliveries wait in memory.
         * 0 leaves it to the prefetch, which bounds a late-ack consumer already.
         */
        public int maxConcurrency;
    }

    public static final class ConsumeRetryOptions {
        public final int maxRetries;
        public final String retryQueueName;
        /**
         * The topic exchange the retry queue is bound to with {@code #}. A retry is
         * published here under the original routing key, which is what lets the
         * post-TTL dead-letter hop route it back to the service's queue.
         */
        public final String retryExchangeName;
        public final String dlqName;
        public final Predicate<Throwable> isHandledError;

        public ConsumeRetryOptions(int maxRetries, String retryQueueName, String retryExchangeName, String dlqName,
                                   Predicate<Throwable> isHandledError) {
            this.maxRetries = maxRetries;
            this.retryQueueName = retryQueueName;
            this.retryExchangeName = retryExchangeName;
            this.dlqName = dlqName;
            this.isHandledError = isHandledError;
        }
    }

    /** What to publish with. */
    public record PublishOptions(BasicProperties properties, boolean mandatory) {
        public static PublishOptions of(BasicProperties properties) {
            return new PublishOptions(properties, false);
        }
    }

    // ---- state -------------------------------------------------------------------------

    private final Transport transport;
    private final ExecutorService handlers;
    private final boolean ownsHandlers;
    private final ExecutorService internal = Threads.cachedPool("protobus-internal");
    private final ScheduledExecutorService timer = Threads.scheduler("protobus-timer");

    private final Object lock = new Object();
    private String url;
    private ReconnectionOptions reconnection = ReconnectionOptions.defaults();
    private AmqpConnection handle;
    private boolean connected;
    private boolean reconnecting;
    private boolean ready;
    private boolean manualDisconnect;
    private long generation;
    private int reconnectAttempts;
    private ScheduledFuture<?> reconnectTimer;
    private String abandoned;
    private final List<CompletableFuture<Void>> readyWaiters = new ArrayList<>();

    /** Single-flight connect: a manual connect() and a reconnection attempt never overlap. */
    private final ReentrantLock connectMutex = new ReentrantLock();

    private final List<Restorer> restorers = new CopyOnWriteArrayList<>();

    private final List<Runnable> reconnectedListeners = new CopyOnWriteArrayList<>();
    private final List<Runnable> disconnectedListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<Integer, Long>> reconnectingListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<Throwable>> errorListeners = new CopyOnWriteArrayList<>();

    /** Deliveries being handled, by correlationId, so an abandoned stream can be stopped. */
    private final Map<String, Set<DeliveryEntry>> activeDeliveries = new ConcurrentHashMap<>();

    private final Object drainLock = new Object();
    private long inFlight;
    private long running;

    public Connection() {
        this(null, null);
    }

    /**
     * @param transport the AMQP transport; null for RabbitMQ
     * @param handlerExecutor where handlers run; null for an internal pool of
     *     daemon threads. On Java 21+, {@code Executors.newVirtualThreadPerTaskExecutor()}
     *     suits handlers that block.
     */
    public Connection(Transport transport, ExecutorService handlerExecutor) {
        this.transport = transport == null ? new RabbitTransport() : transport;
        this.ownsHandlers = handlerExecutor == null;
        this.handlers = handlerExecutor == null ? Threads.cachedPool("protobus-worker") : handlerExecutor;
    }

    // ---- state queries -----------------------------------------------------------------

    public boolean isConnected() {
        synchronized (lock) {
            return connected;
        }
    }

    public boolean isReconnecting() {
        synchronized (lock) {
            return reconnecting;
        }
    }

    /**
     * True when the socket is up AND every restorer has finished. Between the two,
     * channels are gone and queues not yet redeclared, so this is what a publisher
     * wants.
     */
    public boolean isReady() {
        synchronized (lock) {
            return ready;
        }
    }

    Executor handlerExecutor() {
        return handlers;
    }

    Executor internalExecutor() {
        return internal;
    }

    ScheduledExecutorService scheduler() {
        return timer;
    }

    // ---- restorers and readiness ------------------------------------------------------

    /**
     * Register topology to restore on reconnection. Restorers run in registration
     * order, and the connection reports itself reconnected only once they have all
     * finished; one that throws fails the whole attempt, which is retried. A
     * half-restored listener beside a connection claiming to be healthy is the worst
     * of both.
     *
     * @return a function that unregisters the restorer
     */
    public Runnable registerRestorer(Restorer restorer) {
        // Wrapped, so the same function registered twice is removed one at a time.
        Restorer entry = restorer::restore;
        restorers.add(entry);
        return () -> restorers.remove(entry);
    }

    /**
     * Wait until the connection carries traffic again.
     *
     * @throws NotReadyError when it is closed or gives up first, or after
     *     {@code timeoutMs}
     */
    public void whenReady(long timeoutMs) {
        join(whenReadyAsync(timeoutMs));
    }

    public void whenReady() {
        whenReady(Config.connectionReadyTimeoutMs());
    }

    public CompletableFuture<Void> whenReadyAsync() {
        return whenReadyAsync(Config.connectionReadyTimeoutMs());
    }

    public CompletableFuture<Void> whenReadyAsync(long timeoutMs) {
        CompletableFuture<Void> waiter = new CompletableFuture<>();
        synchronized (lock) {
            if (ready) return CompletableFuture.completedFuture(null);
            if (manualDisconnect) return CompletableFuture.failedFuture(new NotReadyError("the connection has been closed"));
            if (abandoned != null) return CompletableFuture.failedFuture(new NotReadyError(abandoned));
            readyWaiters.add(waiter);
        }
        ScheduledFuture<?> deadline = timer.schedule(() -> {
            synchronized (lock) {
                readyWaiters.remove(waiter);
            }
            waiter.completeExceptionally(
                    new NotReadyError("the connection did not become ready within " + timeoutMs + "ms"));
        }, timeoutMs, TimeUnit.MILLISECONDS);
        waiter.whenComplete((v, e) -> deadline.cancel(false));
        return waiter;
    }

    private void releaseWaiters(List<CompletableFuture<Void>> waiters, Throwable error) {
        for (CompletableFuture<Void> w : waiters) {
            if (error == null) runInternal(() -> w.complete(null));
            else runInternal(() -> w.completeExceptionally(error));
        }
    }

    // ---- events ------------------------------------------------------------------------

    /** Returns a function that removes the listener. */
    public Runnable onReconnecting(BiConsumer<Integer, Long> listener) {
        reconnectingListeners.add(listener);
        return () -> reconnectingListeners.remove(listener);
    }

    public Runnable onReconnected(Runnable listener) {
        reconnectedListeners.add(listener);
        return () -> reconnectedListeners.remove(listener);
    }

    /**
     * The connection went down: lost (a reconnection follows) or closed by
     * {@link #disconnect()}. Runs on a transport thread or the disconnecting
     * caller's: must not block.
     */
    public Runnable onDisconnected(Runnable listener) {
        disconnectedListeners.add(listener);
        return () -> disconnectedListeners.remove(listener);
    }

    public Runnable onError(Consumer<Throwable> listener) {
        errorListeners.add(listener);
        return () -> errorListeners.remove(listener);
    }

    private static void safely(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            Logger.error("connection listener failed: " + e);
        }
    }

    private void emitDisconnected() {
        for (Runnable l : disconnectedListeners) safely(l);
    }

    private void emitReconnected() {
        for (Runnable l : reconnectedListeners) safely(l);
    }

    private void emitReconnecting(int attempt, long delay) {
        for (BiConsumer<Integer, Long> l : reconnectingListeners) safely(() -> l.accept(attempt, delay));
    }

    private void emitError(Throwable error) {
        for (Consumer<Throwable> l : errorListeners) safely(() -> l.accept(error));
    }

    // ---- lifecycle ---------------------------------------------------------------------

    public void connect(String url) {
        connect(url, null);
    }

    /**
     * Connect.
     *
     * @throws AlreadyConnectedError when connected
     * @throws AmqpException when the broker cannot be reached
     */
    public void connect(String url, ReconnectionOptions options) {
        synchronized (lock) {
            if (connected) throw new AlreadyConnectedError();
            this.url = url;
            this.reconnection = options == null ? ReconnectionOptions.defaults() : options;
            this.manualDisconnect = false;
            this.abandoned = null;
        }
        doConnect();
        // Nothing to restore on a first connect: components initialise themselves
        // against it, so the socket coming up is readiness, unless it has already
        // gone again, in which case the reconnection it scheduled owns the state.
        List<CompletableFuture<Void>> waiters;
        synchronized (lock) {
            if (!connected) return;
            reconnecting = false;
            ready = true;
            waiters = new ArrayList<>(readyWaiters);
            readyWaiters.clear();
        }
        releaseWaiters(waiters, null);
    }

    private AmqpConnection doConnect() {
        connectMutex.lock();
        try {
            long gen;
            String target;
            synchronized (lock) {
                gen = generation;
                target = url;
            }
            Logger.info("connecting to bus - " + Logger.redactUrl(target));
            AmqpConnection h;
            try {
                h = transport.connect(target, (int) Config.heartbeatSeconds());
            } catch (RuntimeException e) {
                Logger.error("failed to connect: " + Errors.messageOf(e));
                synchronized (lock) {
                    connected = false;
                }
                throw e;
            }
            boolean stale;
            synchronized (lock) {
                // Anything that tears the connection down bumps the generation. A
                // connect completing afterwards belongs to a previous era.
                stale = gen != generation || manualDisconnect;
                if (!stale) {
                    handle = h;
                    connected = true;
                    reconnectAttempts = 0;
                    // `reconnecting` is deliberately not cleared here: on the
                    // reconnect path restoration still has to run, and clearing the
                    // flag would let a socket dying mid-restore schedule a second
                    // reconnection alongside the one its failure schedules.
                }
            }
            if (stale) {
                Logger.info("discarding a connection that completed after disconnect");
                h.close();
                throw new ReconnectionError("connection was torn down while connecting");
            }
            h.onClose(reason -> onHandleClosed(h, reason));
            Logger.info("connected to message bus");
            return h;
        } finally {
            connectMutex.unlock();
        }
    }

    private void onHandleClosed(AmqpConnection h, String reason) {
        synchronized (lock) {
            if (h != handle) return;
            if (manualDisconnect) {
                Logger.info("connection closed (manual disconnect)");
                return;
            }
            connected = false;
            ready = false;
            // Retire this generation: a restoration in flight against it is now
            // working on a dead socket, and its next generation check aborts it.
            generation++;
        }
        Logger.warn("connection closed unexpectedly" + (reason == null ? "" : ": " + reason));
        emitDisconnected();
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        int attempt;
        long delay;
        String giveUp = null;
        synchronized (lock) {
            if (manualDisconnect || reconnecting) return;
            if (reconnection.maxRetries > 0 && reconnectAttempts >= reconnection.maxRetries) {
                giveUp = "max reconnection attempts (" + reconnection.maxRetries + ") exceeded";
                abandoned = giveUp;
                attempt = 0;
                delay = 0;
            } else {
                reconnecting = true;
                attempt = ++reconnectAttempts;
                double base = Math.min(
                        reconnection.initialDelayMs * Math.pow(reconnection.backoffMultiplier, attempt - 1),
                        reconnection.maxDelayMs);
                delay = (long) Math.floor(base + Math.random() * 0.3 * base);
                reconnectTimer = timer.schedule(() -> runInternal(this::reconnectAttempt), delay,
                        TimeUnit.MILLISECONDS);
            }
        }
        if (giveUp != null) {
            ReconnectionError error = new ReconnectionError(giveUp);
            Logger.error(giveUp);
            List<CompletableFuture<Void>> waiters;
            synchronized (lock) {
                ready = false;
                waiters = new ArrayList<>(readyWaiters);
                readyWaiters.clear();
            }
            releaseWaiters(waiters, new NotReadyError(giveUp));
            emitError(error);
            return;
        }
        Logger.info("scheduling reconnection attempt " + attempt + " in " + delay + "ms");
        emitReconnecting(attempt, delay);
    }

    private void reconnectAttempt() {
        int attempt;
        synchronized (lock) {
            reconnectTimer = null;
            // Read before connecting: a successful connect resets it.
            attempt = reconnectAttempts;
            if (manualDisconnect) {
                reconnecting = false;
                return;
            }
        }
        try {
            doConnect();
            long gen;
            synchronized (lock) {
                gen = generation;
            }
            // Restoration is part of reconnecting: until every component has its
            // channel, queues, bindings and consumers back, the socket is up but
            // the application cannot use it.
            runRestorers(gen);
            List<CompletableFuture<Void>> waiters;
            synchronized (lock) {
                if (gen != generation) throw new ReconnectionError("connection was torn down while restoring");
                reconnecting = false;
                ready = true;
                waiters = new ArrayList<>(readyWaiters);
                readyWaiters.clear();
            }
            releaseWaiters(waiters, null);
            Logger.info("reconnection successful after " + attempt + " attempts");
            emitReconnected();
        } catch (Exception e) {
            synchronized (lock) {
                if (manualDisconnect) {
                    reconnecting = false;
                    return;
                }
            }
            Logger.error("reconnection attempt " + attempt + " failed: " + Errors.messageOf(e));
            // A generation that connected but could not be restored looks healthy
            // and serves nothing. Drop it and let the backoff try again.
            discardGeneration();
            synchronized (lock) {
                // doConnect zeroed the counter on the way through; put the attempt
                // back or the retry budget never runs out.
                reconnectAttempts = attempt;
                reconnecting = false;
            }
            scheduleReconnect();
        }
    }

    private void runRestorers(long gen) throws Exception {
        for (Restorer r : new ArrayList<>(restorers)) {
            synchronized (lock) {
                if (gen != generation) throw new ReconnectionError("connection was torn down while restoring");
            }
            r.restore(gen);
        }
        synchronized (lock) {
            if (gen != generation) throw new ReconnectionError("connection was torn down while restoring");
        }
    }

    private void discardGeneration() {
        AmqpConnection h;
        synchronized (lock) {
            generation++;
            ready = false;
            connected = false;
            h = handle;
            handle = null;
        }
        if (h != null) {
            try {
                h.close();
            } catch (RuntimeException e) {
                Logger.debug("failed closing an unrestorable connection: " + e.getMessage());
            }
        }
    }

    /** The generation a restorer is restoring is still current. */
    boolean isCurrent(long gen) {
        synchronized (lock) {
            return gen == generation && connected;
        }
    }

    long generation() {
        synchronized (lock) {
            return generation;
        }
    }

    /**
     * Close deliberately. Waiters on readiness fail with NotReadyError, and the
     * {@link #onDisconnected} listeners run, so pending calls and streams fail at
     * once.
     */
    public void disconnect() {
        AmqpConnection h;
        List<CompletableFuture<Void>> waiters;
        synchronized (lock) {
            manualDisconnect = true;
            // Invalidate any connect already past its timer.
            generation++;
            if (reconnectTimer != null) {
                reconnectTimer.cancel(false);
                reconnectTimer = null;
            }
            reconnecting = false;
            ready = false;
            waiters = new ArrayList<>(readyWaiters);
            readyWaiters.clear();
            h = handle;
            handle = null;
            connected = false;
        }
        releaseWaiters(waiters, new NotReadyError("the connection has been closed"));
        // Listeners first, so pending calls and streams fail as disconnected
        // before closing the socket can fail a queued write under them.
        emitDisconnected();
        if (h != null) {
            try {
                h.close();
            } catch (RuntimeException e) {
                Logger.debug("closing the connection: " + e.getMessage());
            }
            Logger.info("connection closed (manual disconnect)");
        }
    }

    /** Release the connection's threads. Call once, after {@link #disconnect()}. */
    void shutdownExecutors() {
        timer.shutdownNow();
        internal.shutdown();
        if (ownsHandlers) handlers.shutdown();
    }

    // ---- streams and draining -----------------------------------------------------------

    private static final class DeliveryEntry {
        final AbortController controller = new AbortController();
        volatile boolean cancelled;
    }

    /**
     * Stop producing a streaming reply the caller has abandoned: abort the handler's
     * signal and publish nothing more it produces. Cooperative: a producer that
     * ignores its signal keeps running, but its output goes nowhere.
     *
     * @return whether a matching in-flight delivery was found
     */
    public boolean cancelStream(String correlationId) {
        Set<DeliveryEntry> entries = activeDeliveries.get(correlationId);
        if (entries == null || entries.isEmpty()) return false;
        // A redelivery can overlap its predecessor; both are the caller's stream.
        for (DeliveryEntry e : entries.toArray(new DeliveryEntry[0])) {
            e.cancelled = true;
            e.controller.abort();
        }
        Logger.debug("stream " + correlationId + " cancelled by the caller");
        return true;
    }

    /** Messages being handled, counting handlers still running after a processing timeout settled them. */
    public long inFlightDeliveries() {
        synchronized (drainLock) {
            return Math.max(inFlight, running);
        }
    }

    /**
     * Wait for in-flight work to finish, up to {@code timeoutMs}. Returns false when
     * the deadline passed with work still running: information for the caller, not
     * an error.
     */
    public boolean drainInFlight(long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (drainLock) {
            while (Math.max(inFlight, running) > 0) {
                long left = deadline - System.nanoTime();
                if (left <= 0) return false;
                try {
                    TimeUnit.NANOSECONDS.timedWait(drainLock, left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    private void deliveryStarted() {
        synchronized (drainLock) {
            inFlight++;
        }
    }

    private void deliveryFinished() {
        synchronized (drainLock) {
            if (inFlight > 0) inFlight--;
            drainLock.notifyAll();
        }
    }

    private void handlerStarted() {
        synchronized (drainLock) {
            running++;
        }
    }

    private void handlerFinished() {
        synchronized (drainLock) {
            if (running > 0) running--;
            drainLock.notifyAll();
        }
    }

    // ---- channel operations ------------------------------------------------------------

    public AmqpChannel openChannel() {
        AmqpConnection h;
        synchronized (lock) {
            h = handle;
        }
        if (h == null) throw new NotConnectedError("no broker connection");
        return h.openChannel();
    }

    public void closeChannel(AmqpChannel channel) {
        if (channel != null) channel.close();
    }

    public void declareExchange(AmqpChannel channel, String exchange, String type) {
        channel.declareExchange(exchange, type, true, false, false, Map.of());
    }

    public String declareQueue(AmqpChannel channel, String queue, boolean durable, boolean exclusive,
                               boolean autoDelete, Map<String, Object> arguments) {
        return channel.declareQueue(queue, durable, exclusive, autoDelete, arguments == null ? Map.of() : arguments);
    }

    public void bindQueue(AmqpChannel channel, String queue, String exchange, String routingKey) {
        channel.bindQueue(queue, exchange, routingKey, Map.of());
    }

    public void unbindQueue(AmqpChannel channel, String queue, String exchange, String routingKey) {
        channel.unbindQueue(queue, exchange, routingKey, Map.of());
    }

    public void deleteQueue(AmqpChannel channel, String queue) {
        channel.deleteQueue(queue);
    }

    public void purgeQueue(AmqpChannel channel, String queue) {
        channel.purgeQueue(queue);
    }

    public void cancel(AmqpChannel channel, String consumerTag) {
        channel.cancel(consumerTag);
    }

    // ---- consuming ---------------------------------------------------------------------

    /**
     * AMQP properties a retry or dead-letter republish copies from the original
     * delivery. protobus moves a failed message by republishing it with properties
     * built by hand, so anything not carried here is silently dropped.
     *
     * Not carried, each for a reason: {@code deliveryMode} (re-expressed as
     * persistent at every hop), {@code expiration} (it would race the retry queue's
     * own TTL, and on the DLQ delete the evidence), and {@code userId} (RabbitMQ
     * validates it against the publishing connection's user, which for a republish
     * is the consumer's).
     */
    static BasicProperties.Builder carried(BasicProperties p) {
        BasicProperties.Builder b = new BasicProperties.Builder();
        if (p == null) return b;
        if (p.getContentType() != null) b.contentType(p.getContentType());
        if (p.getContentEncoding() != null) b.contentEncoding(p.getContentEncoding());
        if (p.getPriority() != null) b.priority(p.getPriority());
        if (p.getTimestamp() != null) b.timestamp(p.getTimestamp());
        if (p.getType() != null) b.type(p.getType());
        if (p.getAppId() != null) b.appId(p.getAppId());
        return b;
    }

    private static final class InFlight {
        final AmqpChannel channel;
        final String queue;
        final Delivery delivery;
        final ConsumeOptions options;
        final boolean lateAck;
        final ConsumeRetryOptions retry;
        final DeliveryEntry entry = new DeliveryEntry();
        final String correlationId;
        final String replyTo;
        final Map<String, Object> headers;
        final AtomicBoolean finished = new AtomicBoolean();

        InFlight(AmqpChannel channel, String queue, Delivery delivery, ConsumeOptions options, boolean lateAck,
                 ConsumeRetryOptions retry) {
            this.channel = channel;
            this.queue = queue;
            this.delivery = delivery;
            this.options = options;
            this.lateAck = lateAck;
            this.retry = retry;
            BasicProperties p = delivery.properties();
            this.correlationId = p == null || p.getCorrelationId() == null ? "" : p.getCorrelationId();
            this.replyTo = p == null ? null : p.getReplyTo();
            this.headers = p == null || p.getHeaders() == null ? Map.of() : p.getHeaders();
        }

        boolean settlesLate() {
            return !options.noAck && lateAck;
        }
    }

    /**
     * Consume {@code queue} with {@code handler}.
     *
     * An early-ack consumer (lateAck false) acknowledges on delivery and never
     * retries. A late-ack consumer settles after the handler: it replies, then acks;
     * on a failure it retries through {@code retry}, dead-letters once the retries
     * are spent, and answers the caller on every terminal path.
     *
     * @return the consumer tag
     */
    public String consume(AmqpChannel channel, String queue, MessageHandler handler, ConsumeOptions options,
                          boolean lateAck, ConsumeRetryOptions retry, Long processingTimeoutMs) {
        Limited limited = options.maxConcurrency > 0 ? new Limited(handlers, options.maxConcurrency, timer) : null;
        return channel.consume(queue, options.consumerTag, options.noAck, options.exclusive, delivery -> {
            // Counted for the whole settle, so a graceful shutdown waits for the
            // reply, retry or dead-letter publish and not just the handler body.
            deliveryStarted();
            InFlight d = new InFlight(channel, queue, delivery, options, lateAck, retry);
            if (options.ordered) {
                handleDelivery(d, handler, processingTimeoutMs);
                return;
            }
            Runnable refused = () -> refuseDelivery(d, channel, delivery, queue);
            if (limited != null) {
                limited.execute(() -> handleDelivery(d, handler, processingTimeoutMs), refused);
                return;
            }
            try {
                handlers.execute(() -> handleDelivery(d, handler, processingTimeoutMs));
            } catch (RejectedExecutionException e) {
                refused.run();
            }
        }, () -> {
            Logger.warn("consumer for " + queue + " was cancelled by the broker");
            if (options.onCancelled != null) safely(options.onCancelled);
        });
    }

    /** A delivery the handler executor will not run: settle it so it is not held. */
    private void refuseDelivery(InFlight d, AmqpChannel channel, Delivery delivery, String queue) {
        deliveryFinished();
        if (!d.settlesLate()) {
            Logger.error("handler executor refused an acknowledged delivery on " + queue + "; it is lost");
            return;
        }
        // Holding it unacknowledged would keep its prefetch slot for the life
        // of the channel: hand it back, after a pause.
        Logger.error("handler executor refused a delivery on " + queue + "; requeueing it in 1 s");
        later(() -> {
            try {
                channel.reject(delivery.deliveryTag(), true);
            } catch (RuntimeException err) {
                Logger.debug("requeue failed: " + err.getMessage());
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * Runs at most {@code limit} tasks at once on {@code target}; the rest wait, in
     * order. A worker the target accepted goes on to run the queued tasks itself,
     * so the queue never depends on the target accepting a resubmission from a
     * worker that is still occupying its slot. A task the target refuses is
     * retried shortly, unless the target has shut down: then it is handed to its
     * {@code refused} callback, so it is settled rather than stranded.
     */
    private static final class Limited {
        private final Executor target;
        private final int limit;
        private final ScheduledExecutorService timer;
        private final ArrayDeque<Runnable[]> waiting = new ArrayDeque<>();
        private int active;

        Limited(Executor target, int limit, ScheduledExecutorService timer) {
            this.target = target;
            this.limit = limit;
            this.timer = timer;
        }

        /** @param refused settles the task if it can never run */
        void execute(Runnable task, Runnable refused) {
            Runnable[] entry = {task, refused};
            synchronized (this) {
                if (active >= limit) {
                    waiting.add(entry);
                    return;
                }
                active++;
            }
            launch(entry);
        }

        private void launch(Runnable[] first) {
            try {
                target.execute(() -> work(first));
            } catch (RejectedExecutionException e) {
                boolean shutdown = target instanceof ExecutorService && ((ExecutorService) target).isShutdown();
                List<Runnable[]> dropped = new ArrayList<>();
                synchronized (this) {
                    active--;
                    if (shutdown) {
                        dropped.add(first);
                        dropped.addAll(waiting);
                        waiting.clear();
                    } else {
                        waiting.addFirst(first);
                    }
                }
                for (Runnable[] d : dropped) d[1].run();
                if (!shutdown) retryLater();
            }
        }

        private void retryLater() {
            try {
                timer.schedule(this::retry, 20, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                List<Runnable[]> dropped;
                synchronized (this) {
                    dropped = new ArrayList<>(waiting);
                    waiting.clear();
                }
                for (Runnable[] d : dropped) d[1].run();
            }
        }

        private void retry() {
            Runnable[] next;
            synchronized (this) {
                if (active >= limit || waiting.isEmpty()) return;
                next = waiting.poll();
                active++;
            }
            launch(next);
        }

        private void work(Runnable[] first) {
            Runnable[] current = first;
            while (current != null) {
                try {
                    current[0].run();
                } catch (RuntimeException e) {
                    Logger.error("a delivery task failed unexpectedly: " + e);
                }
                synchronized (this) {
                    current = waiting.poll();
                    if (current == null) active--;
                }
            }
        }
    }

    private void handleDelivery(InFlight d, MessageHandler handler, Long processingTimeoutMs) {
        long retryCount = retryCount(d.headers);
        Logger.debug("incoming message on " + d.queue + " (routing key " + d.delivery.routingKey() + ")"
                + (retryCount > 0 ? " (retry " + retryCount + ")" : ""));

        if (!d.options.noAck && !d.lateAck) {
            try {
                d.channel.ack(d.delivery.deliveryTag());
            } catch (RuntimeException e) {
                Logger.debug("early ack failed on " + d.queue + ": " + e.getMessage());
            }
        }

        // Registered for the whole delivery, so cancelStream() reaches the
        // handler's signal at any point.
        activeDeliveries.computeIfAbsent(d.correlationId, k -> ConcurrentHashMap.newKeySet()).add(d.entry);

        long limit = processingTimeoutMs != null ? processingTimeoutMs : Config.messageProcessingTimeout();
        AtomicBoolean claimed = new AtomicBoolean();
        ScheduledFuture<?> expiry = timer.schedule(() -> {
            if (!claimed.compareAndSet(false, true)) return;
            d.entry.controller.abort();
            TimeoutError timeout = new TimeoutError(
                    "message " + d.correlationId + " exceeded the " + limit + "ms processing timeout");
            runInternal(() -> settleError(d, timeout));
        }, limit, TimeUnit.MILLISECONDS);

        BasicProperties p = d.delivery.properties();
        MessageHandlerContext context = new MessageHandlerContext(d.entry.controller.signal(),
                d.delivery.routingKey(), p == null ? null : p.getMessageId(), d.delivery.redelivered(), d.headers);
        HandlerResult result = null;
        Throwable failure = null;
        handlerStarted();
        try {
            result = handler.handle(d.delivery.body(), d.correlationId, context);
            if (result == null) result = HandlerResult.none();
        } catch (Throwable t) {
            failure = t;
        } finally {
            handlerFinished();
        }
        if (!claimed.compareAndSet(false, true)) {
            Logger.debug("message " + d.correlationId + " finished after its processing timeout; result discarded");
            return;
        }
        expiry.cancel(false);
        if (failure != null) settleError(d, failure);
        else settle(d, result);
    }

    private static long retryCount(Map<String, Object> headers) {
        Long n = Headers.integer(headers.get("x-retry-count"));
        return n == null || n < 0 ? 0 : n;
    }

    private void finishDelivery(InFlight d) {
        if (!d.finished.compareAndSet(false, true)) return;
        Set<DeliveryEntry> group = activeDeliveries.get(d.correlationId);
        if (group != null) {
            group.remove(d.entry);
            if (group.isEmpty()) activeDeliveries.remove(d.correlationId, group);
        }
        deliveryFinished();
    }

    private static BasicProperties replyProperties(String correlationId, Map<String, Object> headers) {
        BasicProperties.Builder b = new BasicProperties.Builder()
                .contentType("application/octet-stream")
                .correlationId(correlationId);
        if (headers != null) b.headers(headers);
        return b.build();
    }

    private void settle(InFlight d, HandlerResult result) {
        try {
            // The reply is published before the request is settled, so the worst
            // case is a redelivered request (at-least-once) rather than a settled
            // request whose reply was never sent.
            if (d.replyTo != null && !d.replyTo.isEmpty()) {
                if (result.stream != null) {
                    publishStreamReply(d.channel, d.replyTo, d.correlationId, result.stream,
                            () -> d.entry.cancelled);
                } else if (result.reply != null) {
                    publish(d.channel, Config.callbacksExchangeName(), d.replyTo, result.reply,
                            PublishOptions.of(replyProperties(d.correlationId, null)));
                }
            }
            if (d.settlesLate()) d.channel.ack(d.delivery.deliveryTag());
        } catch (Throwable t) {
            settleError(d, t);
            return;
        }
        finishDelivery(d);
    }

    private void settleError(InFlight d, Throwable error) {
        try {
            settleErrorOrThrow(d, error);
        } catch (Throwable t) {
            // Every path that can throw runs before the settlement, so the message
            // is still unacknowledged. Hand it back to the broker, after a pause so
            // a persistent failure does not spin.
            Logger.error("failed to settle message on " + d.queue + ": " + Errors.messageOf(t)
                    + (d.settlesLate() ? ". Requeueing it in 1 s." : ""));
            if (d.settlesLate()) {
                later(() -> {
                    try {
                        d.channel.reject(d.delivery.deliveryTag(), true);
                    } catch (RuntimeException e) {
                        Logger.debug("requeue failed (the channel is likely gone, which requeues it anyway): "
                                + e.getMessage());
                    }
                }, 1000, TimeUnit.MILLISECONDS);
            }
        } finally {
            finishDelivery(d);
        }
    }

    private void settleErrorOrThrow(InFlight d, Throwable error) {
        // A cancelled delivery is a normal outcome: the caller asked to stop.
        if (d.entry.cancelled) {
            Logger.debug("message " + d.correlationId + " ended because its stream was cancelled");
            if (d.settlesLate()) d.channel.ack(d.delivery.deliveryTag());
            return;
        }

        Throwable cause = error;
        byte[] errorReply = null;
        if (error instanceof ErrorWithReply) {
            cause = error.getCause();
            errorReply = ((ErrorWithReply) error).reply();
        } else if (d.options.buildErrorReply != null) {
            try {
                errorReply = d.options.buildErrorReply.apply(d.delivery.body(), error);
            } catch (RuntimeException e) {
                Logger.debug("could not build an error reply: " + e.getMessage());
            }
        }
        Logger.error("unhandled error consuming bus message - " + Errors.messageOf(cause)
                + "\n" + Threads.stackTrace(cause));

        final byte[] reply = errorReply;
        // BEST EFFORT, deliberately: the reply may be lost (the caller has a
        // timeout), while the dead-letter queue is the only durable record that
        // the message existed. A failed reply must not take the settlement with it.
        Runnable publishErrorReply = () -> {
            if (d.replyTo == null || d.replyTo.isEmpty() || reply == null) return;
            try {
                publish(d.channel, Config.callbacksExchangeName(), d.replyTo, reply,
                        PublishOptions.of(replyProperties(d.correlationId, null)));
            } catch (RuntimeException e) {
                Logger.error("failed to publish the error reply for " + d.correlationId + " to " + d.replyTo + ": "
                        + Errors.messageOf(e) + ". The caller will time out; settling the message anyway.");
            }
        };

        if (!d.settlesLate()) {
            // Acked before processing, so retry and dead-lettering are impossible,
            // but the caller must still be told.
            publishErrorReply.run();
            return;
        }

        ConsumeRetryOptions retry = d.retry;
        boolean handled = retry != null && retry.isHandledError != null && retry.isHandledError.test(cause);
        BasicProperties original = d.delivery.properties();
        String messageId = original == null ? null : original.getMessageId();
        long retryCount = retryCount(d.headers);
        // The delivered routing key, which the retry hop preserves. The header is
        // written for operators; nothing routes by it.
        String originalRoutingKey = d.delivery.routingKey();
        Object firstFailure = d.headers.get("x-first-failure-time");
        if (firstFailure == null) firstFailure = System.currentTimeMillis();

        if (retry != null && !handled && retry.maxRetries > 0) {
            if (retryCount < retry.maxRetries) {
                long next = retryCount + 1;
                Logger.warn("retrying message " + d.correlationId + " (attempt " + next + "/" + retry.maxRetries + ")");
                Map<String, Object> headers = new LinkedHashMap<>(d.headers);
                headers.put("x-retry-count", next);
                headers.put("x-original-routing-key", originalRoutingKey);
                headers.put("x-first-failure-time", firstFailure);
                headers.put("x-last-error", Errors.safeErrorSummary(cause));
                BasicProperties props = carried(original).deliveryMode(2).correlationId(d.correlationId)
                        .messageId(messageId).replyTo(d.replyTo).headers(headers).build();
                // Published to the retry EXCHANGE under the original routing key, so
                // the post-TTL dead-letter hop routes it back to the service queue.
                // The caller stays parked: no reply here.
                if (retry.retryExchangeName != null && !retry.retryExchangeName.isEmpty()) {
                    publish(d.channel, retry.retryExchangeName, originalRoutingKey, d.delivery.body(),
                            new PublishOptions(props, true));
                } else {
                    publish(d.channel, "", retry.retryQueueName, d.delivery.body(), new PublishOptions(props, true));
                }
                d.channel.ack(d.delivery.deliveryTag());
            } else {
                Logger.error("message " + d.correlationId + " exceeded max retries (" + retry.maxRetries
                        + "), sending to DLQ");
                // Answer the caller first, so it gets the error rather than its own
                // timeout, then keep the message for operators.
                publishErrorReply.run();
                Map<String, Object> headers = new LinkedHashMap<>(d.headers);
                headers.put("x-retry-count", retryCount);
                headers.put("x-original-routing-key", originalRoutingKey);
                headers.put("x-original-queue", d.queue);
                headers.put("x-first-failure-time", firstFailure);
                headers.put("x-dlq-time", System.currentTimeMillis());
                headers.put("x-last-error", Errors.safeErrorSummary(cause));
                BasicProperties props = carried(original).deliveryMode(2).correlationId(d.correlationId)
                        .messageId(messageId).headers(headers).build();
                publish(d.channel, "", retry.dlqName, d.delivery.body(), new PublishOptions(props, true));
                d.channel.ack(d.delivery.deliveryTag());
            }
        } else {
            // No retry configured, retries disabled, or a handled error: answer the
            // caller, then reject without requeue so it cannot loop forever.
            if (handled) {
                Logger.warn("handled error for message " + d.correlationId + ", not retrying: "
                        + Errors.messageOf(cause));
            }
            publishErrorReply.run();
            Logger.warn("rejecting message " + d.correlationId);
            d.channel.reject(d.delivery.deliveryTag(), false);
        }
    }

    /**
     * Publish a streaming reply: every chunk carries the same correlationId and an
     * {@code x-protobus-seq} from 0; all but the last carry
     * {@code x-protobus-final=false}, the last {@code true}. A producer that writes
     * nothing produces one empty final chunk, so the caller's iteration ends.
     *
     * Look-ahead by one: a chunk is held until the next exists or the producer
     * returns, which is what lets the last data chunk be the final one.
     */
    private void publishStreamReply(AmqpChannel channel, String replyTo, String correlationId,
                                    StreamProducer producer, BooleanSupplier isCancelled) throws Exception {
        final class Sink implements ChunkSink {
            byte[] buffered;
            long seq;

            void publishOne(byte[] body, long sequence, boolean last) {
                Map<String, Object> headers = new HashMap<>();
                headers.put(Config.HEADER_FINAL, last);
                headers.put(Config.HEADER_SEQ, sequence);
                publish(channel, Config.callbacksExchangeName(), replyTo, body,
                        PublishOptions.of(replyProperties(correlationId, headers)));
            }

            @Override
            public void write(byte[] chunk) {
                // A caller that cancelled is not listening: stop sending, and stop
                // the producer.
                if (isCancelled.getAsBoolean()) {
                    Logger.debug("stream " + correlationId + " cancelled after " + seq + " chunk(s)");
                    throw new StreamCancelledException();
                }
                if (buffered != null) {
                    publishOne(buffered, seq, false);
                    seq++;
                }
                buffered = chunk == null ? new byte[0] : chunk;
            }

            @Override
            public boolean cancelled() {
                return isCancelled.getAsBoolean();
            }
        }
        Sink sink = new Sink();
        producer.produce(sink);
        if (isCancelled.getAsBoolean()) throw new StreamCancelledException();
        if (sink.buffered != null) sink.publishOne(sink.buffered, sink.seq, true);
        else sink.publishOne(new byte[0], 0, true);
    }

    // ---- publishing --------------------------------------------------------------------

    /** Per-channel publish bookkeeping. */
    private static final class PublishState {
        final Object lock = new Object();
        /** Writes waiting for the channel, in publish order; one drains them at a time. */
        final ArrayDeque<Runnable> writes = new ArrayDeque<>();
        boolean writing;
        final Map<String, Integer> awaiting = new HashMap<>();
        int inFlight;
        final ArrayDeque<Runnable> waiters = new ArrayDeque<>();
    }

    private final Map<AmqpChannel, PublishState> publishStates = new WeakHashMap<>();

    private PublishState publishStateFor(AmqpChannel channel) {
        PublishState created;
        synchronized (publishStates) {
            PublishState existing = publishStates.get(channel);
            if (existing != null) return existing;
            created = new PublishState();
            publishStates.put(channel, created);
        }
        // A channel closing with publishes parked on the outstanding bound would
        // hold them forever: wake them, and they fail on the closed channel.
        channel.onClose(reason -> {
            List<Runnable> parked;
            synchronized (created.lock) {
                parked = new ArrayList<>(created.waiters);
                created.waiters.clear();
            }
            for (Runnable wake : parked) runInternal(wake);
        });
        return created;
    }

    /**
     * Publish and return only once RabbitMQ has confirmed the message: positively
     * and, for a mandatory publish, routed. Everything else throws a
     * {@link PublishError}. {@link PublishConfirmTimeoutError} and
     * {@link ChannelClosedError} are AMBIGUOUS: the broker may have stored the
     * message, so a retry can duplicate it, which is why every publish carries a
     * messageId (a UUID unless one is set) for consumers to deduplicate on.
     *
     * @return the messageId
     */
    public String publish(AmqpChannel channel, String exchange, String routingKey, byte[] content,
                          PublishOptions options) {
        return join(publishAsync(channel, exchange, routingKey, content, options));
    }

    /**
     * {@link #publish} without waiting. The future completes on whichever thread
     * settles it, often the transport's: continuations attached to it must not
     * block.
     */
    public CompletableFuture<String> publishAsync(AmqpChannel channel, String exchange, String routingKey,
                                                  byte[] content, PublishOptions options) {
        BasicProperties props = options.properties() == null ? new BasicProperties() : options.properties();
        String messageId = props.getMessageId() != null && !props.getMessageId().isEmpty()
                ? props.getMessageId() : UUID.randomUUID().toString();
        String describe = (exchange == null || exchange.isEmpty() ? "(default)" : exchange) + " -> " + routingKey;
        CompletableFuture<String> result = new CompletableFuture<>();
        if (channel == null || !channel.isOpen()) {
            result.completeExceptionally(new ChannelClosedError(describe + ": the channel is closed", messageId));
            return result;
        }
        PublishState state = publishStateFor(channel);
        Runnable send = () -> send(channel, state, exchange, routingKey, content, props, options.mandatory(),
                messageId, describe, result);
        // The caller's deadline runs from here, so time spent waiting for a slot
        // counts against it too.
        long confirmTimeout = Config.publishConfirmTimeoutMs();
        Runnable[] parked = new Runnable[1];
        ScheduledFuture<?> deadline;
        try {
            deadline = timer.schedule(() -> {
                boolean neverSent;
                synchronized (state.lock) {
                    neverSent = parked[0] != null && state.waiters.remove(parked[0]);
                }
                result.completeExceptionally(new PublishConfirmTimeoutError(neverSent
                        ? describe + " waited " + confirmTimeout + "ms for one of the channel's "
                                + Config.maxOutstandingConfirms() + " confirm slots and was not sent"
                        : "no broker confirm for " + describe + " within " + confirmTimeout + "ms", messageId));
            }, confirmTimeout, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(new NotConnectedError("the connection has been shut down"));
            return result;
        }
        result.whenComplete((v, e) -> deadline.cancel(false));
        boolean now;
        synchronized (state.lock) {
            now = state.inFlight < Config.maxOutstandingConfirms();
            if (now) {
                state.inFlight++;
            } else {
                parked[0] = () -> {
                    if (result.isDone()) {
                        // Timed out while parked: hand the slot straight on.
                        releaseSlot(state);
                        return;
                    }
                    write(state, send);
                };
                state.waiters.add(parked[0]);
            }
        }
        if (now) write(state, send);
        return result;
    }

    /**
     * Hand a write to the channel's writer. Writes run on the library's own
     * threads, one at a time per channel and in order, never on the caller's or a
     * timer's: a socket write can block (a full TCP buffer, broker flow control),
     * and a blocked caller could not see its deadline pass. The queue is bounded
     * by the channel's confirm slots, since only a publish holding one is queued.
     */
    private void write(PublishState state, Runnable task) {
        boolean start;
        synchronized (state.lock) {
            state.writes.add(task);
            start = !state.writing;
            if (start) state.writing = true;
        }
        if (start) runInternal(() -> drainWrites(state));
    }

    private void drainWrites(PublishState state) {
        while (true) {
            Runnable next;
            synchronized (state.lock) {
                next = state.writes.poll();
                if (next == null) {
                    state.writing = false;
                    return;
                }
            }
            try {
                next.run();
            } catch (RuntimeException e) {
                Logger.error("a publish failed unexpectedly: " + e);
            }
        }
    }

    /** A slot came free: give it to the next parked publish, which then owns it. */
    private void releaseSlot(PublishState state) {
        Runnable next;
        synchronized (state.lock) {
            next = state.waiters.poll();
            if (next == null) state.inFlight--;
        }
        if (next != null) runInternal(next);
    }

    /**
     * Send one publish holding a slot. The slot, and the messageId's place among
     * the channel's unconfirmed publishes, are held until the broker answers or the
     * channel closes, not until the caller stops waiting: a publish whose confirm
     * timed out may still be stored, and the bound is on what the broker has not
     * answered.
     */
    private void send(AmqpChannel channel, PublishState state, String exchange, String routingKey, byte[] content,
                      BasicProperties props, boolean mandatory, String messageId, String describe,
                      CompletableFuture<String> result) {
        if (result.isDone()) {
            // Its deadline passed while it waited for the writer: never sent.
            releaseSlot(state);
            return;
        }
        AtomicBoolean settled = new AtomicBoolean();
        BasicProperties.Builder b = props.builder().messageId(messageId);
        synchronized (state.lock) {
            // A return is matched to its publish by messageId. When two mandatory
            // publishes sharing one are pending on this channel, a per-publish tag
            // tells their returns apart. Every port ignores or copies unknown headers.
            if (mandatory && state.awaiting.getOrDefault(messageId, 0) > 0) {
                Map<String, Object> headers = new HashMap<>(props.getHeaders() == null ? Map.of() : props.getHeaders());
                headers.put(RabbitTransport.PUBLISH_TAG_HEADER, UUID.randomUUID().toString());
                b.headers(headers);
            }
            state.awaiting.merge(messageId, 1, Integer::sum);
        }
        BasicProperties finalProps = b.build();
        Consumer<Throwable> settle = error -> {
            if (!settled.compareAndSet(false, true)) return;
            synchronized (state.lock) {
                state.awaiting.computeIfPresent(messageId, (k, n) -> n <= 1 ? null : n - 1);
            }
            releaseSlot(state);
            // A caller whose deadline already passed keeps that answer.
            if (error == null) result.complete(messageId);
            else result.completeExceptionally(error);
        };
        try {
            channel.publish(exchange, routingKey, content, finalProps, mandatory, (outcome, detail) -> {
                switch (outcome) {
                    case ACK:
                        settle.accept(null);
                        break;
                    case NACK:
                        settle.accept(new PublishNackedError("broker nacked " + describe
                                + (detail == null || detail.isEmpty() ? "" : ": " + detail), messageId));
                        break;
                    case RETURNED:
                        settle.accept(new UnroutableError(describe + " was confirmed but returned as unroutable",
                                messageId));
                        break;
                    default:
                        settle.accept(new ChannelClosedError(describe + " was unconfirmed when the channel closed"
                                + (detail == null || detail.isEmpty() ? "" : " (" + detail + ")"), messageId));
                }
            });
        } catch (AmqpException e) {
            settle.accept(new ChannelClosedError(describe + " could not be written: " + e.getMessage(), messageId));
        } catch (RuntimeException e) {
            settle.accept(e);
        }
    }

    /**
     * Run on the internal executor, or right here once it has been shut down: a
     * late callback after Context.close() must still complete what it completes.
     */
    /** Run {@code task} on the internal executor in a second; dropped once the connection is shut down. */
    private void later(Runnable task, long ms, TimeUnit unit) {
        try {
            timer.schedule(() -> runInternal(task), ms, unit);
        } catch (RejectedExecutionException e) {
            Logger.debug("connection shut down; a delayed requeue is dropped (the broker requeues on close)");
        }
    }

    void runInternal(Runnable task) {
        try {
            internal.execute(task);
        } catch (RejectedExecutionException e) {
            task.run();
        }
    }

    // ---- helpers -----------------------------------------------------------------------

    /** Wait for a future, rethrowing its failure as it was raised. */
    static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            if (cause instanceof Error) throw (Error) cause;
            throw new ProtobusException(Errors.messageOf(cause), null, cause);
        } catch (java.util.concurrent.CancellationException e) {
            throw new ProtobusException("cancelled", null, e);
        }
    }
}
