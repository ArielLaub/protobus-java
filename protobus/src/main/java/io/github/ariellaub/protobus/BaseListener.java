package io.github.ariellaub.protobus;

import io.github.ariellaub.protobus.amqp.AmqpChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * A queue and its consumer: declared on {@link #init}, consuming from
 * {@link #start}, and restored after a reconnection (or a channel lost on a live
 * connection) without the owner having to do anything.
 */
public abstract class BaseListener {
    protected final Connection connection;

    protected String exchangeName = "";
    protected String exchangeType = "";
    protected boolean lateAck;
    /** Prefetch for late-ack consumers; null falls back to {@link Config#defaultPrefetch()}. */
    protected Integer maxConcurrent;
    protected Long messageTtlMs;
    protected Integer maxPriority;
    protected Long processingTimeoutMs;
    /** Handle deliveries in order on the transport's thread; see {@link Connection.ConsumeOptions#ordered}. */
    protected boolean orderedDelivery;
    /** Builds the caller's reply for a failure that carries none (a processing timeout). */
    protected BiFunction<byte[], Throwable, byte[]> buildErrorReply;

    private final Object lock = new Object();
    /** Serialises restoration against start(): a listener is never consuming twice. */
    private final Object restoreLock = new Object();
    private AmqpChannel channel;
    private String queueName = "";
    private String configuredQueueName = "";
    private boolean isAnonymous = true;
    private String consumerTag = "";
    private Connection.MessageHandler handler;
    private final List<String> bindings = new ArrayList<>();
    private boolean initialized;
    private boolean wasStarted;
    private boolean closing;
    private boolean consumerLost;
    private boolean rebuildScheduled;
    private AmqpChannel rebuildFor;
    private int rebuildFailures;

    private Runnable detachRestorer;
    private final Runnable detachDisconnected;

    protected BaseListener(Connection connection) {
        this.connection = connection;
        attachRestorer();
        this.detachDisconnected = connection.onDisconnected(this::onDisconnected);
    }

    /** A name for log lines. */
    protected String listenerName() {
        return getClass().getSimpleName();
    }

    /** Payload size and correlation id only: never the body. */
    protected Connection.HandlerResult defaultHandler(byte[] message, String correlationId,
                                                      Connection.MessageHandlerContext context) {
        Logger.warn("unhandled message by default handler (" + message.length + " bytes, correlationId "
                + (correlationId == null || correlationId.isEmpty() ? "none" : correlationId) + ")");
        return Connection.HandlerResult.none();
    }

    public boolean isConnected() {
        return connection.isConnected();
    }

    public boolean isInitialized() {
        synchronized (lock) {
            return initialized;
        }
    }

    protected AmqpChannel channel() {
        synchronized (lock) {
            return channel;
        }
    }

    public String queueName() {
        synchronized (lock) {
            return queueName;
        }
    }

    protected String configuredQueueName() {
        synchronized (lock) {
            return configuredQueueName;
        }
    }

    protected boolean isAnonymous() {
        synchronized (lock) {
            return isAnonymous;
        }
    }

    /**
     * Take part in the connection's restoration. Idempotent, like
     * {@link #detachRestorer}: a listener attaches on construction and on every
     * start(), and detaches on stopConsuming() and close(). Re-attaching in start()
     * keeps stop/start reversible.
     */
    private void attachRestorer() {
        synchronized (lock) {
            if (detachRestorer != null) return;
            detachRestorer = connection.registerRestorer(gen -> restore());
        }
    }

    private void detachRestorer() {
        Runnable detach;
        synchronized (lock) {
            detach = detachRestorer;
            detachRestorer = null;
        }
        if (detach != null) detach.run();
    }

    /** The connection was lost: the channel went with it. Runs on a transport thread. */
    protected void onDisconnected() {
        Logger.debug(listenerName() + ": connection lost, clearing channel state");
        synchronized (lock) {
            channel = null;
            consumerTag = "";
        }
    }

    /**
     * Put this listener's channel, queue, bindings and consumer back. A failure
     * propagates to the connection, which treats the whole generation as unusable
     * and retries.
     */
    protected void restore() {
        synchronized (restoreLock) {
            boolean started;
            List<String> keys;
            AmqpChannel previous;
            synchronized (lock) {
                if (!initialized || closing) return;
                started = wasStarted;
                keys = new ArrayList<>(bindings);
                previous = channel;
            }
            Logger.info(listenerName() + ": reconnected, re-initializing...");
            reinitialize();
            AmqpChannel ch = channel();
            String queue = queueName();
            for (String routingKey : keys) {
                connection.bindQueue(ch, queue, exchangeName, routingKey);
                Logger.debug(listenerName() + ": re-bound " + routingKey);
            }
            restoreTopology();
            synchronized (lock) {
                started = started && wasStarted && !closing;
                consumerLost = false;
            }
            if (started) startConsuming();
            // The channel this replaces can still be open (one whose consumer the
            // broker cancelled). Closed only now, once it is no longer current, so
            // its close is not mistaken for a loss to rebuild from.
            if (previous != null && previous != ch && previous.isOpen()) {
                try {
                    connection.closeChannel(previous);
                } catch (RuntimeException e) {
                    Logger.debug(listenerName() + ": failed closing the replaced channel: " + e.getMessage());
                }
            }
            Logger.info(listenerName() + ": successfully re-initialized after reconnection");
        }
    }

    /** Redeclare whatever a subclass declares beyond the main queue. */
    protected void restoreTopology() {}

    /** Open a channel and redeclare the exchange and queue, without changing configuration. */
    protected void reinitialize() {
        AmqpChannel ch = connection.openChannel();
        if (lateAck) ch.prefetch(effectivePrefetch());
        connection.declareExchange(ch, exchangeName, exchangeType);
        boolean anonymous = isAnonymous();
        // An anonymous queue is gone with the old connection: declare a new one.
        String requested = anonymous ? "" : configuredQueueName();
        String name = connection.declareQueue(ch, requested, !anonymous, anonymous, anonymous, buildQueueArguments());
        if (exchangeType.equals("direct")) connection.bindQueue(ch, name, exchangeName, name);
        synchronized (lock) {
            channel = ch;
            queueName = name;
        }
        watchChannel(ch);
    }

    /**
     * A channel can close while the connection stays up: a broker-side consumer
     * timeout, an ack the broker refused. The listener is rebuilt then, as on a
     * reconnection; otherwise it would go quiet behind a healthy connection.
     */
    private void watchChannel(AmqpChannel ch) {
        ch.onClose(reason -> {
            synchronized (lock) {
                if (closing || !initialized || channel != ch) return;
            }
            if (!connection.isReady()) return; // the reconnection restores it
            scheduleRebuild("channel closed on a live connection (" + reason + ")");
        });
    }

    /**
     * One rebuild pending at a time, backing off while they keep failing, so a
     * channel error that recurs on every attempt cannot spin against the broker.
     */
    private void scheduleRebuild(String reason) {
        int failures;
        synchronized (lock) {
            if (rebuildScheduled || closing || !initialized) return;
            rebuildScheduled = true;
            rebuildFor = channel;
            failures = rebuildFailures;
        }
        long delay = Math.min(100L << Math.min(failures, 9), 30000);
        Logger.warn(listenerName() + ": " + reason + "; rebuilding it in " + delay + "ms");
        connection.scheduler().schedule(() -> connection.internalExecutor().execute(this::rebuild), delay,
                TimeUnit.MILLISECONDS);
    }

    private void rebuild() {
        synchronized (lock) {
            rebuildScheduled = false;
            if (closing || !initialized) return;
            // Stopped since the broker cancelled the consumer: nothing to put back.
            if (consumerLost && !wasStarted) {
                consumerLost = false;
                return;
            }
            // A channel lost with its connection can report its close first; the
            // reconnection restores the listener, and rebuilding again would replace
            // a working consumer.
            if (rebuildFor != channel) return;
        }
        if (!connection.isReady()) return;
        synchronized (lock) {
            rebuildFailures++;
        }
        try {
            restore();
            synchronized (lock) {
                rebuildFailures = 0;
            }
        } catch (RuntimeException e) {
            Logger.error(listenerName() + ": failed to rebuild its channel: " + e.getMessage());
            scheduleRebuild(e.getMessage());
        }
    }

    /**
     * basic.cancel leaves the channel and the connection open, so neither recovery
     * would notice. The listener forgets the consumer and rebuilds.
     */
    private void onConsumerCancelled(String tag) {
        synchronized (lock) {
            if (closing || !initialized || !wasStarted || !consumerTag.equals(tag)) return;
            consumerTag = "";
            consumerLost = true;
        }
        scheduleRebuild("the broker cancelled its consumer");
    }

    /** Start consuming; subclasses enable retry through {@link #getRetryOptions()}. */
    protected void startConsuming() {
        String tag = UUID.randomUUID().toString();
        AmqpChannel ch;
        String queue;
        Connection.MessageHandler h;
        boolean anonymous;
        synchronized (lock) {
            consumerTag = tag;
            ch = channel;
            queue = queueName;
            h = handler;
            anonymous = isAnonymous;
        }
        if (ch == null) throw new NotConnectedError(listenerName() + ": no channel to consume on");
        Connection.ConsumeOptions options = new Connection.ConsumeOptions();
        options.consumerTag = tag;
        options.noAck = false;
        options.exclusive = anonymous;
        options.ordered = orderedDelivery;
        options.buildErrorReply = buildErrorReply;
        options.onCancelled = () -> onConsumerCancelled(tag);
        connection.consume(ch, queue, h, options, lateAck, getRetryOptions(), processingTimeoutMs);
        Logger.debug(listenerName() + ": started consuming from " + queue);
    }

    /** Retry options for consume. Subclasses override to enable retry. */
    protected Connection.ConsumeRetryOptions getRetryOptions() {
        return null;
    }

    /**
     * Open the channel and declare the exchange and the queue: {@code queueName},
     * or an exclusive, server-named one when it is null or empty.
     */
    public void init(Connection.MessageHandler messageHandler, String queueName) {
        synchronized (lock) {
            if (initialized) return;
        }
        if (exchangeName.isEmpty()) throw new MissingExchangeError();
        if (!connection.isConnected()) throw new ConnectionError();
        boolean anonymous = queueName == null || queueName.isEmpty();
        synchronized (lock) {
            handler = messageHandler != null ? messageHandler : this::defaultHandler;
            isAnonymous = anonymous;
            configuredQueueName = anonymous ? "" : queueName;
            closing = false;
        }
        attachRestorer();
        AmqpChannel ch = connection.openChannel();
        if (lateAck) ch.prefetch(effectivePrefetch());
        connection.declareExchange(ch, exchangeName, exchangeType);
        String name = connection.declareQueue(ch, anonymous ? "" : queueName, !anonymous, anonymous, anonymous,
                buildQueueArguments());
        // A direct-exchange listener is bound to its own name.
        if (exchangeType.equals("direct")) connection.bindQueue(ch, name, exchangeName, name);
        synchronized (lock) {
            channel = ch;
            this.queueName = name;
            initialized = true;
        }
        watchChannel(ch);
    }

    public void start() {
        synchronized (lock) {
            if (!initialized) throw new NotInitializedError();
            // Recovering from a broker cancellation counts as started: the rebuild
            // consumes again, and a second consumer here would be a duplicate.
            if (wasStarted && (!consumerTag.isEmpty() || consumerLost)) throw new AlreadyStartedError();
        }
        if (!connection.isConnected()) throw new NotConnectedError();
        // Restored again from here on: stopConsuming() drops out of restoration.
        attachRestorer();
        synchronized (restoreLock) {
            startConsuming();
            synchronized (lock) {
                wasStarted = true;
            }
        }
    }

    /**
     * Stop accepting NEW deliveries while leaving the channel open, so handlers
     * still running can ack and publish their replies: the first step of a graceful
     * shutdown. Safe to call more than once, and when disconnected.
     */
    public void stopConsuming() {
        String tag;
        AmqpChannel ch;
        synchronized (lock) {
            // Recorded first and unconditionally: a reconnection inside the drain
            // window must not put the consumer back in a process shutting down.
            tag = consumerTag;
            consumerTag = "";
            wasStarted = false;
            ch = channel;
        }
        detachRestorer();
        if (tag.isEmpty() || ch == null || !connection.isConnected()) return;
        try {
            connection.cancel(ch, tag);
            Logger.debug(listenerName() + ": stopped consuming (" + tag + ")");
        } catch (RuntimeException e) {
            Logger.debug(listenerName() + ": failed to cancel consumer '" + tag + "' during drain: " + e.getMessage());
        }
    }

    public void close() {
        String tag;
        AmqpChannel ch;
        synchronized (lock) {
            if (!initialized) throw new NotInitializedError();
            closing = true;
            tag = consumerTag;
            ch = channel;
        }
        detachRestorer();
        detachDisconnected.run();
        if (ch != null && connection.isConnected()) {
            try {
                if (!tag.isEmpty()) connection.cancel(ch, tag);
                connection.closeChannel(ch);
            } catch (RuntimeException e) {
                Logger.debug(listenerName() + ": error during close (may be expected): " + e.getMessage());
            }
        }
        synchronized (lock) {
            consumerTag = "";
            channel = null;
            initialized = false;
            wasStarted = false;
            bindings.clear();
        }
    }

    /**
     * The arguments the main queue is declared with. One method, because init() and
     * reinitialize() must agree: RabbitMQ fixes a queue's arguments at declare time,
     * and a mismatch fails the first reconnection with a 406. Each key is added only
     * when its option is set, so a listener that configures nothing declares an
     * empty table, as every earlier version did.
     */
    protected Map<String, Object> buildQueueArguments() {
        Map<String, Object> args = new LinkedHashMap<>();
        if (messageTtlMs != null) args.put("x-message-ttl", messageTtlMs);
        if (maxPriority != null) args.put("x-max-priority", maxPriority);
        return args;
    }

    /**
     * Prefetch for late-ack consumers. 0 would mean unlimited, which with late ack
     * lets the broker push a whole backlog into memory, so an unset value falls back
     * to a bounded default.
     */
    protected int effectivePrefetch() {
        if (maxConcurrent != null && maxConcurrent > 0) return maxConcurrent;
        return (int) Math.min(Config.defaultPrefetch(), 65535);
    }

    /** Bind the queue under a routing key, and remember it for restoration. */
    protected void bind(String routingKey) {
        AmqpChannel ch = channel();
        if (ch == null) throw new NotConnectedError(listenerName() + ": not connected");
        connection.bindQueue(ch, queueName(), exchangeName, routingKey);
        synchronized (lock) {
            if (!bindings.contains(routingKey)) bindings.add(routingKey);
        }
    }
}
