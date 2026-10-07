package io.github.ariellaub.protobus;

import com.google.protobuf.MessageOrBuilder;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * One process's place on the bus. It owns the connection, the message factory
 * and the two dispatchers, and is what services and proxies are built on.
 *
 * <pre>{@code
 * try (Context ctx = new Context()) {
 *     ctx.init("amqp://guest:guest@localhost:5672/");
 *     ...
 * }
 * }</pre>
 *
 * Closing it fails pending calls and streams at once, closes the connection, and
 * then waits up to {@code SHUTDOWN_DRAIN_TIMEOUT_MS} for handlers still running.
 */
public final class Context implements AutoCloseable {
    private final ContextOptions options;
    private final Connection connection;
    private final MessageFactory factory = new MessageFactory();
    private final MessageDispatcher messageDispatcher;
    private final EventDispatcher eventDispatcher;
    private boolean closed;

    public Context() {
        this(ContextOptions.DEFAULT);
    }

    public Context(ContextOptions options) {
        this.options = options == null ? ContextOptions.DEFAULT : options;
        this.connection = new Connection(this.options.transport(), this.options.handlerExecutor());
        this.messageDispatcher = new MessageDispatcher(connection);
        this.eventDispatcher = new EventDispatcher(connection);
        connection.onReconnecting((attempt, delay) ->
                Logger.info("Context: reconnecting (attempt " + attempt + ", delay " + delay + "ms)"));
        connection.onReconnected(() -> Logger.info("Context: reconnected successfully"));
        connection.onError(err -> Logger.error("Context: connection error - " + Errors.messageOf(err)));
    }

    public void init(String amqpUrl) {
        init(amqpUrl, List.of());
    }

    /**
     * Load the descriptor sets under {@code schemaLocations} (none is fine when
     * everything is compiled in), connect, and start the dispatchers.
     */
    public void init(String amqpUrl, List<String> schemaLocations) {
        factory.init(schemaLocations);
        connection.connect(amqpUrl, options.reconnection());
        messageDispatcher.init();
        eventDispatcher.init();
    }

    public boolean isConnected() {
        return connection.isConnected();
    }

    public boolean isReconnecting() {
        return connection.isReconnecting();
    }

    /** Publish an encoded RequestContainer and return the raw reply (null when {@code rpc} is false). */
    public byte[] publishMessage(byte[] content, String routingKey, CallOptions options) {
        return messageDispatcher.publish(content, routingKey, options);
    }

    public CompletableFuture<byte[]> publishMessageAsync(byte[] content, String routingKey, CallOptions options) {
        return messageDispatcher.publishAsync(content, routingKey, options);
    }

    /** Publish an encoded RequestContainer expecting a streaming reply. */
    public MessageDispatcher.ChunkStream publishStreamingMessage(byte[] content, String routingKey,
                                                                 StreamOptions options) {
        return messageDispatcher.publishStreaming(content, routingKey, options);
    }

    /** Publish an event of the message's own type, on {@code EVENT.<type>}. */
    public void publishEvent(MessageOrBuilder content) {
        publishEvent(content.getDescriptorForType().getFullName(), content, null);
    }

    public void publishEvent(String type, MessageOrBuilder content, String topic) {
        eventDispatcher.publish(type, content, topic);
    }

    public CompletableFuture<Void> publishEventAsync(String type, MessageOrBuilder content, String topic) {
        return eventDispatcher.publishAsync(type, content, topic);
    }

    public MessageFactory factory() {
        return factory;
    }

    public Connection connection() {
        return connection;
    }

    /** Close the dispatchers and the connection, then wait for running handlers. Safe to call more than once. */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
        }
        try {
            messageDispatcher.close();
            eventDispatcher.close();
        } catch (RuntimeException e) {
            Logger.debug("Context: closing the dispatchers: " + e.getMessage());
        }
        connection.disconnect();
        if (!connection.drainInFlight(Config.shutdownDrainTimeoutMs())) {
            Logger.warn("Context: " + connection.inFlightDeliveries() + " handler(s) still running after "
                    + Config.shutdownDrainTimeoutMs() + "ms; closing anyway");
        }
        connection.shutdownExecutors();
    }
}
