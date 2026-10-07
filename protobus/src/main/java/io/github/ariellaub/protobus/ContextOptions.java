package io.github.ariellaub.protobus;

import io.github.ariellaub.protobus.amqp.Transport;
import java.util.concurrent.ExecutorService;

/** How a {@link Context} connects and runs handlers. Immutable; each {@code with} method returns a copy. */
public final class ContextOptions {
    public static final ContextOptions DEFAULT = new ContextOptions(null, null, null);

    private final Connection.ReconnectionOptions reconnection;
    private final Transport transport;
    private final ExecutorService handlerExecutor;

    private ContextOptions(Connection.ReconnectionOptions reconnection, Transport transport,
                           ExecutorService handlerExecutor) {
        this.reconnection = reconnection;
        this.transport = transport;
        this.handlerExecutor = handlerExecutor;
    }

    /** Reconnection backoff; null for {@link Connection.ReconnectionOptions#defaults()}. */
    public Connection.ReconnectionOptions reconnection() {
        return reconnection;
    }

    /** The AMQP transport; null for RabbitMQ. Tests pass a {@link io.github.ariellaub.protobus.testing.MemoryBroker}. */
    public Transport transport() {
        return transport;
    }

    /**
     * Where service handlers run; null for an internal pool of daemon threads. On
     * Java 21+, {@code Executors.newVirtualThreadPerTaskExecutor()} suits handlers
     * that block. The context does not shut down an executor it was given.
     */
    public ExecutorService handlerExecutor() {
        return handlerExecutor;
    }

    public ContextOptions withReconnection(Connection.ReconnectionOptions value) {
        return new ContextOptions(value, transport, handlerExecutor);
    }

    public ContextOptions withTransport(Transport value) {
        return new ContextOptions(reconnection, value, handlerExecutor);
    }

    public ContextOptions withHandlerExecutor(ExecutorService value) {
        return new ContextOptions(reconnection, transport, value);
    }
}
