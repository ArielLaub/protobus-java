package io.github.ariellaub.protobus;

import java.util.Map;

/** What a service handler receives besides its request. */
public final class CallContext {
    private final String actor;
    private final String correlationId;
    private final String method;
    private final Connection.MessageHandlerContext delivery;

    CallContext(String actor, String correlationId, String method, Connection.MessageHandlerContext delivery) {
        this.actor = actor;
        this.correlationId = correlationId;
        this.method = method;
        this.delivery = delivery;
    }

    /** Free-text caller identity from the request envelope; empty when none. Not authenticated. */
    public String actor() {
        return actor;
    }

    public String correlationId() {
        return correlationId;
    }

    /** The contract method, {@code <package>.<Service>.<method>}. */
    public String method() {
        return method;
    }

    /**
     * Fires on the processing timeout and, for a stream, when the caller cancels.
     * Watch it in long or cancellable work: the handler's thread is never
     * interrupted.
     */
    public AbortSignal signal() {
        return delivery.signal;
    }

    /** The routing key the broker delivered on. */
    public String routingKey() {
        return delivery.routingKey;
    }

    /** Stable across redeliveries and retries: deduplicate on it. Null if the publisher set none. */
    public String messageId() {
        return delivery.messageId;
    }

    public boolean redelivered() {
        return delivery.redelivered;
    }

    public Map<String, Object> headers() {
        return delivery.headers;
    }
}
