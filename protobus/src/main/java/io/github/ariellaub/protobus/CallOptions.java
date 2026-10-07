package io.github.ariellaub.protobus;

/**
 * Options for a unary call or a fire-and-forget publish. Immutable; each
 * {@code with} method returns a copy.
 *
 * <pre>{@code
 * calculator.add(request, CallOptions.DEFAULT.withActor("billing").withTimeoutMs(5000));
 * }</pre>
 */
public final class CallOptions {
    public static final CallOptions DEFAULT = new CallOptions(null, true, null, null, null);

    private final String actor;
    private final boolean rpc;
    private final Long timeoutMs;
    private final Integer priority;
    private final String messageId;

    private CallOptions(String actor, boolean rpc, Long timeoutMs, Integer priority, String messageId) {
        this.actor = actor;
        this.rpc = rpc;
        this.timeoutMs = timeoutMs;
        this.priority = priority;
        this.messageId = messageId;
    }

    /** Free-text caller identity, carried in the request envelope for tracing. Not authenticated by anything. */
    public String actor() {
        return actor;
    }

    /** false: publish without waiting for a reply; the call returns once the broker confirms the request. */
    public boolean rpc() {
        return rpc;
    }

    /**
     * How long the call may take; null for {@link Config#rpcCallTimeoutMs()}. The
     * deadline starts once the connection is ready to publish and bounds the broker
     * confirm as well as the reply.
     */
    public Long timeoutMs() {
        return timeoutMs;
    }

    /**
     * AMQP message priority, 0-255. Only takes effect on a queue declared with
     * {@code maxPriority}; the broker ignores it elsewhere.
     */
    public Integer priority() {
        return priority;
    }

    /**
     * The message's identity, as the consumer sees it. Null for a fresh UUID. Set
     * it to make a caller-driven republish recognisable after an AMBIGUOUS failure
     * ({@link PublishConfirmTimeoutError}, {@link ChannelClosedError}): derive it
     * from the work (an order id), never from a clock. Refused when blank or longer
     * than 255 bytes.
     */
    public String messageId() {
        return messageId;
    }

    public CallOptions withActor(String value) {
        return new CallOptions(value, rpc, timeoutMs, priority, messageId);
    }

    public CallOptions withRpc(boolean value) {
        return new CallOptions(actor, value, timeoutMs, priority, messageId);
    }

    public CallOptions withTimeoutMs(long value) {
        return new CallOptions(actor, rpc, value, priority, messageId);
    }

    public CallOptions withPriority(int value) {
        return new CallOptions(actor, rpc, timeoutMs, value, messageId);
    }

    public CallOptions withMessageId(String value) {
        return new CallOptions(actor, rpc, timeoutMs, priority, value);
    }
}
