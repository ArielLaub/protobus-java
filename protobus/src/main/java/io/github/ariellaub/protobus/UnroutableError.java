package io.github.ariellaub.protobus;

/**
 * A mandatory publish reached the exchange but matched no queue, so the broker
 * returned it. For an RPC request this usually means no service is bound to the routing
 * key: worth failing fast on rather than waiting out the full RPC timeout.
 */
public class UnroutableError extends PublishError {
    private static final long serialVersionUID = 1L;

    public UnroutableError(String message, String messageId) {
        super(message, "UNROUTABLE", messageId);
    }
}
