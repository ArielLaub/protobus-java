package io.github.ariellaub.protobus;

/**
 * A unary call got no reply within its timeout: nothing was bound to the routing key,
 * the exchange dropped the message, or the handler died without replying.
 */
public class RpcTimeoutError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public RpcTimeoutError(String message) {
        super(message, "RPC_TIMEOUT");
    }
}
