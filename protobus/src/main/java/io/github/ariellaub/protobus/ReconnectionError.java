package io.github.ariellaub.protobus;

/**
 * A reconnection attempt failed, or the connection gave up reconnecting.
 */
public class ReconnectionError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public ReconnectionError(String message) {
        super(message, null);
    }

    public ReconnectionError(String message, Throwable cause) {
        super(message, null, cause);
    }
}
