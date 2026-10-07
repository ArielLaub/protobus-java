package io.github.ariellaub.protobus;

/**
 * The connection is not carrying traffic: it is reconnecting past
 * {@link Config#connectionReadyTimeoutMs()}, has been closed, or has given up. Nothing was
 * attempted.
 */
public class NotReadyError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public NotReadyError(String message) {
        super(message, "NOT_READY");
    }
}
