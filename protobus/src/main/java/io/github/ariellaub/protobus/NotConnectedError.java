package io.github.ariellaub.protobus;

/**
 * Something that needs a connection was attempted without one, and none is being re-established.
 */
public class NotConnectedError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public NotConnectedError() {
        super(null, null);
    }

    public NotConnectedError(String message) {
        super(message, null);
    }
}
