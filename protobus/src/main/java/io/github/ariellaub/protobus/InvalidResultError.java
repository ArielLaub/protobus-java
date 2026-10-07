package io.github.ariellaub.protobus;

/**
 * A handler returned something that is not of its method's response type.
 */
public class InvalidResultError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidResultError() {
        super(null, null);
    }

    public InvalidResultError(String message) {
        super(message, null);
    }
}
