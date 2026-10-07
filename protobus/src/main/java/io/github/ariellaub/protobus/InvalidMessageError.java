package io.github.ariellaub.protobus;

/**
 * An event could not be encoded.
 */
public class InvalidMessageError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidMessageError() {
        super(null, null);
    }

    public InvalidMessageError(String message) {
        super(message, null);
    }
}
