package io.github.ariellaub.protobus;

/**
 * A request could not be encoded.
 */
public class InvalidRequestError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidRequestError() {
        super(null, null);
    }

    public InvalidRequestError(String message) {
        super(message, null);
    }
}
