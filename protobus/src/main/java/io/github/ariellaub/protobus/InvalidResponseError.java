package io.github.ariellaub.protobus;

/**
 * A reply could not be decoded, or carried neither a result nor an error.
 */
public class InvalidResponseError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidResponseError() {
        super(null, null);
    }

    public InvalidResponseError(String message) {
        super(message, null);
    }
}
