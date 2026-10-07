package io.github.ariellaub.protobus;

/**
 * {@code init()} was called a second time.
 */
public class AlreadyInitializedError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public AlreadyInitializedError() {
        super(null, null);
    }

    public AlreadyInitializedError(String message) {
        super(message, null);
    }
}
