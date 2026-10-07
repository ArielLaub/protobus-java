package io.github.ariellaub.protobus;

/**
 * A component was used before its {@code init()}.
 */
public class NotInitializedError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public NotInitializedError() {
        super(null, null);
    }

    public NotInitializedError(String message) {
        super(message, null);
    }
}
