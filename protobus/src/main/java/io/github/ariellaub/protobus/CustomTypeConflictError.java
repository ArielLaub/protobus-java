package io.github.ariellaub.protobus;

/**
 * A custom type name was registered again with a different wire type. Registration is
 * idempotent for an identical definition; this is the one case that cannot be.
 */
public class CustomTypeConflictError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public CustomTypeConflictError(String message) {
        super(message, null);
    }

    public CustomTypeConflictError(String message, Throwable cause) {
        super(message, null, cause);
    }
}
