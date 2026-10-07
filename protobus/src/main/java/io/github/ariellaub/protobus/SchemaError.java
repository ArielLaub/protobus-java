package io.github.ariellaub.protobus;

/**
 * A schema could not be loaded: an unreadable descriptor set, or a type that does not resolve.
 */
public class SchemaError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public SchemaError(String message) {
        super(message, null);
    }

    public SchemaError(String message, Throwable cause) {
        super(message, null, cause);
    }
}
