package io.github.ariellaub.protobus;

/**
 * A well-formed method name whose method the named service does not declare.
 */
public class UnknownMethodError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public UnknownMethodError() {
        super(null, null);
    }

    public UnknownMethodError(String message) {
        super(message, null);
    }
}
