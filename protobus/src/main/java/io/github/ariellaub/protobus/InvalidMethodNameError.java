package io.github.ariellaub.protobus;

/**
 * A name that is not of the form {@code <package>.<Service>.<method>}.
 */
public class InvalidMethodNameError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidMethodNameError() {
        super(null, null);
    }

    public InvalidMethodNameError(String message) {
        super(message, null);
    }
}
