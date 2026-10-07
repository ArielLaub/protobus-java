package io.github.ariellaub.protobus;

/**
 * A listener that is already consuming was started again.
 */
public class AlreadyStartedError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public AlreadyStartedError() {
        super(null, null);
    }

    public AlreadyStartedError(String message) {
        super(message, null);
    }
}
