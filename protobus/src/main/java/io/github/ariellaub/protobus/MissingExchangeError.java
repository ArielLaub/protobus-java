package io.github.ariellaub.protobus;

/**
 * A listener was initialised without an exchange to bind to.
 */
public class MissingExchangeError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public MissingExchangeError() {
        super(null, null);
    }

    public MissingExchangeError(String message) {
        super(message, null);
    }
}
