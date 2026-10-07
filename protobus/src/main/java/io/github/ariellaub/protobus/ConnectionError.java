package io.github.ariellaub.protobus;

/**
 * A listener was initialised on a connection that is not connected.
 */
public class ConnectionError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public ConnectionError() {
        super(null, null);
    }

    public ConnectionError(String message) {
        super(message, null);
    }
}
