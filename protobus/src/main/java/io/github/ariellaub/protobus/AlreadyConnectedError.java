package io.github.ariellaub.protobus;

/**
 * {@link Connection#connect} was called on a connection that is already connected.
 */
public class AlreadyConnectedError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public AlreadyConnectedError() {
        super(null, null);
    }

    public AlreadyConnectedError(String message) {
        super(message, null);
    }
}
