package io.github.ariellaub.protobus;

/**
 * The connection was lost, or the context closed, while a call or stream was pending.
 * The request may or may not have been processed.
 */
public class DisconnectedError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public DisconnectedError() {
        this("Connection lost during RPC call");
    }

    public DisconnectedError(String message) {
        super(message, null);
    }
}
