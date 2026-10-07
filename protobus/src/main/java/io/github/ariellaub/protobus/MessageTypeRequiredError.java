package io.github.ariellaub.protobus;

/**
 * A decode was requested without naming the message type.
 */
public class MessageTypeRequiredError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public MessageTypeRequiredError() {
        super(null, null);
    }

    public MessageTypeRequiredError(String message) {
        super(message, null);
    }
}
