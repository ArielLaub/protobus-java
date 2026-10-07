package io.github.ariellaub.protobus;

/**
 * A caller-supplied messageId that cannot identify anything: blank, or longer than the
 * 255 bytes AMQP's shortstr carries.
 */
public class InvalidMessageIdError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidMessageIdError(String message) {
        super(message, null);
    }

    public InvalidMessageIdError(String message, Throwable cause) {
        super(message, null, cause);
    }
}
