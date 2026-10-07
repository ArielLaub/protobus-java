package io.github.ariellaub.protobus;

/**
 * A {@code maxPriority} or per-message priority that AMQP cannot carry, refused before
 * anything reaches the broker. An out-of-range {@code x-max-priority} would otherwise be a
 * 406 that closes the declaring channel.
 */
public class InvalidPriorityError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidPriorityError(String message) {
        super(message, null);
    }

    public InvalidPriorityError(String message, Throwable cause) {
        super(message, null, cause);
    }
}
