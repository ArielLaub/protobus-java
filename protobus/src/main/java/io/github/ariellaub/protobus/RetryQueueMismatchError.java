package io.github.ariellaub.protobus;

/**
 * The retry queue exists with arguments that differ from this service's configuration,
 * in practice a changed {@code retryDelayMs}. RabbitMQ cannot change a queue's
 * {@code x-message-ttl} in place.
 */
public class RetryQueueMismatchError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public RetryQueueMismatchError(String message) {
        super(message, null);
    }

    public RetryQueueMismatchError(String message, Throwable cause) {
        super(message, null, cause);
    }
}
