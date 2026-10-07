package io.github.ariellaub.protobus;

/**
 * No confirm arrived within {@link Config#publishConfirmTimeoutMs()}. The outcome is
 * UNKNOWN: the broker may have stored the message and lost only the confirm, so a retry
 * can duplicate it. Consumers must be idempotent; republish with the same messageId.
 */
public class PublishConfirmTimeoutError extends PublishError {
    private static final long serialVersionUID = 1L;

    public PublishConfirmTimeoutError(String message, String messageId) {
        super(message, "PUBLISH_CONFIRM_TIMEOUT", messageId);
    }
}
