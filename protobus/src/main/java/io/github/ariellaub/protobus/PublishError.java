package io.github.ariellaub.protobus;

/**
 * A publish that did not demonstrably reach a queue. Each subclass is a distinct
 * outcome; {@link PublishConfirmTimeoutError} and {@link ChannelClosedError} are
 * AMBIGUOUS (the broker may have stored the message), the others definite.
 *
 * {@link #messageId()} is the publish's identity, stable across a caller's
 * republish of the same logical message when it passes the same id, so a consumer
 * can deduplicate on it.
 */
public class PublishError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    private final String messageId;

    public PublishError(String message, String code, String messageId) {
        super(message, code);
        this.messageId = messageId;
    }

    /** The messageId the publish carried. */
    public String messageId() {
        return messageId;
    }
}
