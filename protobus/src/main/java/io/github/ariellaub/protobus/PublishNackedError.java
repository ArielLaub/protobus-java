package io.github.ariellaub.protobus;

/**
 * The broker explicitly refused the message (basic.nack). A definite negative outcome:
 * the message was not stored, and republishing is safe.
 */
public class PublishNackedError extends PublishError {
    private static final long serialVersionUID = 1L;

    public PublishNackedError(String message, String messageId) {
        super(message, "PUBLISH_NACKED", messageId);
    }
}
