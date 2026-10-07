package io.github.ariellaub.protobus;

/**
 * The channel closed while a publish was awaiting its confirm. Like a confirm timeout
 * this is an AMBIGUOUS outcome, not a definite failure.
 */
public class ChannelClosedError extends PublishError {
    private static final long serialVersionUID = 1L;

    public ChannelClosedError(String message, String messageId) {
        super(message, "CHANNEL_CLOSED", messageId);
    }
}
