package io.github.ariellaub.protobus;

/**
 * The message could not be understood: it did not decode, or it named something this
 * service does not serve.
 *
 * Handled by definition. A malformed message is malformed on every delivery, so the
 * retry ladder would buy three more identical failures and a dead-letter entry while
 * the caller waits for a reply the retries were never going to produce.
 */
public class ProtocolError extends HandledError {
    private static final long serialVersionUID = 1L;

    public ProtocolError(String message) {
        super(message, "PROTOCOL_ERROR");
    }
}
