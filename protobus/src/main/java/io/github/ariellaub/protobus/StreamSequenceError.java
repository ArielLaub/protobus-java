package io.github.ariellaub.protobus;

/**
 * A streaming reply arrived with a gap in its sequence numbers, so at least one chunk
 * was lost. Failing is deliberate: yielding what did arrive would hand the caller a short
 * stream that looks complete.
 */
public class StreamSequenceError extends StreamingError {
    private static final long serialVersionUID = 1L;

    public StreamSequenceError(String message) {
        super(message);
    }
}
