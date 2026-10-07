package io.github.ariellaub.protobus;

/**
 * No streaming chunk arrived within the idle timeout. The total-call timeout does not
 * apply to streams: a stream may take far longer than any single gap between chunks.
 */
public class StreamTimeoutError extends StreamingError {
    private static final long serialVersionUID = 1L;

    public StreamTimeoutError(String message) {
        super(message);
    }
}
