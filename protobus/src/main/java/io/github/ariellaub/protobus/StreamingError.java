package io.github.ariellaub.protobus;

/** The base of the failures a streaming call can raise on the caller's side. */
public class StreamingError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public StreamingError(String message) {
        super(message, null);
    }
}
