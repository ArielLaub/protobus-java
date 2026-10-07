package io.github.ariellaub.protobus;

/**
 * A stream's buffer exceeded its chunk or byte bound: the consumer is not keeping up
 * with the producer. Failing the stream is recoverable; running out of memory is not.
 */
public class StreamBackpressureError extends StreamingError {
    private static final long serialVersionUID = 1L;

    public StreamBackpressureError(String message) {
        super(message);
    }
}
