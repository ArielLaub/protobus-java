package io.github.ariellaub.protobus;

/**
 * The request named a method this service does not serve, or one that contradicts the
 * routing key it arrived on. A ProtocolError, so the caller is answered instead of the
 * request being retried.
 */
public class InvalidMethodError extends ProtocolError {
    private static final long serialVersionUID = 1L;

    public InvalidMethodError(String message) {
        super(message);
    }
}
