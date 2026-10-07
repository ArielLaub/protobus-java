package io.github.ariellaub.protobus;

/**
 * A custom-type value outside what the wire format carries: a negative or oversized
 * {@code bigint}, a {@code bigint} wider than 32 bytes, or a {@code timestamp} beyond
 * ±8.64e15 ms.
 */
public class CustomTypeRangeError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public CustomTypeRangeError(String message) {
        super(message, null);
    }

    public CustomTypeRangeError(String message, Throwable cause) {
        super(message, null, cause);
    }
}
