package io.github.ariellaub.protobus;

/**
 * An error a service answered with, as its caller receives it.
 *
 * Only the message, the code and the method cross the wire; the service's
 * exception type and stack stay in the service. {@link #code()} is the code the
 * service sent ({@code HANDLED_ERROR}, {@code PROTOCOL_ERROR},
 * {@code INTERNAL_ERROR}, {@code PROCESSING_TIMEOUT}, or one of its own), or null
 * when it sent none.
 */
public class RemoteError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    private final String method;

    public RemoteError(String message, String code, String method) {
        super(message, code == null || code.isEmpty() ? null : code);
        this.method = method;
    }

    /** The method the error was reported against. */
    public String method() {
        return method;
    }
}
