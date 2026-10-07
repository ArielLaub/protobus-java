package io.github.ariellaub.protobus;

/**
 * An expected failure that is answered, never retried.
 *
 * When a service method throws a HandledError (or a subclass), the caller receives it
 * with its message and code, and the request is settled: no retry, no dead-lettering.
 * Use it for validation and business-rule failures.
 *
 * <pre>{@code
 * final class ValidationError extends HandledError {
 *     ValidationError(String message) { super(message, "VALIDATION_ERROR"); }
 * }
 * }</pre>
 */
public class HandledError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public HandledError(String message) {
        this(message, "HANDLED_ERROR");
    }

    public HandledError(String message, String code) {
        super(message, code == null || code.isEmpty() ? "HANDLED_ERROR" : code);
    }

    public HandledError(String message, String code, Throwable cause) {
        super(message, code == null || code.isEmpty() ? "HANDLED_ERROR" : code, cause);
    }
}
