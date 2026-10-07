package io.github.ariellaub.protobus;

/**
 * The base of every error protobus raises.
 *
 * Unchecked, like the rest of the API. {@link #code()} is the stable,
 * cross-language identifier of the failure ({@code RPC_TIMEOUT},
 * {@code UNROUTABLE}, a service's own {@code VALIDATION_ERROR}, ...), or null
 * for errors that carry none.
 */
public class ProtobusException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String code;

    public ProtobusException(String message, String code) {
        super(message);
        this.code = code;
    }

    public ProtobusException(String message, String code, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    /** The error's code, or null when it has none. */
    public String code() {
        return code;
    }
}
