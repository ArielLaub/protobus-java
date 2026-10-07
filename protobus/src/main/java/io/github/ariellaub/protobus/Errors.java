package io.github.ariellaub.protobus;

/** How errors are classified, and what of them may leave the process. */
public final class Errors {
    private Errors() {}

    /** True for an expected failure that is answered rather than retried. */
    public static boolean isHandledError(Throwable error) {
        return error instanceof HandledError;
    }

    /** The error's code, or null when it carries none. */
    public static String codeOf(Throwable error) {
        return error instanceof ProtobusException ? ((ProtobusException) error).code() : null;
    }

    /**
     * The error's name as it appears in logs and retry headers: its simple class
     * name, matching what the other ports write for their own errors.
     */
    public static String nameOf(Throwable error) {
        if (error == null) return "UnknownError";
        String name = error.getClass().getSimpleName();
        return name.isEmpty() ? error.getClass().getName() : name;
    }

    /**
     * Decide what an error looks like to the caller, as the message and code the
     * reply carries.
     *
     * A HandledError is something the service chose to expose and passes through.
     * A processing timeout passes through too: its message names only the limit.
     * Anything else is an internal failure whose message was written for the
     * service's own log and may quote the data that caused it; unless
     * {@link Config#exposeInternalErrors()} is on, it becomes a generic
     * {@link InternalServiceError} naming the correlation id.
     */
    public static Throwable sanitizeErrorForClient(Throwable error, String correlationId) {
        if (isHandledError(error) || error instanceof TimeoutError) return error;
        if (Config.exposeInternalErrors()) return error;
        return new InternalServiceError(correlationId);
    }

    /**
     * A non-disclosing description of an error: its name and code, never its
     * message.
     *
     * Exception messages routinely interpolate the values that caused them. This
     * summary goes where text travels further than the process (the
     * {@code x-last-error} header on retry and dead-letter copies), so only a
     * HandledError, which is by definition meant to be seen, keeps its message.
     */
    public static String safeErrorSummary(Throwable error) {
        if (error == null) return "UnknownError";
        String name = nameOf(error);
        String code = codeOf(error);
        if (isHandledError(error)) {
            return name + "[" + code + "]: " + error.getMessage();
        }
        return code == null || code.isEmpty() ? name : name + "[" + code + "]";
    }

    /** The message of an error, never null. */
    static String messageOf(Throwable error) {
        if (error == null) return "unknown error";
        String message = error.getMessage();
        return message == null ? nameOf(error) : message;
    }
}
