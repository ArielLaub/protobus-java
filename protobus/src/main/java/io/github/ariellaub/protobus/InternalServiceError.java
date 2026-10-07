package io.github.ariellaub.protobus;

/**
 * Substituted for an unhandled service error before it crosses back to the caller,
 * unless {@link Config#exposeInternalErrors()} is on (the default).
 *
 * Carries the correlation id, so an operator can join the caller's report to the real
 * exception in the service's own log.
 */
public class InternalServiceError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InternalServiceError(String correlationId) {
        super(correlationId == null || correlationId.isEmpty()
                ? "internal service error"
                : "internal service error (correlationId " + correlationId + ")", "INTERNAL_ERROR");
    }
}
