package io.github.ariellaub.protobus;

/**
 * One structured log line, as {@link Log} builds it. Every text field is
 * sanitised: control characters become spaces and the value is capped at 256
 * characters (the message at 1024). Absent fields are null.
 *
 * @param component always {@code protobus}
 * @param level debug, info, warn or error
 * @param timestamp ISO-8601, UTC
 * @param diagnostics what the {@link Log.DiagnosticsSerializer} made of the call's
 *     diagnostics, or null when there is no serializer
 */
public record LogRecord(
        String component,
        String level,
        String timestamp,
        String operation,
        String message,
        String messageType,
        String messageId,
        String correlationId,
        String service,
        String method,
        String queue,
        String exchange,
        String routingKey,
        String errorCode,
        String errorName,
        String outcome,
        Long sizeBytes,
        Long durationMs,
        Long attempt,
        Object diagnostics) {

    LogRecord withDiagnostics(Object value) {
        return new LogRecord(component, level, timestamp, operation, message, messageType, messageId,
                correlationId, service, method, queue, exchange, routingKey, errorCode, errorName, outcome,
                sizeBytes, durationMs, attempt, value);
    }

    /** The record as one line: {@code [protobus] operation: message (key=value ...)}. */
    public String format() {
        StringBuilder detail = new StringBuilder();
        append(detail, "messageType", messageType);
        append(detail, "messageId", messageId);
        append(detail, "correlationId", correlationId);
        append(detail, "service", service);
        append(detail, "method", method);
        append(detail, "queue", queue);
        append(detail, "exchange", exchange);
        append(detail, "routingKey", routingKey);
        append(detail, "errorCode", errorCode);
        append(detail, "errorName", errorName);
        append(detail, "outcome", outcome);
        append(detail, "sizeBytes", sizeBytes);
        append(detail, "durationMs", durationMs);
        append(detail, "attempt", attempt);
        if (diagnostics != null) append(detail, "diagnostics", String.valueOf(diagnostics));
        return "[" + component + "] " + operation + ": " + message
                + (detail.length() > 0 ? " (" + detail + ")" : "");
    }

    private static void append(StringBuilder out, String key, Object value) {
        if (value == null) return;
        if (out.length() > 0) out.append(' ');
        out.append(key).append('=').append(value);
    }
}
