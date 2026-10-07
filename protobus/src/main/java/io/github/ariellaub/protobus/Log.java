package io.github.ariellaub.protobus;

import java.time.Instant;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Structured counterpart to {@link Logger}: an event described as data, with the
 * sink deciding its shape.
 *
 * A {@link LogSink.Structured} sink receives the {@link LogRecord}; any other sink
 * receives {@link LogRecord#format()} on the matching severity. Level filtering
 * happens first either way.
 *
 * <pre>{@code
 * Log.info("published request", Log.fields("publish")
 *         .messageType("example.Service.doThing")
 *         .correlationId(id)
 *         .sizeBytes(body.length)
 *         .outcome(Log.Outcome.CONFIRMED));
 * }</pre>
 *
 * Diagnostics are opt-in and lazy: the supplier passed to
 * {@link Fields#diagnostics} runs only when a {@link DiagnosticsSerializer} is
 * installed, and whatever the serializer returns is the only form that reaches
 * the record.
 */
public final class Log {
    private Log() {}

    /** What an operation came to. */
    public enum Outcome {
        OK, CONFIRMED, FAILED, TIMEOUT, RETRIED, REJECTED, DROPPED, UNROUTABLE;

        String wire() {
            return name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    /** Turns a call's raw diagnostics into what the record carries. Return null to omit them. */
    @FunctionalInterface
    public interface DiagnosticsSerializer extends BiFunction<Map<String, Object>, LogRecord, Object> {}

    private static volatile DiagnosticsSerializer serializer;

    public static void setDiagnosticsSerializer(DiagnosticsSerializer value) {
        serializer = value;
    }

    public static DiagnosticsSerializer getDiagnosticsSerializer() {
        return serializer;
    }

    public static Fields fields(String operation) {
        return new Fields(operation);
    }

    public static void debug(String message, Fields fields) {
        emit("debug", LogLevel.DEBUG, message, fields);
    }

    public static void info(String message, Fields fields) {
        emit("info", LogLevel.INFO, message, fields);
    }

    public static void warn(String message, Fields fields) {
        emit("warn", LogLevel.WARN, message, fields);
    }

    public static void error(String message, Fields fields) {
        emit("error", LogLevel.ERROR, message, fields);
    }

    private static final int FIELD_MAX = 256;
    private static final int MESSAGE_MAX = 1024;

    static String sanitize(Object value, int max) {
        if (value == null) return null;
        String text;
        if (value instanceof String) text = (String) value;
        else if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            text = Double.isFinite(d) ? String.valueOf(value) : "";
        } else if (value instanceof Number || value instanceof Boolean) text = String.valueOf(value);
        else return null;
        text = text.replaceAll("[\\x00-\\x1f\\x7f]+", " ").trim();
        if (text.isEmpty()) return null;
        return text.length() > max ? text.substring(0, max) : text;
    }

    static LogRecord build(String level, String message, Fields f) {
        String op = sanitize(f.operation, FIELD_MAX);
        String msg = sanitize(message, MESSAGE_MAX);
        return new LogRecord("protobus", level, Instant.now().toString(), op == null ? "unknown" : op,
                msg == null ? "" : msg, sanitize(f.messageType, FIELD_MAX), sanitize(f.messageId, FIELD_MAX),
                sanitize(f.correlationId, FIELD_MAX), sanitize(f.service, FIELD_MAX), sanitize(f.method, FIELD_MAX),
                sanitize(f.queue, FIELD_MAX), sanitize(f.exchange, FIELD_MAX), sanitize(f.routingKey, FIELD_MAX),
                sanitize(f.errorCode, FIELD_MAX), sanitize(f.errorName, FIELD_MAX),
                f.outcome == null ? null : f.outcome.wire(), f.sizeBytes, f.durationMs, f.attempt, null);
    }

    private static void emit(String levelName, LogLevel threshold, String message, Fields fields) {
        if (!Logger.enabled(threshold)) return;
        LogRecord record = build(levelName, message, fields == null ? new Fields(null) : fields);
        DiagnosticsSerializer s = serializer;
        if (s != null && fields != null && fields.diagnostics != null) {
            try {
                Object extra = s.apply(fields.diagnostics.get(), record);
                if (extra != null) record = record.withDiagnostics(extra);
            } catch (RuntimeException ignored) {
                // A failing hook must not take down the operation being logged,
                // nor swallow the line itself.
            }
        }
        LogSink target = Logger.sink();
        if (target instanceof LogSink.Structured) {
            try {
                ((LogSink.Structured) target).log(record);
                return;
            } catch (RuntimeException ignored) {
                // A structured sink that throws degrades to the text path.
            }
        }
        String text = record.format();
        switch (levelName) {
            case "debug": target.debug(text); break;
            case "info": target.info(text); break;
            case "warn": target.warn(text); break;
            default: target.error(text); break;
        }
    }

    /** The fields of one structured line. Every setter returns this. */
    public static final class Fields {
        final String operation;
        String messageType;
        String messageId;
        String correlationId;
        String service;
        String method;
        String queue;
        String exchange;
        String routingKey;
        String errorCode;
        String errorName;
        Outcome outcome;
        Long sizeBytes;
        Long durationMs;
        Long attempt;
        Supplier<Map<String, Object>> diagnostics;

        Fields(String operation) {
            this.operation = operation;
        }

        public Fields messageType(String v) { messageType = v; return this; }
        public Fields messageId(String v) { messageId = v; return this; }
        public Fields correlationId(String v) { correlationId = v; return this; }
        public Fields service(String v) { service = v; return this; }
        public Fields method(String v) { method = v; return this; }
        public Fields queue(String v) { queue = v; return this; }
        public Fields exchange(String v) { exchange = v; return this; }
        public Fields routingKey(String v) { routingKey = v; return this; }
        public Fields errorCode(String v) { errorCode = v; return this; }
        public Fields errorName(String v) { errorName = v; return this; }
        public Fields outcome(Outcome v) { outcome = v; return this; }
        public Fields sizeBytes(long v) { sizeBytes = v; return this; }
        public Fields durationMs(long v) { durationMs = v; return this; }
        public Fields attempt(long v) { attempt = v; return this; }
        /** Raw diagnostics (payload, headers, error), built only when a serializer is installed. */
        public Fields diagnostics(Supplier<Map<String, Object>> v) { diagnostics = v; return this; }
    }
}
