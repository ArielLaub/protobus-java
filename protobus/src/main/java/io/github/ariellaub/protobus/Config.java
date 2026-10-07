package io.github.ariellaub.protobus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Environment-backed configuration, mirroring the TypeScript, Python, Go and C++
 * {@code Config}.
 *
 * Every getter reads its variable on each call (memoised per raw value), so a
 * value changed at runtime is picked up. Integer parsing is strict: the trimmed
 * value must be all digits and positive, or the default is kept; a typo never
 * becomes a surprising zero. A boolean must be one of 1/true/yes/on or
 * 0/false/no/off, case-insensitive.
 *
 * {@link #set(String, String)} overrides a variable inside the process, for tests
 * and for applications that cannot set their environment.
 *
 * The exchange names are part of the wire protocol: every process on one bus,
 * whatever its language, must agree on them.
 */
public final class Config {
    private Config() {}

    /** Named message priorities, matching the other ports. RabbitMQ sorts a message with no priority as 0. */
    public static final int PRIORITY_NORMAL = 0;
    public static final int PRIORITY_HIGH = 1;
    public static final int PRIORITY_CONTROL = 2;
    /**
     * The {@code maxPriority} that gives the three levels above. RabbitMQ keeps
     * internal structures per level, so keep the range small.
     */
    public static final int RECOMMENDED_MAX_PRIORITY = 2;

    /** Headers of the server-streaming wire protocol. */
    public static final String HEADER_FINAL = "x-protobus-final";
    public static final String HEADER_SEQ = "x-protobus-seq";

    private static final Map<String, String> overrides = new ConcurrentHashMap<>();
    private static final Map<String, long[]> intCache = new ConcurrentHashMap<>();
    private static final Map<String, String> intRaw = new ConcurrentHashMap<>();
    private static final Pattern DIGITS = Pattern.compile("\\d+");
    private static final String UNSET = "\u0000unset";

    /** Override a variable for this process; null removes the override. */
    public static void set(String name, String value) {
        if (value == null) overrides.remove(name);
        else overrides.put(name, value);
    }

    /** Remove every override. */
    public static void reset() {
        overrides.clear();
    }

    static String raw(String name) {
        String value = overrides.get(name);
        return value != null ? value : System.getenv(name);
    }

    static long envInt(String name, long fallback) {
        String raw = raw(name);
        String key = raw == null ? UNSET : raw;
        if (key.equals(intRaw.get(name))) {
            long[] hit = intCache.get(name);
            if (hit != null) return hit[0];
        }
        long value = fallback;
        if (raw != null) {
            String t = raw.trim();
            if (!t.isEmpty() && DIGITS.matcher(t).matches()) {
                try {
                    long parsed = Long.parseLong(t);
                    // The TypeScript reference refuses anything beyond
                    // Number.MAX_SAFE_INTEGER; so does every port.
                    if (parsed > 0 && parsed <= 9007199254740991L) value = parsed;
                } catch (NumberFormatException ignored) {
                    // Out of range for a long: keep the default.
                }
            }
        }
        intCache.put(name, new long[] {value});
        intRaw.put(name, key);
        return value;
    }

    static boolean envBool(String name, boolean fallback) {
        String raw = raw(name);
        if (raw == null || raw.trim().isEmpty()) return fallback;
        switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "1": case "true": case "yes": case "on": return true;
            case "0": case "false": case "no": case "off": return false;
            default: return fallback;
        }
    }

    static String envString(String name, String fallback) {
        String raw = raw(name);
        return raw == null || raw.isEmpty() ? fallback : raw;
    }

    /**
     * Send the message of an UNHANDLED service error back to the caller. On by
     * default: a protobus caller is another of your own services, already inside
     * the trust boundary. Turn it off ({@code PROTOBUS_EXPOSE_INTERNAL_ERRORS=false})
     * for a service whose callers relay errors to untrusted clients; they then see
     * a generic message naming the correlation id, and the real error stays in the
     * service's log. A HandledError always crosses.
     */
    public static boolean exposeInternalErrors() {
        return envBool("PROTOBUS_EXPOSE_INTERNAL_ERRORS", true);
    }

    /** RPC requests (topic). {@code BUS_EXCHANGE_NAME}, default {@code proto.bus}. */
    public static String busExchangeName() {
        return envString("BUS_EXCHANGE_NAME", "proto.bus");
    }

    /** RPC replies (direct). {@code CALLBACKS_EXCHANGE_NAME}, default {@code proto.bus.callback}. */
    public static String callbacksExchangeName() {
        return envString("CALLBACKS_EXCHANGE_NAME", "proto.bus.callback");
    }

    /**
     * Stream-cancellation notices (fanout). {@code CANCEL_EXCHANGE_NAME}, default
     * {@code proto.bus.cancel}. Fanout, because a cancel has to reach the one
     * replica running that stream and the caller cannot know which one it is.
     */
    public static String cancelExchangeName() {
        return envString("CANCEL_EXCHANGE_NAME", "proto.bus.cancel");
    }

    /** Events (topic). {@code EVENTS_EXCHANGE_NAME}, default {@code proto.bus.events}. */
    public static String eventsExchangeName() {
        return envString("EVENTS_EXCHANGE_NAME", "proto.bus.events");
    }

    /**
     * How long a service spends on one unary request before the attempt is failed
     * and retried. {@code MESSAGE_PROCESSING_TIMEOUT}, default 600000 ms.
     */
    public static long messageProcessingTimeout() {
        return envInt("MESSAGE_PROCESSING_TIMEOUT", 600000);
    }

    /** How long a unary caller waits for its reply. {@code RPC_CALL_TIMEOUT_MS}, default 600000 ms. */
    public static long rpcCallTimeoutMs() {
        return envInt("RPC_CALL_TIMEOUT_MS", 600000);
    }

    /** The longest gap a streaming caller tolerates between chunks. {@code STREAM_IDLE_TIMEOUT_MS}, default 60000 ms. */
    public static long streamIdleTimeoutMs() {
        return envInt("STREAM_IDLE_TIMEOUT_MS", 60000);
    }

    /** Prefetch for late-ack consumers that set no concurrency of their own. {@code DEFAULT_PREFETCH}, default 1. */
    public static long defaultPrefetch() {
        return envInt("DEFAULT_PREFETCH", 1);
    }

    /**
     * How long a publish waits for its broker confirm. Expiry is AMBIGUOUS: the
     * broker may have stored the message. {@code PUBLISH_CONFIRM_TIMEOUT_MS},
     * default 30000 ms.
     */
    public static long publishConfirmTimeoutMs() {
        return envInt("PUBLISH_CONFIRM_TIMEOUT_MS", 30000);
    }

    /**
     * AMQP heartbeat interval, in seconds. A {@code heartbeat} parameter in the
     * broker URL wins, and {@code heartbeat=0} there disables heartbeats.
     * {@code AMQP_HEARTBEAT_SECONDS}, default 30.
     */
    public static long heartbeatSeconds() {
        return envInt("AMQP_HEARTBEAT_SECONDS", 30);
    }

    /**
     * How long a publish parked on a reconnection waits before failing with
     * NotReadyError. {@code CONNECTION_READY_TIMEOUT_MS}, default 30000 ms.
     */
    public static long connectionReadyTimeoutMs() {
        return envInt("CONNECTION_READY_TIMEOUT_MS", 30000);
    }

    /**
     * Publishes awaiting a broker confirm on one channel at a time; further ones
     * wait for a slot. {@code MAX_OUTSTANDING_CONFIRMS}, default 256.
     */
    public static long maxOutstandingConfirms() {
        return envInt("MAX_OUTSTANDING_CONFIRMS", 256);
    }

    /** Chunks one streaming call may buffer unconsumed. {@code STREAM_MAX_BUFFERED_CHUNKS}, default 1024. */
    public static long streamMaxBufferedChunks() {
        return envInt("STREAM_MAX_BUFFERED_CHUNKS", 1024);
    }

    /** Bytes one streaming call may buffer unconsumed. {@code STREAM_MAX_BUFFERED_BYTES}, default 64 MiB. */
    public static long streamMaxBufferedBytes() {
        return envInt("STREAM_MAX_BUFFERED_BYTES", 64L * 1024 * 1024);
    }

    /** Bytes buffered across every streaming call of a context. {@code STREAM_MAX_TOTAL_BUFFERED_BYTES}, default 256 MiB. */
    public static long streamMaxTotalBufferedBytes() {
        return envInt("STREAM_MAX_TOTAL_BUFFERED_BYTES", 256L * 1024 * 1024);
    }

    /** How long a graceful shutdown waits for in-flight work. {@code SHUTDOWN_DRAIN_TIMEOUT_MS}, default 30000 ms. */
    public static long shutdownDrainTimeoutMs() {
        return envInt("SHUTDOWN_DRAIN_TIMEOUT_MS", 30000);
    }
}
