package io.github.ariellaub.protobus.internal;

import com.rabbitmq.client.LongString;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Tolerant readers for AMQP header values, accepting every encoding peers
 * produce: integers of any width or as decimal text, booleans as a boolean,
 * number or text.
 */
public final class Headers {
    private Headers() {}

    public static String text(Object v) {
        if (v == null) return null;
        if (v instanceof LongString) return v.toString();
        if (v instanceof byte[]) return new String((byte[]) v, StandardCharsets.UTF_8);
        return v.toString();
    }

    /** An integer, or null when absent or unparseable. */
    public static Long integer(Object v) {
        if (v == null) return null;
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return ((Number) v).longValue();
        }
        if (v instanceof Number) {
            double d = ((Number) v).doubleValue();
            return d == Math.rint(d) && !Double.isInfinite(d) ? (long) d : null;
        }
        String t = text(v);
        if (t == null) return null;
        try {
            return Long.parseLong(t.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static boolean bool(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof Number) return ((Number) v).doubleValue() != 0;
        String t = text(v);
        return t != null && (t.toLowerCase(Locale.ROOT).equals("true") || t.equals("1"));
    }

    public static Object get(Map<String, Object> headers, String key) {
        return headers == null ? null : headers.get(key);
    }
}
