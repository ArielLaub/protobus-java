package io.github.ariellaub.protobus;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;
import io.github.ariellaub.protobus.types.bigint;
import io.github.ariellaub.protobus.types.timestamp;
import java.math.BigInteger;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The built-in custom types, {@code bigint} and {@code timestamp}.
 *
 * Both are one-field messages declared at the root of the type namespace, as in
 * every port:
 *
 * <pre>
 *   message bigint    { optional bytes value = 1; }  // unsigned, up to 2^256-1: 32 bytes, big-endian
 *   message timestamp { optional int64 value = 1; }  // signed milliseconds since the epoch
 * </pre>
 *
 * In Java they are {@link bigint} and {@link timestamp}, converted with the
 * helpers here:
 *
 * <pre>{@code
 * balance.setAmount(CustomTypes.bigint(new BigInteger("1000000000000000000000000000000")));
 * BigInteger amount = CustomTypes.toBigInteger(balance.getAmount());
 * balance.setAsOf(CustomTypes.timestamp(Instant.now()));
 * Instant when = CustomTypes.toInstant(balance.getAsOf());
 * }</pre>
 *
 * Every port refuses a negative or oversized bigint and refuses to decode one
 * wider than 32 bytes; a timestamp is refused beyond ±8.64e15 ms, the range every
 * port can represent. protobus checks every request, reply and event it encodes or
 * decodes, so a malformed value set through a builder by hand is caught before it
 * leaves, and one arriving in a request is answered PROTOCOL_ERROR.
 */
public final class CustomTypes {
    private CustomTypes() {}

    /** Width of the bigint wire format, in bytes. */
    public static final int BIGINT_BYTES = 32;
    /** The largest bigint: 2^256-1. */
    public static final BigInteger BIGINT_MAX = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
    /** The largest timestamp magnitude, in milliseconds: what an ECMAScript Date holds. */
    public static final long MAX_TIMESTAMP_MS = 8_640_000_000_000_000L;

    public static final String BIGINT = "bigint";
    public static final String TIMESTAMP = "timestamp";

    // ---- bigint ----------------------------------------------------------------------

    /** Encode an unsigned integer as exactly 32 big-endian bytes. */
    public static bigint bigint(BigInteger value) {
        return bigint.newBuilder().setValue(ByteString.copyFrom(bigintBytes(value))).build();
    }

    public static bigint bigint(long value) {
        return bigint(BigInteger.valueOf(value));
    }

    /** Parse decimal, or hexadecimal with a {@code 0x} prefix. */
    public static bigint bigint(String value) {
        String v = value.trim();
        BigInteger parsed;
        try {
            parsed = v.startsWith("0x") || v.startsWith("0X")
                    ? new BigInteger(v.substring(2), 16)
                    : new BigInteger(v);
        } catch (NumberFormatException e) {
            throw new CustomTypeRangeError("bigint value '" + value + "' is not a decimal or 0x-hex integer");
        }
        return bigint(parsed);
    }

    /** The value; an unset or empty bigint is zero. */
    public static BigInteger toBigInteger(bigint value) {
        if (value == null || !value.hasValue()) return BigInteger.ZERO;
        return bigintFromBytes(value.getValue().toByteArray());
    }

    static byte[] bigintBytes(BigInteger value) {
        if (value.signum() < 0) {
            throw new CustomTypeRangeError("bigint value " + value
                    + " is negative; the protobus bigint wire format is unsigned (0 .. 2^256-1)");
        }
        if (value.compareTo(BIGINT_MAX) > 0) {
            throw new CustomTypeRangeError("bigint value " + value + " exceeds the maximum representable value 2^256-1");
        }
        byte[] raw = value.toByteArray(); // two's complement, may carry a leading zero
        byte[] out = new byte[BIGINT_BYTES];
        int n = Math.min(raw.length, BIGINT_BYTES);
        System.arraycopy(raw, raw.length - n, out, BIGINT_BYTES - n, n);
        return out;
    }

    static BigInteger bigintFromBytes(byte[] data) {
        // The encoder never emits more than 32 bytes, so anything longer is
        // malformed and not worth the time to interpret.
        if (data.length > BIGINT_BYTES) {
            throw new CustomTypeRangeError("bigint wire value is " + data.length
                    + " bytes; the protobus bigint wire format is at most " + BIGINT_BYTES);
        }
        return data.length == 0 ? BigInteger.ZERO : new BigInteger(1, data);
    }

    // ---- timestamp -------------------------------------------------------------------

    public static timestamp timestamp(Instant instant) {
        return timestampMillis(instant.toEpochMilli());
    }

    public static timestamp timestampMillis(long epochMillis) {
        checkMillis(epochMillis);
        return timestamp.newBuilder().setValue(epochMillis).build();
    }

    public static Instant toInstant(timestamp value) {
        return Instant.ofEpochMilli(toMillis(value));
    }

    /** Milliseconds since the epoch; an unset timestamp is 0. */
    public static long toMillis(timestamp value) {
        long ms = value == null ? 0 : value.getValue();
        checkMillis(ms);
        return ms;
    }

    private static void checkMillis(long ms) {
        if (ms > MAX_TIMESTAMP_MS || ms < -MAX_TIMESTAMP_MS) {
            throw new CustomTypeRangeError("timestamp value " + ms
                    + " ms is beyond ±8.64e15 ms, the range every port can represent");
        }
    }

    // ---- validation ------------------------------------------------------------------

    private static final Map<Descriptor, Boolean> containsCustom = new ConcurrentHashMap<>();

    /**
     * Check every bigint and timestamp in a message, recursively.
     *
     * @throws CustomTypeRangeError on the first value no port could represent
     */
    public static void validate(MessageOrBuilder message) {
        if (message == null || !reaches(message.getDescriptorForType())) return;
        walk(message);
    }

    private static void walk(MessageOrBuilder message) {
        Descriptor type = message.getDescriptorForType();
        if (isRootType(type, BIGINT)) {
            checkBigint(message);
            return;
        }
        if (isRootType(type, TIMESTAMP)) {
            checkTimestamp(message);
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> e : message.getAllFields().entrySet()) {
            FieldDescriptor field = e.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE || !reaches(field.getMessageType())) continue;
            if (field.isRepeated()) {
                for (Object item : (List<?>) e.getValue()) walk((MessageOrBuilder) item);
            } else {
                walk((MessageOrBuilder) e.getValue());
            }
        }
    }

    private static void checkBigint(MessageOrBuilder m) {
        FieldDescriptor value = m.getDescriptorForType().findFieldByNumber(1);
        if (value == null || value.getJavaType() != FieldDescriptor.JavaType.BYTE_STRING) return;
        if (!value.hasPresence() || m.hasField(value)) {
            bigintFromBytes(((ByteString) m.getField(value)).toByteArray());
        }
    }

    private static void checkTimestamp(MessageOrBuilder m) {
        FieldDescriptor value = m.getDescriptorForType().findFieldByNumber(1);
        if (value == null || value.getJavaType() != FieldDescriptor.JavaType.LONG) return;
        checkMillis((Long) m.getField(value));
    }

    static boolean isRootType(Descriptor type, String name) {
        return type.getFullName().equals(name);
    }

    /** Whether values of this type can contain a custom type, cycles included. */
    static boolean reaches(Descriptor type) {
        Boolean cached = containsCustom.get(type);
        if (cached != null) return cached;
        boolean result = reaches(type, new HashSet<>());
        containsCustom.put(type, result);
        return result;
    }

    private static boolean reaches(Descriptor type, Set<String> visiting) {
        if (isRootType(type, BIGINT) || isRootType(type, TIMESTAMP)) return true;
        if (!visiting.add(type.getFullName())) return false;
        for (FieldDescriptor field : type.getFields()) {
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && reaches(field.getMessageType(), visiting)) {
                return true;
            }
        }
        return false;
    }
}
