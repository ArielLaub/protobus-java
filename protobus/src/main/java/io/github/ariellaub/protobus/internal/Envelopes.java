package io.github.ariellaub.protobus.internal;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * The protobus envelopes: the five small protobuf messages every request, reply
 * and event travels inside.
 *
 * They have no .proto file and no package. In the TypeScript reference they are
 * protobufjs decorator classes, so presence works proto2-style: a field set to an
 * empty string or empty bytes is still written. This codec reproduces those bytes
 * exactly rather than leaning on generated proto3 code, which would omit them.
 * Both forms decode identically on every port; byte parity keeps golden tests and
 * captured traffic comparable across languages.
 *
 * <pre>
 *   message RequestContainer  { string method = 1; string actor = 2; bytes data = 3; }
 *   message ResponseResult    { string method = 1; bytes data = 2; }
 *   message ResponseError     { string method = 1; string message = 2; string code = 3; }
 *   message ResponseContainer { oneof value { ResponseResult result = 1; ResponseError error = 2; } }
 *   message EventContainer    { string type = 1; string topic = 2; bytes data = 3; }
 * </pre>
 */
public final class Envelopes {
    private Envelopes() {}

    private static final byte[] EMPTY = new byte[0];

    /** @param actor null when the caller named none: then it is not written, as TypeScript does. */
    public record Request(String method, String actor, byte[] data) {}

    public record Result(String method, byte[] data) {}

    /** Only these three fields cross the wire; an error's type and stack stay where it was raised. */
    public record Error(String method, String message, String code) {}

    /** Exactly one of {@code result} and {@code error} is set. */
    public record Response(Result result, Error error) {}

    public record Event(String type, String topic, byte[] data) {}

    /** Bytes that are not a valid envelope. */
    public static final class MalformedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public MalformedException(String what) {
            super("protobus: malformed envelope: " + what);
        }
    }

    // ---- encoding --------------------------------------------------------------------

    /** The method and data are always written; the actor only when there is one. */
    public static byte[] encodeRequest(Request r) {
        Writer w = new Writer();
        w.string(1, r.method());
        if (r.actor() != null) w.string(2, r.actor());
        w.bytes(3, r.data());
        return w.toByteArray();
    }

    /** @throws IllegalArgumentException unless exactly one member is set */
    public static byte[] encodeResponse(Response r) {
        if ((r.result() == null) == (r.error() == null)) {
            throw new IllegalArgumentException("a response carries exactly one of result and error");
        }
        Writer w = new Writer();
        if (r.result() != null) {
            w.bytes(1, encodeResult(r.result()));
        } else {
            Writer e = new Writer();
            e.string(1, r.error().method());
            e.string(2, r.error().message());
            e.string(3, r.error().code());
            w.bytes(2, e.toByteArray());
        }
        return w.toByteArray();
    }

    /** A bare ResponseResult, not wrapped in a container. */
    public static byte[] encodeResult(Result r) {
        Writer w = new Writer();
        w.string(1, r.method());
        w.bytes(2, r.data());
        return w.toByteArray();
    }

    /** All three fields are always written. */
    public static byte[] encodeEvent(Event e) {
        Writer w = new Writer();
        w.string(1, e.type());
        w.string(2, e.topic());
        w.bytes(3, e.data());
        return w.toByteArray();
    }

    // ---- decoding --------------------------------------------------------------------

    public static Request decodeRequest(byte[] b) {
        Reader r = new Reader(b);
        String method = "";
        String actor = "";
        byte[] data = EMPTY;
        while (r.more()) {
            int tag = r.tag();
            switch (tag >>> 3) {
                case 1: method = r.string(tag); break;
                case 2: actor = r.string(tag); break;
                case 3: data = r.bytes(tag); break;
                default: r.skip(tag);
            }
        }
        return new Request(method, actor, data);
    }

    /**
     * When both members are present the error wins, matching the TypeScript
     * reader, so the same bytes cannot mean success on one port and failure on
     * another. A container with neither is refused.
     */
    public static Response decodeResponse(byte[] b) {
        Reader r = new Reader(b);
        Result result = null;
        Error error = null;
        while (r.more()) {
            int tag = r.tag();
            switch (tag >>> 3) {
                case 1: result = decodeResult(r.bytes(tag)); break;
                case 2: error = decodeError(r.bytes(tag)); break;
                default: r.skip(tag);
            }
        }
        if (error != null) return new Response(null, error);
        if (result != null) return new Response(result, null);
        throw new MalformedException("response carries neither a result nor an error");
    }

    private static Result decodeResult(byte[] b) {
        Reader r = new Reader(b);
        String method = "";
        byte[] data = EMPTY;
        while (r.more()) {
            int tag = r.tag();
            switch (tag >>> 3) {
                case 1: method = r.string(tag); break;
                case 2: data = r.bytes(tag); break;
                default: r.skip(tag);
            }
        }
        return new Result(method, data);
    }

    private static Error decodeError(byte[] b) {
        Reader r = new Reader(b);
        String method = "";
        String message = "";
        String code = "";
        while (r.more()) {
            int tag = r.tag();
            switch (tag >>> 3) {
                case 1: method = r.string(tag); break;
                case 2: message = r.string(tag); break;
                case 3: code = r.string(tag); break;
                default: r.skip(tag);
            }
        }
        return new Error(method, message, code);
    }

    public static Event decodeEvent(byte[] b) {
        Reader r = new Reader(b);
        String type = "";
        String topic = "";
        byte[] data = EMPTY;
        while (r.more()) {
            int tag = r.tag();
            switch (tag >>> 3) {
                case 1: type = r.string(tag); break;
                case 2: topic = r.string(tag); break;
                case 3: data = r.bytes(tag); break;
                default: r.skip(tag);
            }
        }
        return new Event(type, topic, data);
    }

    // ---- wire helpers ----------------------------------------------------------------

    static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        void varint(long v) {
            while ((v & ~0x7FL) != 0) {
                out.write((int) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            out.write((int) v);
        }

        void string(int field, String value) {
            bytes(field, (value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
        }

        void bytes(int field, byte[] value) {
            byte[] v = value == null ? EMPTY : value;
            varint(((long) field << 3) | 2);
            varint(v.length);
            out.write(v, 0, v.length);
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }
    }

    static final class Reader {
        private final byte[] b;
        private int pos;

        Reader(byte[] b) {
            this.b = b == null ? EMPTY : b;
        }

        boolean more() {
            return pos < b.length;
        }

        long varint() {
            long result = 0;
            for (int shift = 0; shift < 64; shift += 7) {
                if (pos >= b.length) throw new MalformedException("truncated varint");
                int x = b[pos++] & 0xFF;
                result |= (long) (x & 0x7F) << shift;
                if ((x & 0x80) == 0) return result;
            }
            throw new MalformedException("varint longer than ten bytes");
        }

        int tag() {
            long t = varint();
            if (t >>> 3 == 0 || t > 0xFFFFFFFFL) throw new MalformedException("invalid field tag " + t);
            return (int) t;
        }

        byte[] bytes(int tag) {
            if ((tag & 7) != 2) {
                throw new MalformedException("field " + (tag >>> 3) + " has wire type " + (tag & 7) + ", not 2");
            }
            long n = varint();
            if (n < 0 || n > b.length - pos) throw new MalformedException("length " + n + " overruns the message");
            byte[] v = new byte[(int) n];
            System.arraycopy(b, pos, v, 0, (int) n);
            pos += (int) n;
            return v;
        }

        String string(int tag) {
            byte[] raw = bytes(tag);
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(raw)).toString();
            } catch (CharacterCodingException e) {
                throw new MalformedException("field " + (tag >>> 3) + " is not valid UTF-8");
            }
        }

        void skip(int tag) {
            switch (tag & 7) {
                case 0: varint(); break;
                case 1: advance(8); break;
                case 2: bytes(tag); break;
                case 5: advance(4); break;
                default: throw new MalformedException("unsupported wire type " + (tag & 7));
            }
        }

        private void advance(int n) {
            if (n > b.length - pos) throw new MalformedException("truncated field");
            pos += n;
        }
    }
}
