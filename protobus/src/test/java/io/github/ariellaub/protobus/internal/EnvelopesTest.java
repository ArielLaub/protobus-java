package io.github.ariellaub.protobus.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.util.HexFormat;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * The golden values were produced by the TypeScript reference implementation
 * (protobus 2.4.0, MessageFactory); protobus-go and protobus-cpp pin the same
 * bytes. They keep this encoder byte for byte with what a TypeScript peer emits.
 */
class EnvelopesTest {
    static byte[] hex(String h) {
        return HexFormat.of().parseHex(h);
    }

    static String hex(byte[] b) {
        return HexFormat.of().formatHex(b);
    }

    @Test
    void requestMatchesTypeScript() {
        assertEquals("0a09542e5376632e6164641a020801",
                hex(Envelopes.encodeRequest(new Envelopes.Request("T.Svc.add", null, hex("0801")))));
        // proto3 would drop an all-default payload; TypeScript always writes data, even empty.
        assertEquals("0a09542e5376632e6164641201781a00",
                hex(Envelopes.encodeRequest(new Envelopes.Request("T.Svc.add", "x", new byte[0]))));
    }

    @Test
    void responseMatchesTypeScript() {
        assertEquals("0a0f0a09542e5376632e61646412020802", hex(Envelopes.encodeResponse(
                new Envelopes.Response(new Envelopes.Result("T.Svc.add", hex("0802")), null))));
        assertEquals("0a0d0a09542e5376632e6164641200", hex(Envelopes.encodeResponse(
                new Envelopes.Response(new Envelopes.Result("T.Svc.add", new byte[0]), null))));
        // An empty code is written explicitly, as TypeScript does.
        assertEquals("12130a09542e5376632e6164641204626f6f6d1a00", hex(Envelopes.encodeResponse(
                new Envelopes.Response(null, new Envelopes.Error("T.Svc.add", "boom", "")))));
        assertEquals("12110a09542e5376632e61646412016d1a0143", hex(Envelopes.encodeResponse(
                new Envelopes.Response(null, new Envelopes.Error("T.Svc.add", "m", "C")))));
    }

    @Test
    void responseMustCarryExactlyOneMember() {
        assertThrows(IllegalArgumentException.class,
                () -> Envelopes.encodeResponse(new Envelopes.Response(null, null)));
        assertThrows(IllegalArgumentException.class, () -> Envelopes.encodeResponse(new Envelopes.Response(
                new Envelopes.Result("a", new byte[0]), new Envelopes.Error("a", "", ""))));
    }

    @Test
    void eventMatchesTypeScript() {
        assertEquals("0a04542e4576120a4556454e542e542e45761a030a0179",
                hex(Envelopes.encodeEvent(new Envelopes.Event("T.Ev", "EVENT.T.Ev", hex("0a0179")))));
    }

    @Test
    void decodesTypeScriptRequest() {
        Envelopes.Request r = Envelopes.decodeRequest(hex("0a09542e5376632e61646412001a020801"));
        assertEquals("T.Svc.add", r.method());
        assertEquals("", r.actor());
        assertArrayEquals(hex("0801"), r.data());
    }

    @Test
    void decodesTypeScriptResponses() {
        Envelopes.Response e = Envelopes.decodeResponse(hex("12110a09542e5376632e61646412016d1a0143"));
        assertNotNull(e.error());
        assertNull(e.result());
        assertEquals("T.Svc.add", e.error().method());
        assertEquals("m", e.error().message());
        assertEquals("C", e.error().code());

        Envelopes.Response r = Envelopes.decodeResponse(hex("0a0d0a09542e5376632e6164641200"));
        assertNotNull(r.result());
        assertEquals("T.Svc.add", r.result().method());
        assertEquals(0, r.result().data().length);
    }

    @Test
    void errorWinsOverResult() {
        // TypeScript checks `error` first; a container carrying both must read as
        // an error on every port.
        Envelopes.Writer w = new Envelopes.Writer();
        w.bytes(1, Envelopes.encodeResult(new Envelopes.Result("a.B.c", new byte[0])));
        Envelopes.Writer err = new Envelopes.Writer();
        err.string(1, "a.B.c");
        err.string(2, "no");
        w.bytes(2, err.toByteArray());
        Envelopes.Response r = Envelopes.decodeResponse(w.toByteArray());
        assertNotNull(r.error());
        assertNull(r.result());
    }

    @Test
    void emptyResponseIsRefused() {
        assertThrows(Envelopes.MalformedException.class, () -> Envelopes.decodeResponse(new byte[0]));
    }

    @Test
    void decodesTypeScriptEvent() {
        Envelopes.Event e = Envelopes.decodeEvent(hex("0a04542e4576120a4556454e542e542e45761a030a0179"));
        assertEquals("T.Ev", e.type());
        assertEquals("EVENT.T.Ev", e.topic());
        assertArrayEquals(hex("0a0179"), e.data());
    }

    @Test
    void skipsUnknownFields() {
        // A future peer adding a field must not break an older reader.
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(Envelopes.encodeRequest(new Envelopes.Request("a.B.c", null, new byte[] {1})));
        b.write((99 << 3) & 0x7f | 0x80);
        b.write(99 >> 4);
        b.write(7);
        assertEquals("a.B.c", Envelopes.decodeRequest(b.toByteArray()).method());
    }

    @Test
    void rejectsMalformedInput() {
        for (String h : new String[] {"0a0954", "00", "0801", "0aff", "0a01ff"}) {
            assertThrows(Envelopes.MalformedException.class, () -> Envelopes.decodeRequest(hex(h)), h);
        }
    }

    @Test
    void roundTrips() {
        byte[] data = new byte[300];
        java.util.Arrays.fill(data, (byte) 0xab);
        Envelopes.Request out = Envelopes.decodeRequest(
                Envelopes.encodeRequest(new Envelopes.Request("pkg.sub.Service.method", "user:42", data)));
        assertEquals("pkg.sub.Service.method", out.method());
        assertEquals("user:42", out.actor());
        assertArrayEquals(data, out.data());
    }

    @Test
    void randomBytesNeverEscapeAsAnythingButMalformed() {
        // A property test over the decoders: arbitrary input either decodes or is
        // refused as malformed, never with some other exception.
        Random random = new Random(42);
        for (int i = 0; i < 20000; i++) {
            byte[] b = new byte[random.nextInt(40)];
            random.nextBytes(b);
            try {
                Envelopes.decodeRequest(b);
            } catch (Envelopes.MalformedException ignored) {
                // Refused.
            }
            try {
                Envelopes.decodeResponse(b);
            } catch (Envelopes.MalformedException ignored) {
                // Refused.
            }
            try {
                Envelopes.decodeEvent(b);
            } catch (Envelopes.MalformedException ignored) {
                // Refused.
            }
        }
    }
}
