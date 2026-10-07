package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ErrorsTest {
    @AfterEach
    void reset() {
        Config.reset();
    }

    @Test
    void summariesNeverCarryAnUnhandledMessage() {
        assertEquals("IllegalStateException", Errors.safeErrorSummary(new IllegalStateException("password=hunter2")));
        assertEquals("RpcTimeoutError[RPC_TIMEOUT]", Errors.safeErrorSummary(new RpcTimeoutError("secret")));
        assertEquals("HandledError[REFUSED]: refused 7", Errors.safeErrorSummary(new HandledError("refused 7", "REFUSED")));
        assertEquals("ProtocolError[PROTOCOL_ERROR]: bad", Errors.safeErrorSummary(new ProtocolError("bad")));
        assertEquals("UnknownError", Errors.safeErrorSummary(null));
    }

    @Test
    void handledErrorsDefaultTheirCode() {
        assertEquals("HANDLED_ERROR", new HandledError("x").code());
        assertEquals("HANDLED_ERROR", new HandledError("x", "").code());
        assertEquals("PROTOCOL_ERROR", new InvalidMethodError("x").code());
        assertTrue(Errors.isHandledError(new InvalidMethodError("x")));
    }

    @Test
    void sanitisingKeepsHandledErrorsAndTimeouts() {
        Config.set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "false");
        HandledError handled = new HandledError("no", "NO");
        assertSame(handled, Errors.sanitizeErrorForClient(handled, "c1"));
        TimeoutError timeout = new TimeoutError("took too long");
        assertSame(timeout, Errors.sanitizeErrorForClient(timeout, "c1"));
        Throwable hidden = Errors.sanitizeErrorForClient(new IllegalStateException("db password"), "c1");
        assertEquals("internal service error (correlationId c1)", hidden.getMessage());
        assertEquals("INTERNAL_ERROR", Errors.codeOf(hidden));

        Config.set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "true");
        IllegalStateException raw = new IllegalStateException("x");
        assertSame(raw, Errors.sanitizeErrorForClient(raw, "c1"));
    }

    @Test
    void urlsAreRedacted() {
        assertEquals("amqp://user:***@host:5672/%2f", Logger.redactUrl("amqp://user:s3cret@host:5672/%2f"));
        assertEquals("amqp://host/", Logger.redactUrl("amqp://host/"));
        assertEquals("amqps://u:***@h/?heartbeat=5", Logger.redactUrl("amqps://u:p@h/?heartbeat=5"));
        assertEquals("<redacted>", Logger.redactUrl("not a url with spaces"));
    }
}
