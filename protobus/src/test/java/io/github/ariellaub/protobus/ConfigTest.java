package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ConfigTest {
    @AfterEach
    void reset() {
        Config.reset();
    }

    @Test
    void defaultsMatchTheOtherPorts() {
        assertEquals("proto.bus", Config.busExchangeName());
        assertEquals("proto.bus.callback", Config.callbacksExchangeName());
        assertEquals("proto.bus.cancel", Config.cancelExchangeName());
        assertEquals("proto.bus.events", Config.eventsExchangeName());
        assertEquals(600000, Config.messageProcessingTimeout());
        assertEquals(600000, Config.rpcCallTimeoutMs());
        assertEquals(60000, Config.streamIdleTimeoutMs());
        assertEquals(1, Config.defaultPrefetch());
        assertEquals(30000, Config.publishConfirmTimeoutMs());
        assertEquals(30, Config.heartbeatSeconds());
        assertEquals(256, Config.maxOutstandingConfirms());
        assertTrue(Config.exposeInternalErrors());
    }

    @Test
    void integersAreStrict() {
        for (String bad : new String[] {"6oo000", "123abc", "-5", "0", "1.5", " ", "", "99999999999999999999"}) {
            Config.set("MESSAGE_PROCESSING_TIMEOUT", bad);
            assertEquals(600000, Config.messageProcessingTimeout(), "'" + bad + "'");
        }
        Config.set("MESSAGE_PROCESSING_TIMEOUT", " 1500 ");
        assertEquals(1500, Config.messageProcessingTimeout());
    }

    @Test
    void aChangedValueIsPickedUp() {
        Config.set("RPC_CALL_TIMEOUT_MS", "100");
        assertEquals(100, Config.rpcCallTimeoutMs());
        Config.set("RPC_CALL_TIMEOUT_MS", "200");
        assertEquals(200, Config.rpcCallTimeoutMs());
    }

    @Test
    void booleansNeedAnExplicitWord() {
        Config.set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "OFF");
        assertFalse(Config.exposeInternalErrors());
        Config.set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "nope");
        assertTrue(Config.exposeInternalErrors());
        Config.set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "0");
        assertFalse(Config.exposeInternalErrors());
    }

    @Test
    void emptyExchangeNameKeepsTheDefault() {
        Config.set("BUS_EXCHANGE_NAME", "");
        assertEquals("proto.bus", Config.busExchangeName());
        Config.set("BUS_EXCHANGE_NAME", "other.bus");
        assertEquals("other.bus", Config.busExchangeName());
    }
}
