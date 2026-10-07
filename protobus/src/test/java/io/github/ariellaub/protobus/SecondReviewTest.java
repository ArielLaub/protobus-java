package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.ConfirmOutcome;
import io.github.ariellaub.protobus.internal.Envelopes;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import pbtest.Tick;
import pbtest.TickRequest;

/** Regressions for the findings of the second review. */
class SecondReviewTest extends MemoryBus {
    @Test
    void aTimedOutConfirmKeepsItsSlotUntilTheBrokerAnswers() throws Exception {
        Config.set("MAX_OUTSTANDING_CONFIRMS", "1");
        Config.set("PUBLISH_CONFIRM_TIMEOUT_MS", "50");
        AmqpChannel ch = ctx.connection().openChannel();
        ctx.connection().declareQueue(ch, "q", true, false, false, Map.of());
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        List<CompletableFuture<String>> publishes = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            publishes.add(ctx.connection().publishAsync(ch, "", "q", new byte[0], Connection.PublishOptions.of(null)));
        }
        for (CompletableFuture<String> f : publishes) {
            Exception e = assertThrows(Exception.class, () -> f.get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof PublishConfirmTimeoutError, String.valueOf(e.getCause()));
        }
        broker.flush();
        // The bound holds: the broker never had more than one unanswered publish.
        assertEquals(1, broker.heldConfirms());
        // Once the broker answers, the slot is free again.
        broker.releaseHeldConfirms(ConfirmOutcome.ACK);
        broker.setConfirmMode(MemoryBroker.ConfirmMode.ACK);
        ctx.connection().publish(ch, "", "q", new byte[0], Connection.PublishOptions.of(null));
    }

    @Test
    void aFastReplyDoesNotDisarmTheDeadline() {
        serve();
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        long start = System.nanoTime();
        // The reply arrives at once (the broker routes it), but the request's
        // confirm never comes: the 100 ms deadline must still end the call.
        assertThrows(RpcTimeoutError.class, () -> proxy().add(RpcTest.add(1, 1), CallOptions.DEFAULT.withTimeoutMs(100)));
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2), "took "
                + (System.nanoTime() - start) / 1_000_000 + " ms");
    }

    @Test
    void theIdleTimeoutWakesAReaderWaitingOnThePublish() {
        serve();
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        long start = System.nanoTime();
        ProtobusStream<Tick> stream = proxy().ticks(TickRequest.newBuilder().setCount(3).setDelayMs(2000).build(),
                StreamOptions.DEFAULT.withIdleTimeoutMs(80));
        assertThrows(StreamTimeoutError.class, stream::hasNext);
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2), "took "
                + (System.nanoTime() - start) / 1_000_000 + " ms");
    }

    @Test
    void aCompletedStreamKeepsItsChunksAcrossADisconnect() throws Exception {
        serve();
        byte[] request = Envelopes.encodeRequest(new Envelopes.Request("pbtest.Calc.ticks", null,
                TickRequest.newBuilder().setCount(2).build().toByteArray()));
        MessageDispatcher.ChunkStream raw = ctx.publishStreamingMessage(request, "REQUEST.pbtest.Calc.ticks",
                StreamOptions.DEFAULT);
        assertTrue(eventually(raw::ended));
        broker.killConnections();
        assertTrue(eventually(() -> !ctx.connection().isConnected() || ctx.isReconnecting()));
        List<byte[]> chunks = raw.drainForTest();
        assertEquals(2, chunks.size());
        assertEquals(1, Tick.parseFrom(Envelopes.decodeResponse(chunks.get(1)).result().data()).getSeq(), "seq");
    }

    @Test
    void readingAStreamAfterTheContextClosedRaisesItsOwnOutcome() {
        Context c = newContext();
        CalcService s = serve(CalcService::new, MessageServiceOptions.DEFAULT, ctx);
        CalcProtobusProxy p = new CalcProtobusProxy(c);
        ProtobusStream<Tick> stream = p.proxy.ticks(TickRequest.newBuilder().setCount(100).setDelayMs(100).build());
        c.close();
        assertThrows(DisconnectedError.class, stream::hasNext);
        assertEquals(0, s.failAttempts.get());
    }

    /** A proxy on another context. */
    static final class CalcProtobusProxy {
        final pbtest.CalcProtobus.Proxy proxy;

        CalcProtobusProxy(Context c) {
            proxy = new pbtest.CalcProtobus.Proxy(c);
            proxy.init();
        }
    }

    @Test
    void collidingRpcNamesStillGenerateDistinctMethods() throws Exception {
        // naming.proto compiles at all only if the names were allocated apart.
        Class<?> proxy = naming.NamingProtobus.Proxy.class;
        List<String> names = new ArrayList<>();
        for (var m : proxy.getDeclaredMethods()) names.add(m.getName());
        assertTrue(names.contains("fetch") && names.contains("fetchAsync") && names.contains("fetchAsyncAsync"), names.toString());
        // fetch's own async variant could not be fetchAsync, which is an rpc.
        assertTrue(names.contains("fetchAsync_"), names.toString());
    }
}
