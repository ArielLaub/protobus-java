package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.internal.Envelopes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import pbtest.Tick;
import pbtest.TickRequest;

class StreamTest extends MemoryBus {
    static TickRequest ticks(int count) {
        return TickRequest.newBuilder().setCount(count).build();
    }

    static List<Integer> seqs(ProtobusStream<Tick> stream) {
        List<Integer> out = new ArrayList<>();
        try (stream) {
            for (Tick t : stream) out.add(t.getSeq());
        }
        return out;
    }

    @Test
    void chunksArriveInOrder() {
        CalcService s = serve();
        assertEquals(List.of(0, 1, 2, 3, 4), seqs(proxy().ticks(ticks(5))));
        assertTrue(s.finished.get());
    }

    @Test
    void anEmptyStreamEnds() {
        serve();
        assertEquals(List.of(), seqs(proxy().ticks(ticks(0))));
    }

    @Test
    void aMidStreamHandledErrorEndsTheIteration() {
        serve();
        List<Integer> got = new ArrayList<>();
        RemoteError e = assertThrows(RemoteError.class, () -> {
            try (ProtobusStream<Tick> s = proxy().ticks(TickRequest.newBuilder().setCount(5).setFailAt(2).build())) {
                for (Tick t : s) got.add(t.getSeq());
            }
        });
        assertEquals(List.of(0, 1), got);
        assertEquals("TEST_FAIL", e.code());
    }

    @Test
    void aMidStreamUnhandledErrorIsNotRetried() {
        CalcService s = serve();
        RemoteError e = assertThrows(RemoteError.class, () -> seqs(proxy().ticks(
                TickRequest.newBuilder().setCount(5).setFailAt(1).setUnhandled(true).build())));
        assertEquals("stream broke", e.getMessage());
        assertEquals(0, broker.queueDepth("pbtest.Calc.DLQ"));
        assertEquals(1, s.yielded.get());
    }

    @Test
    void leavingTheLoopEarlyCancelsTheProducer() {
        CalcService s = serve();
        try (ProtobusStream<Tick> stream = proxy().ticks(TickRequest.newBuilder().setCount(1000).setDelayMs(20)
                .build())) {
            assertTrue(stream.hasNext());
            assertEquals(0, stream.next().getSeq());
        }
        assertTrue(eventually(s.stoppedEarly::get));
        assertFalse(s.finished.get());
        // Settled, not retried or dead-lettered.
        assertTrue(eventually(() -> broker.unackedCount("pbtest.Calc") == 0));
        assertEquals(0, broker.queueDepth("pbtest.Calc.DLQ"));
    }

    @Test
    void aSignalCancelsFromAnywhere() {
        CalcService s = serve();
        AbortController controller = new AbortController();
        ProtobusStream<Tick> stream = proxy().ticks(TickRequest.newBuilder().setCount(1000).setDelayMs(20).build(),
                StreamOptions.DEFAULT.withSignal(controller.signal()));
        assertTrue(stream.hasNext());
        controller.abort();
        assertTrue(eventually(s.stoppedEarly::get));
        // A cancelled stream ends rather than raising.
        int rest = 0;
        while (stream.hasNext()) {
            stream.next();
            rest++;
        }
        assertTrue(rest <= 2, "at most what was already buffered");
    }

    @Test
    void anAbortedSignalSendsNothing() {
        CalcService s = serve();
        AbortController controller = new AbortController();
        controller.abort();
        assertEquals(List.of(), seqs(proxy().ticks(ticks(3), StreamOptions.DEFAULT.withSignal(controller.signal()))));
        broker.flush();
        assertEquals(0, s.yielded.get());
    }

    @Test
    void aStalledStreamTimesOutAndStopsTheProducer() {
        CalcService s = serve();
        ProtobusStream<Tick> stream = proxy().ticks(TickRequest.newBuilder().setCount(10).setDelayMs(2000).build(),
                StreamOptions.DEFAULT.withIdleTimeoutMs(150));
        assertThrows(StreamTimeoutError.class, () -> {
            while (stream.hasNext()) stream.next();
        });
        assertTrue(eventually(s.stoppedEarly::get));
    }

    @Test
    void aProducerOutrunningItsConsumerFailsTheStream() {
        Config.set("STREAM_MAX_BUFFERED_CHUNKS", "3");
        serve();
        ProtobusStream<Tick> stream = proxy().ticks(ticks(50));
        // Let the producer run ahead of a consumer that has not started.
        assertTrue(eventually(() -> broker.unackedCount("pbtest.Calc") == 0));
        assertThrows(StreamBackpressureError.class, () -> {
            while (stream.hasNext()) stream.next();
        });
    }

    /** A hand-driven "service" that replies with chunks of its own choosing. */
    private void replyWithChunks(List<Map<String, Object>> headers) {
        Connection c = ctx.connection();
        AmqpChannel ch = c.openChannel();
        c.declareQueue(ch, "pbtest.Calc", true, false, false, Map.of());
        c.bindQueue(ch, "pbtest.Calc", Config.busExchangeName(), "REQUEST.pbtest.Calc.*");
        ch.consume("pbtest.Calc", "fake", true, false, d -> c.internalExecutor().execute(() -> {
            for (int i = 0; i < headers.size(); i++) {
                byte[] body = MessageFactory.buildEncodedResponse("pbtest.Calc.ticks",
                        Tick.newBuilder().setSeq(i).build().toByteArray());
                c.publish(ch, Config.callbacksExchangeName(), d.properties().getReplyTo(), body,
                        Connection.PublishOptions.of(new BasicProperties.Builder()
                                .correlationId(d.properties().getCorrelationId()).headers(headers.get(i)).build()));
            }
        }), null);
    }

    @Test
    void aLostChunkFailsTheStreamRatherThanTruncatingIt() {
        replyWithChunks(List.of(Map.of(Config.HEADER_SEQ, 0, Config.HEADER_FINAL, false),
                Map.of(Config.HEADER_SEQ, 2, Config.HEADER_FINAL, true)));
        ProtobusStream<Tick> stream = proxy().ticks(ticks(3));
        assertTrue(stream.hasNext());
        stream.next();
        assertThrows(StreamSequenceError.class, stream::hasNext);
    }

    @Test
    void peersWithoutSequenceHeadersAndOtherEncodingsAreAccepted() {
        // A final flag as text, and no sequence numbers at all.
        replyWithChunks(List.of(Map.of(Config.HEADER_FINAL, "false"), Map.of(Config.HEADER_FINAL, 0),
                Map.of(Config.HEADER_FINAL, "true")));
        assertEquals(List.of(0, 1, 2), seqs(proxy().ticks(ticks(3))));
    }

    @Test
    void aDuplicateChunkIsDropped() {
        replyWithChunks(List.of(Map.of(Config.HEADER_SEQ, 0L, Config.HEADER_FINAL, false),
                Map.of(Config.HEADER_SEQ, 0L, Config.HEADER_FINAL, false),
                Map.of(Config.HEADER_SEQ, 1L, Config.HEADER_FINAL, true)));
        List<Integer> got = seqs(proxy().ticks(ticks(3)));
        // The second copy of seq 0 carried payload seq=1 but was dropped as a duplicate.
        assertEquals(List.of(0, 2), got);
    }

    @Test
    void aStreamRequestAgainstAUnaryMethodIsRefused() {
        serve();
        ServiceProxy p = new ServiceProxy(ctx, "pbtest.Calc");
        p.init();
        ProtobusStream<com.google.protobuf.Message> s = p.callStream("add", RpcTest.add(1, 2), StreamOptions.DEFAULT);
        assertThrows(InvalidRequestError.class, s::hasNext);
    }

    @Test
    void chunksAreResponseContainers() {
        serve();
        byte[] request = Envelopes.encodeRequest(new Envelopes.Request("pbtest.Calc.ticks", null,
                ticks(2).toByteArray()));
        MessageDispatcher.ChunkStream raw = ctx.publishStreamingMessage(request, "REQUEST.pbtest.Calc.ticks",
                StreamOptions.DEFAULT);
        List<byte[]> chunks = raw.drainForTest();
        assertEquals(2, chunks.size());
        assertEquals("pbtest.Calc.ticks", Envelopes.decodeResponse(chunks.get(1)).result().method());
    }
}
