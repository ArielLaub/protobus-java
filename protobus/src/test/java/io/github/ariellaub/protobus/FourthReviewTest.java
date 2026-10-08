package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ariellaub.protobus.amqp.AmqpChannel;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Regressions for the findings of the fourth review. */
class FourthReviewTest extends MemoryBus {
    @Test
    void anEarlyAckDeliveryRefusedBeforeItsAckIsRequeued() {
        ExecutorService pool = Executors.newFixedThreadPool(1);
        Context c = newContext(ContextOptions.DEFAULT.withHandlerExecutor(pool));
        CalcService s = serve(CalcService::new,
                MessageServiceOptions.DEFAULT.withLateAck(false).withMaxConcurrent(1), c);
        pool.shutdown();
        proxy().add(RpcTest.add(1, 1), CallOptions.DEFAULT.withRpc(false));
        // Refused: never handled, and not acknowledged either. Stop consuming so
        // the requeued message stays visible in the queue.
        assertTrue(eventually(() -> broker.unackedCount("pbtest.Calc") == 1));
        s.stopConsuming();
        assertTrue(eventually(() -> broker.queueDepth("pbtest.Calc") == 1, Duration.ofSeconds(5)),
                "unacked=" + broker.unackedCount("pbtest.Calc") + " ready=" + broker.queueDepth("pbtest.Calc"));
        assertEquals(0, s.slowStarted.get());
    }

    /**
     * The second publish waits behind a write blocked in the transport: queued for
     * the writer when a confirm slot is free (limit 2), or for a slot when none is
     * (limit 1). Either way, closing the context must fail it at once.
     */
    @org.junit.jupiter.params.ParameterizedTest(name = "confirm limit {0}")
    @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2})
    void closingFailsPublishesQueuedBehindABlockedWrite(int limit) throws Exception {
        Config.set("MAX_OUTSTANDING_CONFIRMS", String.valueOf(limit));
        Config.set("PUBLISH_CONFIRM_TIMEOUT_MS", "300");
        ThirdReviewTest.StallingTransport transport = new ThirdReviewTest.StallingTransport(broker);
        Context c = new Context(ContextOptions.DEFAULT.withTransport(transport));
        c.init("amqp://memory/");
        AmqpChannel ch = c.connection().openChannel();
        c.connection().declareQueue(ch, "q", true, false, false, Map.of());
        CountDownLatch stall = new CountDownLatch(1);
        transport.entered = new CountDownLatch(1);
        transport.stall = stall;
        try {
            CompletableFuture<String> first = c.connection().publishAsync(ch, "", "q", new byte[0],
                    Connection.PublishOptions.of(null));
            assertTrue(transport.entered.await(2, TimeUnit.SECONDS), "the first write never reached the transport");
            CompletableFuture<String> second = c.connection().publishAsync(ch, "", "q", new byte[0],
                    Connection.PublishOptions.of(null));
            c.close();
            ExecutionException e = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class,
                    () -> second.get(500, TimeUnit.MILLISECONDS));
            assertTrue(e.getCause() instanceof ChannelClosedError, String.valueOf(e.getCause()));
            assertTrue(!first.isDone() || first.isCompletedExceptionally());
            // A publish made after the close fails at once too.
            CompletableFuture<String> late = c.connection().publishAsync(ch, "", "q", new byte[0],
                    Connection.PublishOptions.of(null));
            assertTrue(late.isCompletedExceptionally());
        } finally {
            stall.countDown();
        }
    }
}
