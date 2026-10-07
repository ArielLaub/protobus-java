package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ariellaub.protobus.amqp.ConfirmOutcome;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import pbtest.AddResponse;
import pbtest.CalcProtobus;
import pbtest.SlowRequest;
import pbtest.Tick;
import pbtest.TickRequest;

/** Regressions for the findings of the independent review. */
class ReviewFindingsTest extends MemoryBus {
    @Test
    void aReplyBeforeItsConfirmDoesNotCompleteTheCallerOnTheTransportThread() throws Exception {
        serve();
        CalcProtobus.Proxy p = proxy();
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        CompletableFuture<AddResponse> call = p.addAsync(RpcTest.add(1, 2));
        // The service's reply is held back too; release everything once both exist.
        assertTrue(eventually(() -> broker.heldConfirms() >= 2));
        broker.setConfirmMode(MemoryBroker.ConfirmMode.ACK);
        AtomicReference<String> thread = new AtomicReference<>();
        CompletableFuture<Void> seen = call.thenAccept(r -> thread.set(Thread.currentThread().getName()));
        while (!call.isDone()) {
            broker.releaseHeldConfirms(ConfirmOutcome.ACK);
            Thread.sleep(5);
        }
        seen.get(5, TimeUnit.SECONDS);
        assertFalse(thread.get().startsWith("memory-broker"), thread.get());
    }

    @Test
    void anEventPublishDoesNotCompleteOnTheTransportThread() throws Exception {
        AtomicReference<String> thread = new AtomicReference<>();
        ctx.publishEventAsync("pbtest.Ping", EventsTest.ping("t"), null)
                .thenRun(() -> thread.set(Thread.currentThread().getName())).get(5, TimeUnit.SECONDS);
        assertFalse(thread.get().startsWith("memory-broker"), thread.get());
    }

    @Test
    void theRpcDeadlineBoundsTheConfirm() {
        serve(MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(60000));
        CalcProtobus.Proxy p = proxy();
        // The confirm never comes and neither does a reply (the handler outlasts the
        // deadline): the deadline, not the 5 s confirm timeout, ends the call.
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        long start = System.nanoTime();
        assertThrows(RpcTimeoutError.class, () -> p.slow(SlowRequest.newBuilder().setMs(3000).build(),
                CallOptions.DEFAULT.withTimeoutMs(150)));
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2));
    }

    @Test
    void aDeliveryTheExecutorRefusesIsRequeued() {
        AtomicInteger refusals = new AtomicInteger();
        ExecutorService pool = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable r) {
                if (refusals.getAndIncrement() == 0) throw new java.util.concurrent.RejectedExecutionException("full");
                super.execute(r);
            }
        };
        Context c = newContext(ContextOptions.DEFAULT.withHandlerExecutor(pool));
        serve(CalcService::new, MessageServiceOptions.DEFAULT, c);
        assertEquals(3, proxy().add(RpcTest.add(1, 2)).getResult());
        assertTrue(refusals.get() >= 2);
        pool.shutdown();
    }

    @Test
    void anEarlyAckServiceRunsAtMostMaxConcurrentHandlers() throws Exception {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CalcService s = serve((c, o) -> new CalcService(c, o) {
            @Override
            public pbtest.Nothing slow(SlowRequest r, CallContext ctx) throws InterruptedException {
                peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                try {
                    return super.slow(r, ctx);
                } finally {
                    running.decrementAndGet();
                }
            }
        }, MessageServiceOptions.DEFAULT.withLateAck(false).withMaxConcurrent(2), ctx);
        CalcProtobus.Proxy p = proxy();
        CompletableFuture<?>[] calls = new CompletableFuture<?>[8];
        for (int i = 0; i < calls.length; i++) calls[i] = p.slowAsync(SlowRequest.newBuilder().setMs(50).build());
        CompletableFuture.allOf(calls).get(10, TimeUnit.SECONDS);
        assertEquals(8, s.slowStarted.get());
        assertTrue(peak.get() <= 2, "peak " + peak.get());
    }

    @Test
    void callsPendingWhenTheReplyQueueIsReplacedFailAtOnce() {
        serve(MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(60000));
        CalcProtobus.Proxy p = proxy();
        CompletableFuture<?> call = p.slowAsync(SlowRequest.newBuilder().setMs(3000).build());
        String replyQueue = broker.queueNames().stream().filter(n -> n.startsWith("amq.gen-"))
                .filter(n -> !broker.bindings(n, Config.callbacksExchangeName()).isEmpty()).findFirst().orElseThrow();
        broker.closeChannelsConsuming(replyQueue, "406 PRECONDITION_FAILED - unknown delivery tag");
        Exception e = assertThrows(Exception.class, () -> call.get(2, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof DisconnectedError, String.valueOf(e.getCause()));
    }

    @Test
    void aFailedStreamReleasesItsEntryWithoutBeingRead() {
        serve(MessageServiceOptions.DEFAULT.withMaxConcurrent(2).withProcessingTimeoutMs(60000));
        ProtobusStream<Tick> stream = proxy().ticks(TickRequest.newBuilder().setCount(100).setDelayMs(100).build());
        assertTrue(stream.hasNext());
        broker.killConnections();
        assertTrue(eventually(() -> ctx.messageDispatcher().pendingStreamCount() == 0));
    }
}
