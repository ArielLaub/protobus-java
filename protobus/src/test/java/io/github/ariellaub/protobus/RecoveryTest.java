package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import pbtest.AddResponse;
import pbtest.CalcProtobus;
import pbtest.Ping;
import pbtest.SlowRequest;
import pbtest.Tick;
import pbtest.TickRequest;

class RecoveryTest extends MemoryBus {
    @Test
    void servicesAndClientsComeBackAfterAConnectionLoss() {
        CalcService s = serve();
        CalcProtobus.Proxy p = proxy();
        assertEquals(3, p.add(RpcTest.add(1, 2)).getResult());
        AtomicInteger reconnected = new AtomicInteger();
        ctx.connection().onReconnected(reconnected::incrementAndGet);
        broker.killConnections();
        assertTrue(eventually(() -> reconnected.get() == 1));
        assertTrue(ctx.connection().isReady());
        assertEquals(7, p.add(RpcTest.add(3, 4)).getResult());
        assertEquals(1, broker.consumerCount("pbtest.Calc"));
        // Events too: the event queue's consumer and bindings are back.
        List<String> got = new CopyOnWriteArrayList<>();
        s.subscribeEvent(Ping.class, (e, t, x) -> got.add(e.getId()));
        ctx.publishEvent(EventsTest.ping("after"));
        assertTrue(eventually(() -> got.size() == 1));
    }

    @Test
    void pendingCallsAndStreamsFailWhenTheConnectionDrops() {
        serve(MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(60000).withMaxConcurrent(2));
        CalcProtobus.Proxy p = proxy();
        CompletableFuture<?> call = p.slowAsync(SlowRequest.newBuilder().setMs(3000).build());
        ProtobusStream<Tick> stream = p.ticks(TickRequest.newBuilder().setCount(100).setDelayMs(200).build());
        assertTrue(stream.hasNext());
        stream.next();
        broker.killConnections();
        Exception e = assertThrows(Exception.class, () -> call.get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof DisconnectedError, String.valueOf(e.getCause()));
        assertThrows(DisconnectedError.class, () -> {
            while (stream.hasNext()) stream.next();
        });
    }

    @Test
    void aCallMadeDuringAReconnectionWaitsForIt() throws Exception {
        serve();
        CalcProtobus.Proxy p = proxy();
        broker.refuseConnections(true);
        broker.killConnections();
        assertTrue(eventually(() -> ctx.isReconnecting()));
        CompletableFuture<AddResponse> call = p.addAsync(RpcTest.add(2, 2));
        Thread.sleep(50);
        assertFalse(call.isDone());
        broker.refuseConnections(false);
        assertEquals(4, call.get(5, TimeUnit.SECONDS).getResult());
    }

    @Test
    void aReconnectionThatNeverSucceedsGivesUp() {
        Context c = newContext(ContextOptions.DEFAULT.withReconnection(fastReconnect().withMaxRetries(3)));
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        c.connection().onError(errors::add);
        broker.refuseConnections(true);
        broker.killConnections();
        assertTrue(eventually(() -> !errors.isEmpty()));
        assertTrue(errors.get(0) instanceof ReconnectionError);
        NotReadyError e = assertThrows(NotReadyError.class, () -> c.connection().whenReady(1000));
        assertTrue(e.getMessage().contains("max reconnection attempts"));
    }

    @Test
    void aWaitOnReadinessIsBounded() {
        broker.refuseConnections(true);
        broker.killConnections();
        assertTrue(eventually(() -> ctx.isReconnecting()));
        assertThrows(NotReadyError.class, () -> ctx.connection().whenReady(50));
    }

    @Test
    void aChannelLostOnALiveConnectionIsRebuilt() {
        serve();
        CalcProtobus.Proxy p = proxy();
        broker.closeChannelsConsuming("pbtest.Calc", "541 INTERNAL_ERROR");
        assertTrue(eventually(() -> broker.consumerCount("pbtest.Calc") == 1, Duration.ofSeconds(5)));
        assertEquals(9, p.add(RpcTest.add(4, 5)).getResult());
        assertEquals(1, broker.openConnections() > 0 ? 1 : 0);
    }

    @Test
    void aConsumerCancelledByTheBrokerIsRestored() {
        serve();
        broker.cancelConsumers("pbtest.Calc");
        assertTrue(eventually(() -> broker.consumerCount("pbtest.Calc") == 1));
        assertEquals(2, proxy().add(RpcTest.add(1, 1)).getResult());
    }

    @Test
    void theReplyQueueIsRebuiltWhenItsChannelIsLost() {
        serve();
        CalcProtobus.Proxy p = proxy();
        String replyQueue = broker.queueNames().stream().filter(n -> n.startsWith("amq.gen-")).filter(n ->
                !broker.bindings(n, Config.callbacksExchangeName()).isEmpty()).findFirst().orElseThrow();
        broker.closeChannelsConsuming(replyQueue, "406 PRECONDITION_FAILED - unknown delivery tag");
        assertTrue(eventually(() -> {
            try {
                return p.add(RpcTest.add(1, 1), CallOptions.DEFAULT.withTimeoutMs(300)).getResult() == 2;
            } catch (RuntimeException e) {
                return false;
            }
        }));
    }

    @Test
    void aStoppedServiceIsNotRestoredByAReconnection() {
        CalcService s = serve();
        s.stopConsuming();
        AtomicInteger reconnected = new AtomicInteger();
        ctx.connection().onReconnected(reconnected::incrementAndGet);
        broker.killConnections();
        assertTrue(eventually(() -> reconnected.get() == 1));
        assertEquals(0, broker.consumerCount("pbtest.Calc"));
    }
}
