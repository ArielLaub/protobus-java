package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import pbtest.CalcProtobus;
import pbtest.SlowRequest;

class LifecycleTest extends MemoryBus {
    @Test
    void drainWaitsForRunningHandlers() throws Exception {
        CalcService s = serve(MessageServiceOptions.DEFAULT.withMaxConcurrent(4));
        CompletableFuture<?> call = proxy().slowAsync(SlowRequest.newBuilder().setMs(200).build());
        assertTrue(eventually(() -> s.slowStarted.get() == 1));
        s.stopConsuming();
        assertEquals(1, ctx.connection().inFlightDeliveries());
        assertTrue(ctx.connection().drainInFlight(5000));
        // Stopping kept the channel open, so the reply still went out.
        call.get(5, TimeUnit.SECONDS);
    }

    @Test
    void drainReportsADeadlineItMissed() {
        CalcService s = serve(MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(60000));
        proxy().slowAsync(SlowRequest.newBuilder().setMs(2000).build());
        assertTrue(eventually(() -> s.slowStarted.get() == 1));
        assertFalse(ctx.connection().drainInFlight(50));
    }

    @Test
    void aHandlerOutlivingItsTimeoutStillCountsAsRunning() {
        // A handler that ignores its signal: the timeout settles its delivery, but
        // it is still running, and a drain must wait for it.
        serve((c, o) -> new CalcService(c, o) {
            @Override
            public pbtest.Nothing slow(SlowRequest r, CallContext ctx) throws InterruptedException {
                Thread.sleep(r.getMs());
                return pbtest.Nothing.getDefaultInstance();
            }
        }, MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(30)
                .withRetry(RetryOptions.defaults().withMaxRetries(0)), ctx);
        RemoteError e = assertThrows(RemoteError.class, () -> proxy().slow(SlowRequest.newBuilder().setMs(500).build()));
        assertEquals("PROCESSING_TIMEOUT", e.code());
        assertEquals(1, ctx.connection().inFlightDeliveries());
        assertFalse(ctx.connection().drainInFlight(50));
        assertTrue(ctx.connection().drainInFlight(5000));
        assertEquals(0, ctx.connection().inFlightDeliveries());
    }

    @Test
    void handlersRunInParallelUpToThePrefetch() throws Exception {
        CalcService s = serve(MessageServiceOptions.DEFAULT.withMaxConcurrent(3));
        CalcProtobus.Proxy p = proxy();
        for (int i = 0; i < 5; i++) p.slowAsync(SlowRequest.newBuilder().setMs(300).build());
        assertTrue(eventually(() -> s.slowStarted.get() == 3));
        Thread.sleep(100);
        assertEquals(3, s.slowStarted.get());
    }

    @Test
    void aSuppliedExecutorRunsTheHandlers() {
        var pool = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "custom-handler");
            t.setDaemon(true);
            return t;
        });
        Context c = newContext(ContextOptions.DEFAULT.withHandlerExecutor(pool));
        String[] thread = new String[1];
        CalcService s = new CalcService(c, MessageServiceOptions.DEFAULT) {
            @Override
            public pbtest.AddResponse add(pbtest.AddRequest r, CallContext ctx) {
                thread[0] = Thread.currentThread().getName();
                return super.add(r, ctx);
            }
        };
        s.init();
        services.add(s);
        assertEquals(2, proxy().add(RpcTest.add(1, 1)).getResult());
        assertEquals("custom-handler", thread[0]);
        pool.shutdown();
    }

    @Test
    void closingTheContextFailsPendingCalls() throws Exception {
        serve(MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(60000));
        Context client = newContext();
        CalcProtobus.Proxy p = new CalcProtobus.Proxy(client, CalcProtobus.SERVICE_NAME);
        p.init();
        CompletableFuture<?> call = p.slowAsync(SlowRequest.newBuilder().setMs(3000).build());
        Thread.sleep(50);
        client.close();
        Exception e = assertThrows(Exception.class, () -> call.get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof DisconnectedError, String.valueOf(e.getCause()));
        assertThrows(NotConnectedError.class, () -> p.add(RpcTest.add(1, 1)));
    }

    @Test
    void aServiceThatFailsToStartLeavesNothingConsuming() {
        // A retry queue declared with another TTL: the declare is refused.
        serve(MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withRetryDelayMs(1000))).close();
        Context other = newContext();
        CalcService s = new CalcService(other,
                MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withRetryDelayMs(2000)));
        assertThrows(RetryQueueMismatchError.class, s::init);
        assertEquals(0, broker.consumerCount("pbtest.Calc"));
    }

    @Test
    void anUnknownServiceNameIsRefused() {
        CalcService s = new CalcService.Instance(ctx, MessageServiceOptions.DEFAULT, "nope.Nothing");
        // Its own schema is registered, but no prefix of the name is a service.
        assertThrows(MissingProto.class, s::init);
        assertThrows(InvalidServiceNameError.class, () -> new ServiceProxy(ctx, "nope.Nothing").init());
        ServiceProxy p = new ServiceProxy(ctx, "pbtest.Calc");
        p.init();
        assertThrows(AlreadyInitializedError.class, p::init);
    }

    @Test
    void runnableServiceShutsDownGracefully() throws Exception {
        RunnableService.resetForTests();
        Context c = newContext();
        CalcService s = RunnableService.start(c, CalcService::new, MessageServiceOptions.DEFAULT, null);
        RunnableService.requestShutdown();
        assertEquals(0, RunnableService.awaitShutdown());
        assertTrue(s.cleanedUp.get());
        assertFalse(c.isConnected());
        RunnableService.resetForTests();
    }
}
