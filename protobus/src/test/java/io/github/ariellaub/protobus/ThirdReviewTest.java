package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.AmqpConnection;
import io.github.ariellaub.protobus.amqp.ConfirmOutcome;
import io.github.ariellaub.protobus.amqp.Delivery;
import io.github.ariellaub.protobus.amqp.Transport;
import io.github.ariellaub.protobus.internal.Envelopes;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import pbtest.SlowRequest;
import pbtest.Tick;
import pbtest.TickRequest;

/** Regressions for the findings of the third review. */
class ThirdReviewTest extends MemoryBus {
    @Test
    void aBoundedExecutorNeverStrandsQueuedDeliveries() throws Exception {
        // One worker and no queue: the executor refuses whatever it cannot start now.
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new SynchronousQueue<>());
        Context c = newContext(ContextOptions.DEFAULT.withHandlerExecutor(pool));
        CalcService s = serve(CalcService::new,
                MessageServiceOptions.DEFAULT.withLateAck(false).withMaxConcurrent(1), c);
        pbtest.CalcProtobus.Proxy p = proxy();
        CompletableFuture<?>[] calls = new CompletableFuture<?>[3];
        for (int i = 0; i < calls.length; i++) calls[i] = p.slowAsync(SlowRequest.newBuilder().setMs(30).build());
        CompletableFuture.allOf(calls).get(10, TimeUnit.SECONDS);
        assertEquals(3, s.slowStarted.get());
        assertTrue(c.connection().drainInFlight(2000));
        pool.shutdown();
    }

    /** A transport whose writes can be made to block, as a full socket would. */
    static final class StallingTransport implements Transport {
        final Transport inner;
        volatile CountDownLatch stall;
        /** Counted down when a write reaches the stall, so a test knows it is blocked. */
        volatile CountDownLatch entered = new CountDownLatch(1);

        StallingTransport(Transport inner) {
            this.inner = inner;
        }

        @Override
        public AmqpConnection connect(String url, int heartbeatSeconds) {
            AmqpConnection c = inner.connect(url, heartbeatSeconds);
            return new AmqpConnection() {
                @Override
                public AmqpChannel openChannel() {
                    return new Stalling(c.openChannel());
                }

                @Override
                public void close() {
                    c.close();
                }

                @Override
                public boolean isOpen() {
                    return c.isOpen();
                }

                @Override
                public void onClose(Consumer<String> listener) {
                    c.onClose(listener);
                }
            };
        }

        final class Stalling implements AmqpChannel {
            final AmqpChannel ch;

            Stalling(AmqpChannel ch) {
                this.ch = ch;
            }

            @Override
            public void publish(String exchange, String routingKey, byte[] body, BasicProperties properties,
                                boolean mandatory, BiConsumer<ConfirmOutcome, String> onConfirm) {
                CountDownLatch s = stall;
                if (s != null) {
                    entered.countDown();
                    try {
                        s.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                ch.publish(exchange, routingKey, body, properties, mandatory, onConfirm);
            }

            @Override public void declareExchange(String n, String t, boolean d, boolean a, boolean i, Map<String, Object> args) { ch.declareExchange(n, t, d, a, i, args); }
            @Override public String declareQueue(String n, boolean d, boolean e, boolean a, Map<String, Object> args) { return ch.declareQueue(n, d, e, a, args); }
            @Override public void bindQueue(String q, String x, String k, Map<String, Object> a) { ch.bindQueue(q, x, k, a); }
            @Override public void unbindQueue(String q, String x, String k, Map<String, Object> a) { ch.unbindQueue(q, x, k, a); }
            @Override public void deleteQueue(String n) { ch.deleteQueue(n); }
            @Override public void purgeQueue(String n) { ch.purgeQueue(n); }
            @Override public void prefetch(int count) { ch.prefetch(count); }
            @Override public String consume(String q, String t, boolean n, boolean e, Consumer<Delivery> d, Runnable c) { return ch.consume(q, t, n, e, d, c); }
            @Override public void cancel(String t) { ch.cancel(t); }
            @Override public void ack(long t) { ch.ack(t); }
            @Override public void reject(long t, boolean r) { ch.reject(t, r); }
            @Override public void close() { ch.close(); }
            @Override public boolean isOpen() { return ch.isOpen(); }
            @Override public void onClose(Consumer<String> l) { ch.onClose(l); }
        }
    }

    @Test
    void aBlockedWriteDoesNotHoldTheCallerPastItsDeadline() throws Exception {
        StallingTransport transport = new StallingTransport(broker);
        Context c = new Context(ContextOptions.DEFAULT.withTransport(transport));
        c.init("amqp://memory/");
        contexts.add(c);
        serve();
        transport.stall = new CountDownLatch(1);
        // Released after a second whatever happens, so a regression fails rather than hangs.
        CountDownLatch stall = transport.stall;
        Thread release = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                return;
            }
            stall.countDown();
        });
        release.setDaemon(true);
        release.start();
        try {
            byte[] request = Envelopes.encodeRequest(new Envelopes.Request("pbtest.Calc.add", null,
                    RpcTest.add(1, 1).toByteArray()));
            long start = System.nanoTime();
            CompletableFuture<byte[]> call = c.publishMessageAsync(request, "REQUEST.pbtest.Calc.add",
                    CallOptions.DEFAULT.withTimeoutMs(50));
            long returned = System.nanoTime() - start;
            assertTrue(returned < TimeUnit.MILLISECONDS.toNanos(200), "publishMessageAsync took "
                    + returned / 1_000_000 + " ms to return");
            ExecutionException e = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class,
                    () -> call.get(2, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof RpcTimeoutError, String.valueOf(e.getCause()));
        } finally {
            transport.stall.countDown();
        }
    }

    @Test
    void aCancelledStreamStaysCancelledWhenItsPublishFailsLater() throws Exception {
        Config.set("PUBLISH_CONFIRM_TIMEOUT_MS", "100");
        serve();
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        AbortController stop = new AbortController();
        ProtobusStream<Tick> stream = proxy().ticks(TickRequest.newBuilder().setCount(3).setDelayMs(1000).build(),
                StreamOptions.DEFAULT.withSignal(stop.signal()));
        stop.abort();
        Thread.sleep(300); // past the request's confirm deadline
        assertFalse(stream.hasNext());
    }
}
