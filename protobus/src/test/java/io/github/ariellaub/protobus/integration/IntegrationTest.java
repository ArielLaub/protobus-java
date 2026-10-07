package io.github.ariellaub.protobus.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Config;
import io.github.ariellaub.protobus.Connection;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.ContextOptions;
import io.github.ariellaub.protobus.CustomTypes;
import io.github.ariellaub.protobus.EventRetryOptions;
import io.github.ariellaub.protobus.HandledError;
import io.github.ariellaub.protobus.LogLevel;
import io.github.ariellaub.protobus.Logger;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.ProtobusStream;
import io.github.ariellaub.protobus.RemoteError;
import io.github.ariellaub.protobus.RetryOptions;
import io.github.ariellaub.protobus.RetryQueueMismatchError;
import io.github.ariellaub.protobus.StreamWriter;
import io.github.ariellaub.protobus.UnroutableError;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.internal.Headers;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pbtest.AddRequest;
import pbtest.AddResponse;
import pbtest.CalcProtobus;
import pbtest.FailRequest;
import pbtest.Nothing;
import pbtest.Ping;
import pbtest.SlowRequest;
import pbtest.Tick;
import pbtest.TickRequest;
import pbtest.Wallet;
import pbtest.Who;

/** The suites that need a real RabbitMQ. */
@Tag("integration")
class IntegrationTest {
    private final List<Context> contexts = new ArrayList<>();
    private String name;

    /** A Calc service under a unique instance name, so suites never share queues. */
    static class Calc extends CalcProtobus.Base {
        final String name;
        final AtomicInteger failAttempts = new AtomicInteger();
        final AtomicInteger stopped = new AtomicInteger();

        Calc(Context c, MessageServiceOptions o, String name) {
            super(c, o);
            this.name = name;
        }

        @Override
        public String serviceName() {
            return name;
        }

        @Override
        public AddResponse add(AddRequest r, CallContext ctx) {
            return AddResponse.newBuilder().setResult(r.getA() + r.getB()).build();
        }

        @Override
        public Nothing fail(FailRequest r, CallContext ctx) {
            failAttempts.incrementAndGet();
            if (r.getHandled()) throw new HandledError("refused", "REFUSED");
            throw new IllegalStateException("boom");
        }

        @Override
        public Nothing slow(SlowRequest r, CallContext ctx) throws InterruptedException {
            ctx.signal().await(Duration.ofMillis(r.getMs()));
            return Nothing.getDefaultInstance();
        }

        @Override
        public void ticks(TickRequest r, StreamWriter<Tick> out, CallContext ctx) throws InterruptedException {
            for (int i = 0; i < r.getCount(); i++) {
                if (ctx.signal().aborted()) {
                    stopped.incrementAndGet();
                    return;
                }
                out.write(Tick.newBuilder().setSeq(i).build());
                if (r.getDelayMs() > 0) ctx.signal().await(Duration.ofMillis(r.getDelayMs()));
            }
        }

        @Override
        public Who whoami(Nothing r, CallContext ctx) {
            return Who.newBuilder().setActor(ctx.actor()).setMessageId(ctx.messageId()).setRoutingKey(ctx.routingKey())
                    .build();
        }

        @Override
        public Wallet echo(Wallet w, CallContext ctx) {
            return w;
        }
    }

    @BeforeEach
    void setUp() {
        Broker.amqpUrl();
        Config.reset();
        Config.set("RPC_CALL_TIMEOUT_MS", "15000");
        if (System.getenv("PROTOBUS_TEST_LOG") == null) Logger.setLevel(LogLevel.SILENT);
        name = "pbtest.Calc.it" + UUID.randomUUID().toString().substring(0, 8);
    }

    @AfterEach
    void tearDown() {
        for (Context c : contexts) c.close();
        Broker.deleteService(name);
        Config.reset();
        Logger.setLevel(LogLevel.INFO);
    }

    Context context() {
        Context c = new Context(ContextOptions.DEFAULT.withReconnection(
                Connection.ReconnectionOptions.defaults().withInitialDelayMs(100).withMaxDelayMs(500)));
        c.init(Broker.amqpUrl());
        contexts.add(c);
        return c;
    }

    Calc serve(Context c, MessageServiceOptions o) {
        Calc s = new Calc(c, o, name);
        s.init();
        return s;
    }

    CalcProtobus.Proxy proxy(Context c) {
        CalcProtobus.Proxy p = new CalcProtobus.Proxy(c, name);
        p.init();
        return p;
    }

    @Test
    void unaryStreamingAndCustomTypesOverRabbitMq() {
        Context c = context();
        serve(c, MessageServiceOptions.DEFAULT.withMaxConcurrent(4));
        CalcProtobus.Proxy p = proxy(c);
        assertEquals(42, p.add(AddRequest.newBuilder().setA(40).setB(2).build()).getResult());
        Who who = p.whoami(Nothing.getDefaultInstance(), CallOptions.DEFAULT.withActor("it").withMessageId("m-1"));
        assertEquals("it", who.getActor());
        assertEquals("m-1", who.getMessageId());
        assertEquals("REQUEST." + name + ".whoami", who.getRoutingKey());
        List<Integer> seqs = new ArrayList<>();
        try (ProtobusStream<Tick> s = p.ticks(TickRequest.newBuilder().setCount(20).build())) {
            for (Tick t : s) seqs.add(t.getSeq());
        }
        assertEquals(20, seqs.size());
        assertEquals(19, seqs.get(19));
        Wallet w = Wallet.newBuilder().setAmount(CustomTypes.bigint(CustomTypes.BIGINT_MAX))
                .setAt(CustomTypes.timestamp(Instant.parse("1969-07-20T20:17:40Z")))
                .putBalances("k", CustomTypes.bigint(BigInteger.TWO.pow(200))).build();
        assertEquals(w, p.echo(w));
    }

    @Test
    void theRetryLadderRunsOnRealTtlAndDeadLettering() throws Exception {
        Context c = context();
        Calc s = serve(c, MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withRetryDelayMs(100)));
        CalcProtobus.Proxy p = proxy(c);
        RemoteError e = assertThrows(RemoteError.class, () -> p.fail(FailRequest.newBuilder().setId("r").build(),
                CallOptions.DEFAULT.withPriority(1).withMessageId("ladder-1")));
        assertEquals("boom", e.getMessage());
        assertEquals(4, s.failAttempts.get());
        assertTrue(MemoryBroker.waitFor(() -> Broker.queueMessages(name + ".DLQ") == 1, Duration.ofSeconds(10)));
        // Read the dead-letter copy as the broker stores it.
        com.rabbitmq.client.ConnectionFactory f = new com.rabbitmq.client.ConnectionFactory();
        f.setUri(Broker.amqpUrl());
        if (f.getVirtualHost().isEmpty()) f.setVirtualHost("/");
        try (com.rabbitmq.client.Connection raw = f.newConnection();
             com.rabbitmq.client.Channel ch = raw.createChannel()) {
            GetResponse dead = ch.basicGet(name + ".DLQ", true);
            BasicProperties props = dead.getProps();
            Map<String, Object> h = props.getHeaders();
            assertEquals(3L, Headers.integer(h.get("x-retry-count")));
            assertEquals("REQUEST." + name + ".fail", Headers.text(h.get("x-original-routing-key")));
            assertEquals(name, Headers.text(h.get("x-original-queue")));
            assertEquals("IllegalStateException", Headers.text(h.get("x-last-error")));
            assertTrue(Headers.integer(h.get("x-dlq-time")) > 0);
            assertEquals("ladder-1", props.getMessageId());
            assertEquals(1, props.getPriority());
            assertEquals("application/octet-stream", props.getContentType());
            assertEquals(2, props.getDeliveryMode());
        }
    }

    @Test
    void aCallToNoServiceIsUnroutableAtOnce() {
        Context c = context();
        CalcProtobus.Proxy p = proxy(c);
        long start = System.nanoTime();
        assertThrows(UnroutableError.class, () -> p.add(AddRequest.getDefaultInstance()));
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
    }

    @Test
    void returnsAreMatchedToTheirPublishWhenMessageIdsRepeat() throws Exception {
        Context c = context();
        Connection conn = c.connection();
        AmqpChannel ch = conn.openChannel();
        String queue = name;
        conn.declareQueue(ch, queue, false, false, true, Map.of());
        String exchange = name + ".x";
        conn.declareExchange(ch, exchange, "topic");
        conn.bindQueue(ch, queue, exchange, "routed");
        try {
            BasicProperties same = new BasicProperties.Builder().messageId("same-id").build();
            List<CompletableFuture<String>> routed = new ArrayList<>();
            List<CompletableFuture<String>> unroutable = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                routed.add(conn.publishAsync(ch, exchange, "routed", new byte[] {1},
                        new Connection.PublishOptions(same, true)));
                unroutable.add(conn.publishAsync(ch, exchange, "nowhere", new byte[] {2},
                        new Connection.PublishOptions(same, true)));
            }
            for (CompletableFuture<String> f : routed) assertEquals("same-id", f.get(10, TimeUnit.SECONDS));
            for (CompletableFuture<String> f : unroutable) {
                Exception e = assertThrows(Exception.class, () -> f.get(10, TimeUnit.SECONDS));
                assertTrue(e.getCause() instanceof UnroutableError, String.valueOf(e.getCause()));
            }
        } finally {
            Broker.deleteExchange(exchange);
        }
    }

    @Test
    void servicesComeBackWhenTheBrokerDropsTheConnection() {
        Context c = context();
        serve(c, MessageServiceOptions.DEFAULT);
        CalcProtobus.Proxy p = proxy(c);
        assertEquals(2, p.add(AddRequest.newBuilder().setA(1).setB(1).build()).getResult());
        AtomicInteger reconnected = new AtomicInteger();
        c.connection().onReconnected(reconnected::incrementAndGet);
        assertTrue(MemoryBroker.waitFor(() -> Broker.closeConnections() > 0, Duration.ofSeconds(10)));
        assertTrue(MemoryBroker.waitFor(() -> reconnected.get() >= 1, Duration.ofSeconds(20)));
        assertEquals(5, p.add(AddRequest.newBuilder().setA(2).setB(3).build()).getResult());
    }

    @Test
    void aChangedRetryDelayIsReportedPlainly() {
        Context c = context();
        serve(c, MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withRetryDelayMs(1000))).close();
        Context other = context();
        Calc s = new Calc(other, MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withRetryDelayMs(2000)),
                name);
        RetryQueueMismatchError e = assertThrows(RetryQueueMismatchError.class, s::init);
        assertTrue(e.getMessage().contains("retryDelayMs"));
    }

    @Test
    void aPriorityQueueServesHigherPrioritiesFirst() throws Exception {
        Context c = context();
        Calc s = serve(c, MessageServiceOptions.DEFAULT.withMaxPriority(2));
        List<String> order = new CopyOnWriteArrayList<>();
        s.stopConsuming();
        // Queue them up behind a stopped consumer, then let it go.
        CalcProtobus.Proxy p = proxy(c);
        List<CompletableFuture<Who>> calls = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            calls.add(p.whoamiAsync(Nothing.getDefaultInstance(), CallOptions.DEFAULT.withActor("low" + i)
                    .withPriority(Config.PRIORITY_NORMAL)));
        }
        calls.add(p.whoamiAsync(Nothing.getDefaultInstance(), CallOptions.DEFAULT.withActor("high")
                .withPriority(Config.PRIORITY_CONTROL)));
        assertTrue(MemoryBroker.waitFor(() -> Broker.queueMessages(name) == 4, Duration.ofSeconds(10)));
        Calc second = new Calc(c, MessageServiceOptions.DEFAULT.withMaxPriority(2), name) {
            @Override
            public Who whoami(Nothing r, CallContext ctx) {
                order.add(ctx.actor());
                return super.whoami(r, ctx);
            }
        };
        second.init();
        for (CompletableFuture<Who> f : calls) f.get(10, TimeUnit.SECONDS);
        assertEquals("high", order.get(0));
        assertFalse(order.subList(1, 4).contains("high"));
    }

    @Test
    void eventsAndEventRetryOverRabbitMq() {
        Context c = context();
        Calc s = serve(c, MessageServiceOptions.DEFAULT.withEventRetry(EventRetryOptions.of(1).withRetryDelayMs(100)));
        AtomicInteger attempts = new AtomicInteger();
        List<String> ok = new CopyOnWriteArrayList<>();
        String topic = "EVENT." + name;
        s.subscribeEvent(Ping.class, (e, t, x) -> {
            if (e.getId().equals("bad")) {
                attempts.incrementAndGet();
                throw new IllegalStateException("no");
            }
            ok.add(e.getId());
        }, topic);
        c.publishEvent("pbtest.Ping", Ping.newBuilder().setId("good").setN(CustomTypes.bigint(1)).build(), topic);
        c.publishEvent("pbtest.Ping", Ping.newBuilder().setId("bad").build(), topic);
        assertTrue(MemoryBroker.waitFor(() -> ok.size() == 1, Duration.ofSeconds(10)));
        assertTrue(MemoryBroker.waitFor(() -> Broker.queueMessages(name + ".Events.DLQ") == 1,
                Duration.ofSeconds(10)));
        assertEquals(2, attempts.get());
    }

    @Test
    void cancellingAStreamReachesTheProducerAcrossTheBroker() {
        Context c = context();
        Calc s = serve(c, MessageServiceOptions.DEFAULT);
        CalcProtobus.Proxy p = proxy(c);
        try (ProtobusStream<Tick> stream = p.ticks(TickRequest.newBuilder().setCount(1000).setDelayMs(20).build())) {
            assertTrue(stream.hasNext());
            stream.next();
        }
        assertTrue(MemoryBroker.waitFor(() -> s.stopped.get() == 1, Duration.ofSeconds(10)));
    }
}
