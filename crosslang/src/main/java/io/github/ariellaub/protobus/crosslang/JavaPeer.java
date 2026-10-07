package io.github.ariellaub.protobus.crosslang;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.CustomTypes;
import io.github.ariellaub.protobus.EventListener;
import io.github.ariellaub.protobus.HandledError;
import io.github.ariellaub.protobus.LogLevel;
import io.github.ariellaub.protobus.Logger;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.ProtobusStream;
import io.github.ariellaub.protobus.RemoteError;
import io.github.ariellaub.protobus.RetryOptions;
import io.github.ariellaub.protobus.StreamWriter;
import io.github.ariellaub.protobus.types.bigint;
import interop.AddRequest;
import interop.AddResponse;
import interop.Attempted;
import interop.Balance;
import interop.CounterProtobus;
import interop.FailRequest;
import interop.FlakyProtobus;
import interop.Inner;
import interop.Kind;
import interop.ListenerProtobus;
import interop.Nothing;
import interop.Ping;
import interop.Produced;
import interop.Query;
import interop.Tick;
import interop.TickRequest;
import interop.WalletProtobus;
import interop.Who;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The Java participant of the cross-language suites.
 *
 * <pre>
 *   JavaPeer server   serve the interop services; prints READY
 *   JavaPeer client   run the client scenario against PEER_TARGET's services;
 *                     prints PASS/FAIL lines, then DONE
 * </pre>
 *
 * The broker is PROTOBUS_TEST_AMQP. Behaviour mirrors the Go, TypeScript, Python
 * and C++ peers exactly: the same services, the same canonical values, the same
 * fifteen client checks.
 */
public final class JavaPeer {
    private JavaPeer() {}

    static final String LANG = "java";

    static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isEmpty() ? fallback : v;
    }

    /** The Balance every peer returns for an ordinary account. */
    static Balance canonical() {
        return Balance.newBuilder()
                .setAmount(CustomTypes.bigint(BigInteger.TEN.pow(30)))
                .setAsOf(CustomTypes.timestampMillis(1577836800000L)) // 2020-01-01T00:00:00Z
                .setBig(9007199254740993L)
                .addTags("a").addTags("b")
                .putCounts("x", 1).putCounts("y", 2)
                .putBalances("k", CustomTypes.bigint(BigInteger.TWO.pow(200)))
                .addParts(CustomTypes.bigint(1)).addParts(CustomTypes.bigint(2)).addParts(CustomTypes.bigint(3))
                .setKind(Kind.KIND_FUTURE)
                .setInner(Inner.newBuilder().setName("root").setValue(CustomTypes.bigint(7))
                        .addChildren(Inner.newBuilder().setName("leaf").setValue(CustomTypes.bigint(8))))
                .setUbig(-1L) // 18446744073709551615 as an unsigned 64-bit value
                .setBlob(ByteString.copyFrom(new byte[] {0, 1, (byte) 0xff}))
                .setRatio(0.5)
                .setFlag(true)
                .setNeg(-5)
                .setBeforeEpoch(CustomTypes.timestampMillis(-14182940000L)) // 1969-07-20T20:17:40Z
                .setZero(0)
                .build();
    }

    // ---- server --------------------------------------------------------------------

    static final Object producedLock = new Object();
    static Produced produced = Produced.getDefaultInstance();

    static void record(java.util.function.UnaryOperator<Produced.Builder> f) {
        synchronized (producedLock) {
            produced = f.apply(produced.toBuilder()).build();
        }
    }

    static final class Counter extends CounterProtobus.Base {
        private final String name;

        Counter(Context ctx, String name) {
            super(ctx);
            this.name = name;
        }

        @Override
        public String serviceName() {
            return name;
        }

        @Override
        public AddResponse add(AddRequest r, CallContext ctx) {
            return AddResponse.newBuilder().setSum(r.getA() + r.getB()).build();
        }

        @Override
        public void tick(TickRequest r, StreamWriter<Tick> out, CallContext ctx) throws InterruptedException {
            if (r.getEmitNothing()) return;
            record(p -> p.clear());
            for (int i = 0; i < r.getCount(); i++) {
                if (r.getFailAt() > 0 && i >= r.getFailAt()) {
                    if (r.getUnhandled()) throw new IllegalStateException("stream broke");
                    throw new HandledError("deliberate failure at chunk " + i, "TEST_FAIL");
                }
                if (ctx.signal().aborted()) {
                    record(p -> p.setStoppedEarly(true));
                    return;
                }
                out.write(Tick.newBuilder().setSeq(i).setPayload("chunk-" + i).build());
                record(p -> p.setYielded(p.getYielded() + 1));
                if (r.getDelayMs() > 0 && ctx.signal().await(Duration.ofMillis(r.getDelayMs()))) {
                    record(p -> p.setStoppedEarly(true));
                    return;
                }
            }
            record(p -> p.setFinished(true));
        }

        @Override
        public Produced produced(Nothing r, CallContext ctx) {
            synchronized (producedLock) {
                return produced;
            }
        }

        @Override
        public Who whoami(Nothing r, CallContext ctx) {
            return Who.newBuilder().setActor(ctx.actor()).setMessageId(ctx.messageId() == null ? "" : ctx.messageId())
                    .setRoutingKey(ctx.routingKey()).setLang(LANG).build();
        }
    }

    static final class Wallet extends WalletProtobus.Base {
        Wallet(Context ctx, MessageServiceOptions o) {
            super(ctx, o);
        }

        @Override
        public Balance balance(Query q, CallContext ctx) {
            if (q.getAccount().equals("boom")) throw new HandledError("no such account", "NOT_FOUND");
            if (q.getAccount().equals("crash")) throw new IllegalStateException("kaboom");
            return canonical();
        }

        @Override
        public Balance echo(Balance b, CallContext ctx) {
            return b;
        }
    }

    static final class Flaky extends FlakyProtobus.Base {
        Flaky(Context ctx, MessageServiceOptions o) {
            super(ctx, o);
        }

        @Override
        public Nothing fail(FailRequest r, CallContext ctx) {
            context().publishEvent("interop.Attempted",
                    Attempted.newBuilder().setLang(LANG).setMessageId(ctx.messageId()).build(), "EVENT.attempted");
            throw new IllegalStateException("flaky " + LANG);
        }
    }

    static final class Listener extends ListenerProtobus.Base {
        Listener(Context ctx, MessageServiceOptions o) {
            super(ctx, o);
        }

        @Override
        public String serviceName() {
            return "interop.Listener." + LANG;
        }
    }

    static void serve() throws InterruptedException {
        Context ctx = new Context();
        ctx.init(env("PROTOBUS_TEST_AMQP", ""));
        MessageServiceOptions noRetry = MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withMaxRetries(0));
        // Shared with the other languages' replicas: the retry arguments must match theirs.
        MessageServiceOptions flaky = MessageServiceOptions.DEFAULT
                .withRetry(RetryOptions.defaults().withMaxRetries(3).withRetryDelayMs(100)).withMaxConcurrent(4);
        new Counter(ctx, "interop.Counter").init();
        new Counter(ctx, "interop.Counter.inst1").init();
        new Wallet(ctx, noRetry).init();
        new Flaky(ctx, flaky).init();
        Listener listener = new Listener(ctx, noRetry);
        listener.init();
        listener.subscribeEvent(Ping.class, (ping, type, topic) -> ctx.publishEvent("interop.Ping",
                Ping.newBuilder().setId("pong:" + ping.getId())
                        .setN(CustomTypes.bigint(CustomTypes.toBigInteger(ping.getN()).add(BigInteger.ONE)))
                        .setFrom(LANG).build(),
                "EVENT.pong." + LANG), "EVENT.ping." + LANG);
        System.out.println("READY");
        System.out.flush();
        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ctx.close();
            stop.countDown();
        }));
        stop.await();
    }

    // ---- client --------------------------------------------------------------------

    @FunctionalInterface
    interface Check {
        void run() throws Exception;
    }

    static final List<String> failures = new ArrayList<>();

    static void check(String name, Check fn) {
        try {
            fn.run();
            System.out.println("PASS " + name);
        } catch (Throwable e) {
            System.out.println(("FAIL " + name + ": " + e).replace('\n', ' '));
            failures.add(name);
        }
        System.out.flush();
    }

    static void require(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    /** bigints compared by value: a peer may encode one shorter than 32 bytes. */
    static Message normalize(Message m) {
        if (m instanceof bigint) return CustomTypes.bigint(CustomTypes.toBigInteger((bigint) m));
        Message.Builder b = m.toBuilder();
        for (var e : m.getAllFields().entrySet()) {
            var f = e.getKey();
            if (f.getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            if (f.isRepeated()) {
                b.clearField(f);
                for (Object item : (List<?>) e.getValue()) b.addRepeatedField(f, normalize((Message) item));
            } else {
                b.setField(f, normalize((Message) e.getValue()));
            }
        }
        return b.build();
    }

    static void eq(Object got, Object want, String what) {
        Object g = got instanceof Message ? normalize((Message) got) : got;
        Object w = want instanceof Message ? normalize((Message) want) : want;
        require(g.equals(w), what + ": got " + g + ", want " + w);
    }

    static RemoteError expectRemote(Check fn) throws Exception {
        try {
            fn.run();
        } catch (RemoteError e) {
            return e;
        }
        throw new AssertionError("expected a RemoteError");
    }

    static void client() {
        String target = env("PEER_TARGET", LANG);
        Context ctx = new Context();
        ctx.init(env("PROTOBUS_TEST_AMQP", ""));
        CounterProtobus.Proxy counter = new CounterProtobus.Proxy(ctx);
        counter.init();
        CounterProtobus.Proxy inst = new CounterProtobus.Proxy(ctx, "interop.Counter.inst1");
        inst.init();
        WalletProtobus.Proxy wallet = new WalletProtobus.Proxy(ctx);
        wallet.init();

        check("unary", () -> eq(counter.add(AddRequest.newBuilder().setA(2).setB(3).build()).getSum(), 5, "add"));
        check("priority on a plain queue", () -> eq(counter.add(AddRequest.newBuilder().setA(1).setB(1).build(),
                CallOptions.DEFAULT.withPriority(2)).getSum(), 2, "add"));
        check("stream in order", () -> {
            List<Integer> seqs = new ArrayList<>();
            List<String> payloads = new ArrayList<>();
            try (ProtobusStream<Tick> s = counter.tick(TickRequest.newBuilder().setCount(5).build())) {
                for (Tick t : s) {
                    seqs.add(t.getSeq());
                    payloads.add(t.getPayload());
                }
            }
            eq(seqs, List.of(0, 1, 2, 3, 4), "seq");
            eq(payloads, List.of("chunk-0", "chunk-1", "chunk-2", "chunk-3", "chunk-4"), "payload");
        });
        check("empty stream", () -> {
            int n = 0;
            try (ProtobusStream<Tick> s = counter.tick(TickRequest.newBuilder().setEmitNothing(true).build())) {
                for (Tick ignored : s) n++;
            }
            eq(n, 0, "chunks");
        });
        check("mid-stream handled error", () -> {
            List<Tick> got = new ArrayList<>();
            RemoteError e = expectRemote(() -> {
                try (ProtobusStream<Tick> s = counter.tick(TickRequest.newBuilder().setCount(10).setFailAt(2).build())) {
                    for (Tick t : s) got.add(t);
                }
            });
            eq(e.code(), "TEST_FAIL", "code");
            require(e.getMessage().contains("deliberate failure at chunk 2"), e.getMessage());
            eq(got.size(), 2, "chunks before the error");
        });
        check("mid-stream unhandled error", () -> {
            RemoteError e = expectRemote(() -> {
                try (ProtobusStream<Tick> s = counter.tick(TickRequest.newBuilder().setCount(10).setFailAt(1)
                        .setUnhandled(true).build())) {
                    for (Tick ignored : s) {
                        // drain
                    }
                }
            });
            eq(e.getMessage(), "stream broke", "message");
        });
        check("cancellation reaches the producer", () -> {
            int n = 0;
            try (ProtobusStream<Tick> s = counter.tick(TickRequest.newBuilder().setCount(500).setDelayMs(10).build())) {
                for (Tick ignored : s) {
                    if (++n == 3) break;
                }
            }
            Produced p = null;
            for (int i = 0; i < 100; i++) {
                p = counter.produced(Nothing.getDefaultInstance());
                if (p.getStoppedEarly()) break;
                Thread.sleep(50);
            }
            require(p.getStoppedEarly() && !p.getFinished() && p.getYielded() < 500, "produced " + p);
        });
        check("custom types, defaults and maps", () -> eq(wallet.balance(Query.newBuilder().setAccount("acc").build()),
                canonical(), "balance"));
        check("echo round trip", () -> eq(wallet.echo(canonical()), canonical(), "echo"));
        check("handled error", () -> {
            RemoteError e = expectRemote(() -> wallet.balance(Query.newBuilder().setAccount("boom").build()));
            eq(e.code(), "NOT_FOUND", "code");
            eq(e.getMessage(), "no such account", "message");
        });
        check("unhandled error", () -> {
            RemoteError e = expectRemote(() -> wallet.balance(Query.newBuilder().setAccount("crash").build()));
            eq(e.getMessage(), "kaboom", "message");
        });
        check("unimplemented method", () -> {
            RemoteError e = expectRemote(() -> counter.unimplemented(Nothing.getDefaultInstance()));
            eq(e.code(), "PROTOCOL_ERROR", "code");
        });
        check("call metadata", () -> {
            Who who = counter.whoami(Nothing.getDefaultInstance(),
                    CallOptions.DEFAULT.withActor("client-java").withMessageId("mid-java-1"));
            eq(who, Who.newBuilder().setActor("client-java").setMessageId("mid-java-1")
                    .setRoutingKey("REQUEST.interop.Counter.whoami").setLang(target).build(), "who");
        });
        check("instance routing", () -> eq(inst.whoami(Nothing.getDefaultInstance()).getRoutingKey(),
                "REQUEST.interop.Counter.inst1.whoami", "routing key"));
        check("events both ways", () -> {
            EventListener listener = new EventListener(ctx.connection(), ctx.factory(), null);
            listener.init(null, "");
            CompletableFuture<Ping> got = new CompletableFuture<>();
            listener.subscribe(Ping.getDefaultInstance(), (e, t, x) -> got.complete(e), "EVENT.pong." + target);
            listener.start();
            BigInteger n = BigInteger.TWO.pow(70);
            ctx.publishEvent("interop.Ping", Ping.newBuilder().setId("java-1").setN(CustomTypes.bigint(n))
                    .setFrom(LANG).build(), "EVENT.ping." + target);
            Ping pong = got.get(10, TimeUnit.SECONDS);
            eq(pong, Ping.newBuilder().setId("pong:java-1").setN(CustomTypes.bigint(n.add(BigInteger.ONE)))
                    .setFrom(target).build(), "pong");
            listener.close();
        });

        System.out.println("DONE");
        System.out.flush();
        ctx.close();
        System.exit(failures.isEmpty() ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Logger.setLevel(System.getenv("PROTOBUS_TEST_LOG") != null ? LogLevel.DEBUG : LogLevel.ERROR);
        String mode = args.length > 0 ? args[0] : "client";
        try {
            if (mode.equals("server")) serve();
            else client();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
    }
}
