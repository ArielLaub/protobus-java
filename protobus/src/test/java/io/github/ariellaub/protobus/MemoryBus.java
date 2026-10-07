package io.github.ariellaub.protobus;

import io.github.ariellaub.protobus.amqp.Delivery;
import io.github.ariellaub.protobus.internal.Headers;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import pbtest.CalcProtobus;

/** A bus on the in-memory broker: a broker, a context and helpers to run services and proxies on it. */
abstract class MemoryBus {
    MemoryBroker broker;
    Context ctx;
    final List<MessageService> services = new ArrayList<>();
    final List<Context> contexts = new ArrayList<>();

    @BeforeEach
    void setUpBus() {
        Config.reset();
        Config.set("RPC_CALL_TIMEOUT_MS", "10000");
        Config.set("STREAM_IDLE_TIMEOUT_MS", "10000");
        Config.set("PUBLISH_CONFIRM_TIMEOUT_MS", "5000");
        Config.set("SHUTDOWN_DRAIN_TIMEOUT_MS", "2000");
        if (System.getenv("PROTOBUS_TEST_LOG") == null) Logger.setLevel(LogLevel.SILENT);
        else Logger.setLevel(LogLevel.DEBUG);
        broker = new MemoryBroker();
        ctx = newContext();
    }

    @AfterEach
    void tearDownBus() {
        for (MessageService s : services) {
            try {
                s.close();
            } catch (RuntimeException ignored) {
                // Some tests close their own.
            }
        }
        for (Context c : contexts) c.close();
        broker.close();
        Config.reset();
        Logger.setLevel(LogLevel.INFO);
    }

    static Connection.ReconnectionOptions fastReconnect() {
        return Connection.ReconnectionOptions.defaults().withInitialDelayMs(10).withMaxDelayMs(50).withMaxRetries(50);
    }

    Context newContext() {
        return newContext(ContextOptions.DEFAULT.withReconnection(fastReconnect()));
    }

    Context newContext(ContextOptions options) {
        Context c = new Context(options.withTransport(broker));
        c.init("amqp://guest:guest@memory/");
        contexts.add(c);
        return c;
    }

    CalcService serve() {
        return serve(MessageServiceOptions.DEFAULT);
    }

    CalcService serve(MessageServiceOptions options) {
        return serve(CalcService::new, options, ctx);
    }

    <T extends MessageService> T serve(BiFunction<Context, MessageServiceOptions, T> make,
                                       MessageServiceOptions options, Context on) {
        T s = make.apply(on, options);
        s.init();
        services.add(s);
        return s;
    }

    CalcProtobus.Proxy proxy() {
        return proxy(CalcProtobus.SERVICE_NAME);
    }

    CalcProtobus.Proxy proxy(String name) {
        CalcProtobus.Proxy p = new CalcProtobus.Proxy(ctx, name);
        p.init();
        return p;
    }

    static boolean eventually(BooleanSupplier predicate) {
        return MemoryBroker.waitFor(predicate, Duration.ofSeconds(5));
    }

    static boolean eventually(BooleanSupplier predicate, Duration timeout) {
        return MemoryBroker.waitFor(predicate, timeout);
    }

    /** A header's value as text, or "" when absent. */
    static String header(Delivery d, String name) {
        if (d.properties() == null || d.properties().getHeaders() == null) return "";
        String v = Headers.text(d.properties().getHeaders().get(name));
        return v == null ? "" : v;
    }
}
