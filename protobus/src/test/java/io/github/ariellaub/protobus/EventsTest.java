package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import pbtest.Ping;
import pbtest.Pong;

class EventsTest extends MemoryBus {
    static Ping ping(String id) {
        return Ping.newBuilder().setId(id).setN(CustomTypes.bigint(7)).build();
    }

    @Test
    void aTypedSubscriberReceivesEventsOnTheDefaultTopic() {
        CalcService s = serve();
        List<String> got = new CopyOnWriteArrayList<>();
        List<String> topics = new CopyOnWriteArrayList<>();
        s.subscribeEvent(Ping.class, (e, type, topic) -> {
            got.add(e.getId());
            topics.add(type + "@" + topic);
        });
        ctx.publishEvent(ping("a"));
        assertTrue(eventually(() -> got.size() == 1));
        assertEquals("a", got.get(0));
        assertEquals("pbtest.Ping@EVENT.pbtest.Ping", topics.get(0));
    }

    @Test
    void eventsFanOutToEverySubscribingService() {
        Context other = newContext();
        CalcService a = serve();
        CalcService b = serve((c, o) -> new CalcService.Instance(c, o, "pbtest.Calc.two"),
                MessageServiceOptions.DEFAULT, other);
        AtomicInteger count = new AtomicInteger();
        a.subscribeEvent(Ping.class, (e, t, p) -> count.incrementAndGet());
        b.subscribeEvent(Ping.class, (e, t, p) -> count.incrementAndGet());
        ctx.publishEvent(ping("x"));
        assertTrue(eventually(() -> count.get() == 2));
    }

    @Test
    void patternsAndSubscribeAllMatchLikeTheBroker() {
        CalcService s = serve();
        List<String> star = new CopyOnWriteArrayList<>();
        List<String> all = new CopyOnWriteArrayList<>();
        s.subscribeEvent(Ping.class, (e, t, p) -> star.add(p), "billing.*");
        s.subscribeEvent("pbtest.Pong", (e, t, p) -> all.add(t), "#");
        ctx.publishEvent("pbtest.Ping", ping("1"), "billing.paid");
        ctx.publishEvent("pbtest.Ping", ping("2"), "billing.paid.late");
        ctx.publishEvent("pbtest.Pong", Pong.newBuilder().setId("3").build(), "other");
        assertTrue(eventually(() -> star.size() == 1 && all.size() == 3));
        assertEquals("billing.paid", star.get(0));
    }

    @Test
    void aHandlerOfAnotherTypeIsSkippedNotFed() {
        CalcService s = serve();
        List<Message> got = new CopyOnWriteArrayList<>();
        s.subscribeEvent(Ping.class, (e, t, p) -> got.add(e), "shared");
        s.subscribeEvent("pbtest.Pong", (e, t, p) -> got.add(e), "shared");
        ctx.publishEvent("pbtest.Pong", Pong.newBuilder().setId("p").build(), "shared");
        assertTrue(eventually(() -> got.size() == 1));
        broker.flush();
        assertEquals("pbtest.Pong", got.get(0).getDescriptorForType().getFullName());
    }

    @Test
    void aFailingHandlerDropsItsEventByDefault() {
        CalcService s = serve();
        AtomicInteger attempts = new AtomicInteger();
        s.subscribeEvent(Ping.class, (e, t, p) -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("nope");
        });
        ctx.publishEvent(ping("d"));
        assertTrue(eventually(() -> attempts.get() == 1));
        assertTrue(eventually(() -> broker.unackedCount("pbtest.Calc.Events") == 0));
        assertEquals(0, broker.queueDepth("pbtest.Calc.Events"));
        assertTrue(!broker.queueExists("pbtest.Calc.Events.DLQ"));
    }

    @Test
    void eventRetryClimbsItsOwnLadderToItsOwnDlq() {
        CalcService s = serve(MessageServiceOptions.DEFAULT
                .withEventRetry(EventRetryOptions.of(2).withRetryDelayMs(10)));
        AtomicInteger attempts = new AtomicInteger();
        s.subscribeEvent(Ping.class, (e, t, p) -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("nope");
        });
        assertNotNull(broker.queueArguments("pbtest.Calc.Events.Retry"));
        ctx.publishEvent(ping("r"));
        assertTrue(eventually(() -> broker.queueDepth("pbtest.Calc.Events.DLQ") == 1));
        assertEquals(3, attempts.get());
        assertEquals("EVENT.pbtest.Ping",
                header(broker.peek("pbtest.Calc.Events.DLQ").get(0), "x-original-routing-key"));
    }

    @Test
    void aHandledErrorFromAnEventHandlerIsDroppedEvenWithRetryOn() {
        CalcService s = serve(MessageServiceOptions.DEFAULT
                .withEventRetry(EventRetryOptions.of(2).withRetryDelayMs(10)));
        AtomicInteger attempts = new AtomicInteger();
        s.subscribeEvent(Ping.class, (e, t, p) -> {
            attempts.incrementAndGet();
            throw new HandledError("not for me");
        });
        ctx.publishEvent(ping("h"));
        assertTrue(eventually(() -> attempts.get() == 1));
        assertTrue(eventually(() -> broker.unackedCount("pbtest.Calc.Events") == 0));
        assertEquals(0, broker.queueDepth("pbtest.Calc.Events.DLQ"));
    }

    @Test
    void anEventIsRoutedByTheKeyItArrivedOnNotItsBody() {
        CalcService s = serve();
        List<String> got = new CopyOnWriteArrayList<>();
        s.subscribeEvent(Ping.class, (e, t, p) -> got.add("secret"), "admin.only");
        s.subscribeEvent(Ping.class, (e, t, p) -> got.add("public"), "public");
        // The body claims admin.only; the broker delivered it on "public".
        byte[] forged = MessageFactory.buildEvent("pbtest.Ping", ping("f"), "admin.only");
        ctx.connection().publish(ctx.connection().openChannel(), Config.eventsExchangeName(), "public", forged,
                Connection.PublishOptions.of(null));
        assertTrue(eventually(() -> got.size() == 1));
        broker.flush();
        assertEquals(List.of("public"), got);
    }

    @Test
    void retryOffDeclaresNoRetryTopology() {
        serve();
        assertNull(broker.queueArguments("pbtest.Calc.Events.Retry"));
    }
}
