package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.ConfirmOutcome;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PublishTest extends MemoryBus {
    private AmqpChannel queueChannel(String queue) {
        AmqpChannel ch = ctx.connection().openChannel();
        ctx.connection().declareQueue(ch, queue, true, false, false, Map.of());
        return ch;
    }

    @Test
    void aConfirmedPublishReturnsItsMessageId() {
        AmqpChannel ch = queueChannel("q");
        String id = ctx.connection().publish(ch, "", "q", new byte[] {1}, Connection.PublishOptions.of(null));
        assertEquals(36, id.length());
        assertEquals(id, broker.peek("q").get(0).properties().getMessageId());
    }

    @Test
    void aNackIsADefiniteFailure() {
        AmqpChannel ch = queueChannel("q");
        broker.setConfirmMode(MemoryBroker.ConfirmMode.NACK);
        assertThrows(PublishNackedError.class,
                () -> ctx.connection().publish(ch, "", "q", new byte[0], Connection.PublishOptions.of(null)));
    }

    @Test
    void aMissingConfirmIsAnAmbiguousTimeout() {
        Config.set("PUBLISH_CONFIRM_TIMEOUT_MS", "100");
        AmqpChannel ch = queueChannel("q");
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        PublishConfirmTimeoutError e = assertThrows(PublishConfirmTimeoutError.class,
                () -> ctx.connection().publish(ch, "", "q", new byte[0], Connection.PublishOptions.of(
                        new com.rabbitmq.client.AMQP.BasicProperties.Builder().messageId("m1").build())));
        assertEquals("m1", e.messageId());
        // The broker stored it all the same: the outcome was unknown, not failed.
        assertEquals(1, broker.queueDepth("q"));
    }

    @Test
    void aChannelClosingUnderAPendingConfirmIsAmbiguous() {
        AmqpChannel ch = queueChannel("q");
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        CompletableFuture<String> f = ctx.connection().publishAsync(ch, "", "q", new byte[0],
                Connection.PublishOptions.of(null));
        broker.closeChannelsConsuming("q", "x");
        ch.close();
        Exception e = assertThrows(Exception.class, f::join);
        assertTrue(e.getCause() instanceof ChannelClosedError, String.valueOf(e.getCause()));
    }

    @Test
    void unconfirmedPublishesAreBoundedPerChannel() throws Exception {
        Config.set("MAX_OUTSTANDING_CONFIRMS", "2");
        AmqpChannel ch = queueChannel("q");
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        List<CompletableFuture<String>> fs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            fs.add(ctx.connection().publishAsync(ch, "", "q", new byte[0], Connection.PublishOptions.of(null)));
        }
        broker.flush();
        assertEquals(2, broker.heldConfirms());
        broker.setConfirmMode(MemoryBroker.ConfirmMode.ACK);
        broker.releaseHeldConfirms(ConfirmOutcome.ACK);
        for (CompletableFuture<String> f : fs) f.get();
        assertEquals(5, broker.queueDepth("q"));
    }

    @Test
    void aMandatoryPublishToNothingIsUnroutable() {
        AmqpChannel ch = ctx.connection().openChannel();
        ctx.connection().declareExchange(ch, "x.topic", "topic");
        assertThrows(UnroutableError.class, () -> ctx.connection().publish(ch, "x.topic", "nowhere", new byte[0],
                new Connection.PublishOptions(null, true)));
        // Not mandatory: dropped by the broker, and fine.
        ctx.connection().publish(ch, "x.topic", "nowhere", new byte[0], Connection.PublishOptions.of(null));
    }

    @Test
    void returnsAreToldApartWhenPublishesShareAMessageId() throws Exception {
        AmqpChannel ch = queueChannel("q");
        ctx.connection().declareExchange(ch, "x.topic", "topic");
        ctx.connection().bindQueue(ch, "q", "x.topic", "routed");
        com.rabbitmq.client.AMQP.BasicProperties props =
                new com.rabbitmq.client.AMQP.BasicProperties.Builder().messageId("same").build();
        broker.setConfirmMode(MemoryBroker.ConfirmMode.DROP);
        CompletableFuture<String> routed = ctx.connection().publishAsync(ch, "x.topic", "routed", new byte[0],
                new Connection.PublishOptions(props, true));
        CompletableFuture<String> unroutable = ctx.connection().publishAsync(ch, "x.topic", "nowhere", new byte[0],
                new Connection.PublishOptions(props, true));
        broker.flush();
        assertEquals(2, broker.heldConfirms());
        broker.releaseHeldConfirms(ConfirmOutcome.ACK);
        assertEquals("same", routed.get());
        // The memory broker reports returns directly; the tag is what RabbitMQ needs.
        assertEquals("same", unroutable.get());
        assertTrue(broker.peek("q").get(0).properties().getHeaders() == null
                || !broker.peek("q").get(0).properties().getHeaders().containsKey("x-protobus-publish-tag"));
    }

    @Test
    void aPublishOnAClosedChannelFailsAtOnce() {
        AmqpChannel ch = queueChannel("q");
        ch.close();
        assertThrows(ChannelClosedError.class,
                () -> ctx.connection().publish(ch, "", "q", new byte[0], Connection.PublishOptions.of(null)));
    }

    @Test
    void retryAndDeadLetterCopiesKeepTheMessagesProperties() {
        CalcService s = serve(MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withMaxRetries(1)
                .withRetryDelayMs(10)));
        assertThrows(RemoteError.class, () -> proxy().fail(pbtest.FailRequest.newBuilder().setId("p").build(),
                CallOptions.DEFAULT.withPriority(1)));
        assertTrue(eventually(() -> broker.queueDepth("pbtest.Calc.DLQ") == 1));
        com.rabbitmq.client.AMQP.BasicProperties dead = broker.peek("pbtest.Calc.DLQ").get(0).properties();
        assertEquals(1, dead.getPriority());
        assertEquals("application/octet-stream", dead.getContentType());
        assertEquals(2, dead.getDeliveryMode());
        assertEquals(null, dead.getReplyTo());
        assertEquals(2, s.failAttempts.get());
    }
}
