package io.github.ariellaub.protobus;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.AmqpException;
import io.github.ariellaub.protobus.internal.Envelopes;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A service's event queue ({@code <Service>.Events}) and the handlers subscribed
 * on it, matched by topic pattern as the broker matches bindings.
 */
public class EventListener extends BaseListener {
    private final MessageFactory factory;
    private final EventRetryOptions retryConfig;
    private final Trie<Subscription<?>> router = new Trie<>();
    private final Object routerLock = new Object();
    private volatile Subscription<Message> allHandler;
    private volatile String retryQueueName = "";
    private volatile String retryExchangeName = "";
    private volatile String redeliveryExchangeName = "";
    private volatile String dlqName = "";

    /** A handler and the type it decodes events as; null decodes dynamically. */
    private static final class Subscription<T extends Message> {
        final T prototype;
        final EventHandler<T> handler;

        Subscription(T prototype, EventHandler<T> handler) {
            this.prototype = prototype;
            this.handler = handler;
        }

        @SuppressWarnings("unchecked")
        void deliver(MessageFactory factory, Envelopes.Event event) throws Exception {
            Message decoded;
            if (prototype == null) {
                // A type this process has no schema for is skipped, not failed: it
                // would fail identically on every retry.
                if (!factory.hasType(event.type())) {
                    throw new ProtocolError("no schema for event type '" + event.type() + "'");
                }
                decoded = factory.decodeMessage(event.type(), event.data());
            } else if (prototype.getDescriptorForType().getFullName().equals(event.type())) {
                try {
                    decoded = prototype.getParserForType().parseFrom(event.data());
                } catch (InvalidProtocolBufferException e) {
                    throw new ProtocolError("event of type " + event.type() + " did not decode");
                }
                CustomTypes.validate(decoded);
            } else {
                // The topic matched, but the payload is another type: decoding it as
                // this handler's type would hand it garbage.
                Logger.warn("event of type '" + event.type() + "' on " + event.topic()
                        + " skipped by a handler of " + prototype.getDescriptorForType().getFullName());
                return;
            }
            handler.handle((T) decoded, event.type(), event.topic());
        }
    }

    public EventListener(Connection connection, MessageFactory factory, EventRetryOptions retry) {
        super(connection);
        this.factory = factory;
        this.retryConfig = retry == null ? EventRetryOptions.none() : retry;
        exchangeName = Config.eventsExchangeName();
        exchangeType = "topic";
        lateAck = true;
    }

    @Override
    protected Connection.HandlerResult defaultHandler(byte[] encodedEvent, String correlationId,
                                                      Connection.MessageHandlerContext context) {
        Envelopes.Event event;
        try {
            event = MessageFactory.decodeEventEnvelope(encodedEvent);
        } catch (Envelopes.MalformedException e) {
            throw new ProtocolError("event envelope did not decode");
        }
        try {
            Subscription<Message> all = allHandler;
            if (all != null) all.deliver(factory, event);
            // Prefer the routing key the broker delivered on over the topic carried
            // in the body: the body is publisher-controlled, so trusting it would let
            // a publisher reach handlers its routing key was never permitted to reach.
            String matchTopic = context != null && context.routingKey != null && !context.routingKey.isEmpty()
                    ? context.routingKey : event.topic();
            if (matchTopic == null || matchTopic.isEmpty()) {
                // Type only: the payload is application data.
                Logger.warn("ignoring unhandled event of type '" + event.type() + "' (no topic to route on)");
                return Connection.HandlerResult.none();
            }
            List<Subscription<?>> handlers;
            synchronized (routerLock) {
                handlers = router.match(matchTopic);
            }
            for (Subscription<?> s : handlers) s.deliver(factory, event);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ProtobusException(Errors.messageOf(e), Errors.codeOf(e), e);
        }
        return Connection.HandlerResult.none();
    }

    /**
     * Declare the retry ladder. Unlike a request's, an event's retry queue cannot
     * dead-letter back to the events exchange: that would redeliver to every
     * subscriber of the topic, including those that handled it. It goes to a
     * per-subscriber exchange bound only to this listener's queue instead.
     */
    private void setupRetryTopology() {
        // An anonymous queue disappears with the connection: nowhere to come back to.
        if (retryConfig.maxRetries() <= 0 || isAnonymous()) return;
        AmqpChannel ch = channel();
        String base = configuredQueueName();
        String dlq = base + ".DLQ";
        String retryQueue = base + ".Retry";
        String retryExchange = base + ".Retry.Exchange";
        String redelivery = base + ".Redelivery";
        connection.declareQueue(ch, dlq, true, false, false, Map.of());
        connection.declareExchange(ch, redelivery, "topic");
        connection.bindQueue(ch, queueName(), redelivery, "#");
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("x-message-ttl", retryConfig.retryDelayMs());
        args.put("x-dead-letter-exchange", redelivery);
        try {
            connection.declareQueue(ch, retryQueue, true, false, false, args);
        } catch (AmqpException e) {
            if (e.preconditionFailed()) {
                throw new RetryQueueMismatchError("event retry queue '" + retryQueue + "' already exists with "
                        + "different arguments (most likely a different retryDelayMs, now "
                        + retryConfig.retryDelayMs() + "ms). RabbitMQ cannot change a queue's x-message-ttl in place: "
                        + "drain and delete the queue, or keep the original retryDelayMs. Original error: "
                        + e.getMessage());
            }
            throw e;
        }
        connection.declareExchange(ch, retryExchange, "topic");
        connection.bindQueue(ch, retryQueue, retryExchange, "#");
        dlqName = dlq;
        retryQueueName = retryQueue;
        retryExchangeName = retryExchange;
        redeliveryExchangeName = redelivery;
    }

    @Override
    protected void restoreTopology() {
        if (!retryQueueName.isEmpty()) setupRetryTopology();
    }

    @Override
    protected Connection.ConsumeRetryOptions getRetryOptions() {
        if (retryConfig.maxRetries() <= 0 || retryQueueName.isEmpty() || dlqName.isEmpty()) return null;
        return new Connection.ConsumeRetryOptions(retryConfig.maxRetries(), retryQueueName, retryExchangeName,
                dlqName, Errors::isHandledError);
    }

    @Override
    public void init(Connection.MessageHandler messageHandler, String queueName) {
        if (isInitialized()) return;
        super.init(messageHandler, queueName);
        // Before start(), so the first delivery already has somewhere to fail to.
        setupRetryTopology();
    }

    /**
     * Subscribe a typed handler. The topic defaults to {@code EVENT.<type>}; a
     * pattern ({@code EVENT.Billing.*}, {@code #}) receives every matching event of
     * this handler's type.
     */
    public <T extends Message> void subscribe(T prototype, EventHandler<T> handler, String topic) {
        String type = prototype.getDescriptorForType().getFullName();
        factory.register(prototype.getDescriptorForType().getFile());
        add(topic == null || topic.isEmpty() ? "EVENT." + type : topic, new Subscription<>(prototype, handler));
    }

    /** Subscribe a dynamic handler to events of a type known to the factory. */
    public void subscribe(String type, EventHandler<Message> handler, String topic) {
        add(topic == null || topic.isEmpty() ? "EVENT." + type : topic, new Subscription<Message>(null, handler));
    }

    private void add(String topic, Subscription<?> subscription) {
        synchronized (routerLock) {
            router.add(topic, subscription);
        }
        bind(topic);
    }

    /** Receive every event on the bus, decoded dynamically. */
    public void subscribeAll(EventHandler<Message> handler) {
        allHandler = new Subscription<>(null, handler);
        bind("#");
    }

    /** The retry objects' names, or null when event retry is off. */
    public Map<String, String> retryTopology() {
        if (retryQueueName.isEmpty()) return null;
        return Map.of("retryQueue", retryQueueName, "dlq", dlqName, "redeliveryExchange", redeliveryExchangeName);
    }
}
