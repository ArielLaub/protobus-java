package io.github.ariellaub.protobus.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.ariellaub.protobus.Logger;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;

/**
 * The production transport, over the RabbitMQ Java client.
 *
 * Supports {@code amqp://} and {@code amqps://} URLs with the RabbitMQ URI query
 * parameters {@code heartbeat}, {@code connection_timeout}, {@code channel_max}
 * and {@code verify}. An {@code amqps://} connection verifies the broker's
 * certificate and host name against the JVM's default trust store (configure it
 * with the standard {@code javax.net.ssl.trustStore} properties); only
 * {@code verify=verify_none} turns that off, for development.
 *
 * The client's own automatic recovery is disabled: protobus reconnects and
 * restores its topology itself, the same way in every port.
 */
public final class RabbitTransport implements Transport {
    /** Header that tells two pending mandatory publishes sharing a messageId apart on a return. */
    public static final String PUBLISH_TAG_HEADER = "x-protobus-publish-tag";

    @Override
    public AmqpConnection connect(String url, int heartbeatSeconds) {
        ConnectionFactory factory = configure(url, heartbeatSeconds);
        try {
            return new RabbitConnection(factory.newConnection());
        } catch (IOException | TimeoutException e) {
            throw wrap("connect", e);
        }
    }

    /** The client's factory for a broker URL: credentials, vhost, TLS and the URL's query parameters. */
    static ConnectionFactory configure(String url, int heartbeatSeconds) {
        ConnectionFactory factory = new ConnectionFactory();
        int q = url.indexOf('?');
        String base = q < 0 ? url : url.substring(0, q);
        Map<String, String> query = q < 0 ? Map.of() : parseQuery(url.substring(q + 1));
        try {
            factory.setUri(base);
        } catch (Exception e) {
            // Never the message: URISyntaxException quotes the whole input,
            // password included.
            String why = e instanceof java.net.URISyntaxException
                    ? ((java.net.URISyntaxException) e).getReason() + " at index "
                            + ((java.net.URISyntaxException) e).getIndex()
                    : e.getClass().getSimpleName();
            throw new AmqpException("invalid broker URL " + Logger.redactUrl(url) + ": " + why, 0);
        }
        // The AMQP URI spec reads "amqp://host/" as the empty vhost. Every other
        // protobus port reaches the default vhost with it, so this one does too.
        if (factory.getVirtualHost().isEmpty()) factory.setVirtualHost("/");
        if (base.regionMatches(true, 0, "amqps:", 0, 6)) {
            if ("verify_none".equalsIgnoreCase(query.get("verify"))) {
                Logger.warn("amqps with verify=verify_none: the broker's certificate is NOT verified");
                // setUri already installed the client's trust-everything context.
            } else {
                try {
                    factory.useSslProtocol(SSLContext.getDefault());
                    factory.enableHostnameVerification();
                } catch (NoSuchAlgorithmException e) {
                    throw new AmqpException("no TLS support in this JVM: " + e.getMessage(), 0, e);
                }
            }
        }
        factory.setRequestedHeartbeat(intParam(query, "heartbeat", heartbeatSeconds));
        if (query.containsKey("connection_timeout")) {
            factory.setConnectionTimeout(intParam(query, "connection_timeout", 60000));
        }
        if (query.containsKey("channel_max")) factory.setRequestedChannelMax(intParam(query, "channel_max", 0));
        factory.setAutomaticRecoveryEnabled(false);
        factory.setTopologyRecoveryEnabled(false);
        Map<String, Object> client = new HashMap<>(factory.getClientProperties());
        client.put("connection_name", "protobus-java");
        factory.setClientProperties(client);
        return factory;
    }

    private static Map<String, String> parseQuery(String query) {
        Map<String, String> out = new HashMap<>();
        for (String part : query.split("&")) {
            if (part.isEmpty()) continue;
            int eq = part.indexOf('=');
            String k = URLDecoder.decode(eq < 0 ? part : part.substring(0, eq), StandardCharsets.UTF_8);
            String v = eq < 0 ? "" : URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
            out.put(k, v);
        }
        return out;
    }

    private static int intParam(Map<String, String> query, String key, int fallback) {
        String v = query.get(key);
        if (v == null) return fallback;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new AmqpException("invalid " + key + " in broker URL: " + v, 0);
        }
    }

    static AmqpException wrap(String what, Exception e) {
        ShutdownSignalException sse = null;
        if (e instanceof ShutdownSignalException) sse = (ShutdownSignalException) e;
        else if (e.getCause() instanceof ShutdownSignalException) sse = (ShutdownSignalException) e.getCause();
        if (sse != null) {
            Method reason = sse.getReason();
            int code = 0;
            String text = sse.getMessage();
            if (reason instanceof AMQP.Channel.Close) {
                code = ((AMQP.Channel.Close) reason).getReplyCode();
                text = ((AMQP.Channel.Close) reason).getReplyText();
            } else if (reason instanceof AMQP.Connection.Close) {
                code = ((AMQP.Connection.Close) reason).getReplyCode();
                text = ((AMQP.Connection.Close) reason).getReplyText();
            }
            return new AmqpException(what + " failed: " + code + " " + text, code, e);
        }
        String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return new AmqpException(what + " failed: " + message, 0, e);
    }

    /** A listener that runs at most once, however many times the close is reported. */
    static Consumer<String> once(Consumer<String> listener) {
        java.util.concurrent.atomic.AtomicBoolean fired = new java.util.concurrent.atomic.AtomicBoolean();
        return reason -> {
            if (fired.compareAndSet(false, true)) listener.accept(reason);
        };
    }

    static String describe(ShutdownSignalException cause) {
        Method reason = cause.getReason();
        if (reason instanceof AMQP.Channel.Close) {
            AMQP.Channel.Close c = (AMQP.Channel.Close) reason;
            return c.getReplyCode() + " " + c.getReplyText();
        }
        if (reason instanceof AMQP.Connection.Close) {
            AMQP.Connection.Close c = (AMQP.Connection.Close) reason;
            return c.getReplyCode() + " " + c.getReplyText();
        }
        return cause.getMessage();
    }

    static final class RabbitConnection implements AmqpConnection {
        private final Connection connection;

        RabbitConnection(Connection connection) {
            this.connection = connection;
        }

        @Override
        public AmqpChannel openChannel() {
            try {
                Channel ch = connection.createChannel();
                if (ch == null) throw new AmqpException("no channel available (channel_max reached)", 0);
                ch.confirmSelect();
                return new RabbitChannel(ch);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("open channel", e);
            }
        }

        @Override
        public void close() {
            try {
                if (connection.isOpen()) connection.close();
            } catch (IOException | AlreadyClosedException e) {
                Logger.debug("closing the connection: " + e.getMessage());
            }
        }

        @Override
        public boolean isOpen() {
            return connection.isOpen();
        }

        @Override
        public void onClose(Consumer<String> listener) {
            Consumer<String> once = once(listener);
            connection.addShutdownListener(cause ->
                    once.accept(cause.isInitiatedByApplication() ? null : describe(cause)));
            if (!connection.isOpen()) {
                ShutdownSignalException cause = connection.getCloseReason();
                once.accept(cause == null || cause.isInitiatedByApplication() ? null : describe(cause));
            }
        }
    }

    static final class RabbitChannel implements AmqpChannel {
        private final Channel ch;
        private final Object publishLock = new Object();
        private final NavigableMap<Long, Pending> pending = new ConcurrentSkipListMap<>();

        private static final class Pending {
            final String messageId;
            final String tag;
            final BiConsumer<ConfirmOutcome, String> onConfirm;
            volatile boolean returned;

            Pending(String messageId, String tag, BiConsumer<ConfirmOutcome, String> onConfirm) {
                this.messageId = messageId;
                this.tag = tag;
                this.onConfirm = onConfirm;
            }
        }

        RabbitChannel(Channel ch) {
            this.ch = ch;
            ch.addConfirmListener((seq, multiple) -> settle(seq, multiple, false),
                    (seq, multiple) -> settle(seq, multiple, true));
            ch.addReturnListener(r -> {
                String id = r.getProperties().getMessageId();
                Object tagHeader = r.getProperties().getHeaders() == null ? null
                        : r.getProperties().getHeaders().get(PUBLISH_TAG_HEADER);
                String tag = tagHeader == null ? null : tagHeader.toString();
                // basic.return precedes the confirm of the same message, so the
                // matching publish is still pending here.
                for (Pending p : pending.values()) {
                    if (!p.returned && java.util.Objects.equals(p.messageId, id)
                            && java.util.Objects.equals(p.tag, tag)) {
                        p.returned = true;
                        return;
                    }
                }
            });
            ch.addShutdownListener(cause -> {
                String reason = describe(cause);
                List<Pending> left = new ArrayList<>(pending.values());
                pending.clear();
                for (Pending p : left) confirm(p, ConfirmOutcome.CLOSED, reason);
            });
        }

        private void settle(long seq, boolean multiple, boolean nack) {
            List<Pending> done = new ArrayList<>();
            if (multiple) {
                NavigableMap<Long, Pending> head = pending.headMap(seq, true);
                done.addAll(head.values());
                head.clear();
            } else {
                Pending p = pending.remove(seq);
                if (p != null) done.add(p);
            }
            for (Pending p : done) {
                ConfirmOutcome outcome = nack ? ConfirmOutcome.NACK
                        : p.returned ? ConfirmOutcome.RETURNED : ConfirmOutcome.ACK;
                confirm(p, outcome, nack ? "basic.nack" : "");
            }
        }

        private static void confirm(Pending p, ConfirmOutcome outcome, String detail) {
            try {
                p.onConfirm.accept(outcome, detail);
            } catch (RuntimeException e) {
                Logger.error("publish confirm callback failed: " + e);
            }
        }

        @Override
        public void declareExchange(String name, String type, boolean durable, boolean autoDelete, boolean internal,
                                    Map<String, Object> arguments) {
            try {
                ch.exchangeDeclare(name, type, durable, autoDelete, internal, arguments);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("exchange.declare " + name, e);
            }
        }

        @Override
        public String declareQueue(String name, boolean durable, boolean exclusive, boolean autoDelete,
                                   Map<String, Object> arguments) {
            try {
                return ch.queueDeclare(name, durable, exclusive, autoDelete, arguments).getQueue();
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("queue.declare " + name, e);
            }
        }

        @Override
        public void bindQueue(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
            try {
                ch.queueBind(queue, exchange, routingKey, arguments);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("queue.bind " + queue, e);
            }
        }

        @Override
        public void unbindQueue(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
            try {
                ch.queueUnbind(queue, exchange, routingKey, arguments);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("queue.unbind " + queue, e);
            }
        }

        @Override
        public void deleteQueue(String name) {
            try {
                ch.queueDelete(name);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("queue.delete " + name, e);
            }
        }

        @Override
        public void purgeQueue(String name) {
            try {
                ch.queuePurge(name);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("queue.purge " + name, e);
            }
        }

        @Override
        public void prefetch(int count) {
            try {
                ch.basicQos(count, false);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("basic.qos", e);
            }
        }

        @Override
        public String consume(String queue, String consumerTag, boolean noAck, boolean exclusive,
                              Consumer<Delivery> onDelivery, Runnable onCancel) {
            try {
                return ch.basicConsume(queue, noAck, consumerTag, false, exclusive, null, new DefaultConsumer(ch) {
                    @Override
                    public void handleDelivery(String tag, Envelope env, BasicProperties props, byte[] body) {
                        try {
                            onDelivery.accept(new Delivery(body, props, env.getExchange(), env.getRoutingKey(), tag,
                                    env.getDeliveryTag(), env.isRedeliver()));
                        } catch (RuntimeException e) {
                            // The client closes the channel on an exception escaping
                            // a consumer; one bad delivery must not do that.
                            Logger.error("delivery callback failed on " + queue + ": " + e);
                        }
                    }

                    @Override
                    public void handleCancel(String tag) {
                        if (onCancel != null) onCancel.run();
                    }
                });
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("basic.consume " + queue, e);
            }
        }

        @Override
        public void cancel(String consumerTag) {
            try {
                ch.basicCancel(consumerTag);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("basic.cancel", e);
            }
        }

        @Override
        public void ack(long deliveryTag) {
            try {
                ch.basicAck(deliveryTag, false);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("basic.ack", e);
            }
        }

        @Override
        public void reject(long deliveryTag, boolean requeue) {
            try {
                ch.basicReject(deliveryTag, requeue);
            } catch (IOException | AlreadyClosedException e) {
                throw wrap("basic.reject", e);
            }
        }

        @Override
        public void publish(String exchange, String routingKey, byte[] body, BasicProperties properties,
                            boolean mandatory, BiConsumer<ConfirmOutcome, String> onConfirm) {
            Map<String, Object> headers = properties.getHeaders();
            Object tag = headers == null ? null : headers.get(PUBLISH_TAG_HEADER);
            synchronized (publishLock) {
                long seq = ch.getNextPublishSeqNo();
                pending.put(seq, new Pending(properties.getMessageId(), tag == null ? null : tag.toString(),
                        onConfirm));
                try {
                    ch.basicPublish(exchange, routingKey, mandatory, properties, body);
                } catch (IOException | AlreadyClosedException e) {
                    pending.remove(seq);
                    throw wrap("basic.publish", e);
                }
            }
        }

        @Override
        public void close() {
            try {
                if (ch.isOpen()) ch.close();
            } catch (IOException | TimeoutException | AlreadyClosedException e) {
                Logger.debug("closing a channel: " + e.getMessage());
            }
        }

        @Override
        public boolean isOpen() {
            return ch.isOpen();
        }

        @Override
        public void onClose(Consumer<String> listener) {
            Consumer<String> once = once(listener);
            ch.addShutdownListener(cause -> once.accept(describe(cause)));
            if (!ch.isOpen()) {
                ShutdownSignalException cause = ch.getCloseReason();
                once.accept(cause == null ? "closed" : describe(cause));
            }
        }
    }
}
