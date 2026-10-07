package io.github.ariellaub.protobus;

import com.google.protobuf.MessageOrBuilder;
import com.rabbitmq.client.AMQP.BasicProperties;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Publishes events to the events exchange. */
public class EventDispatcher {
    private final Connection connection;
    private final Object channelLock = new Object();
    private volatile AmqpChannel channel;
    private volatile boolean initialized;
    private final Runnable detachRestorer;
    private final Runnable detachDisconnected;

    public EventDispatcher(Connection connection) {
        this.connection = connection;
        this.detachDisconnected = connection.onDisconnected(() -> {
            synchronized (channelLock) {
                channel = null;
            }
        });
        this.detachRestorer = connection.registerRestorer(gen -> {
            if (!initialized) return;
            Logger.info("EventDispatcher: reconnected, re-initializing channel");
            open();
        });
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void init() {
        if (initialized) return;
        open();
        initialized = true;
    }

    private AmqpChannel open() {
        AmqpChannel ch = connection.openChannel();
        // Declared by the publisher too, so an event published before any
        // subscriber exists is not refused for want of an exchange.
        connection.declareExchange(ch, Config.eventsExchangeName(), "topic");
        synchronized (channelLock) {
            channel = ch;
        }
        return ch;
    }

    private AmqpChannel publishChannel() {
        AmqpChannel ch = channel;
        if (ch != null && ch.isOpen()) return ch;
        synchronized (channelLock) {
            ch = channel;
            if (ch != null && ch.isOpen()) return ch;
            if (!connection.isReady()) return ch;
            Logger.warn("EventDispatcher: publishing channel lost on a live connection; reopening it");
            return open();
        }
    }

    /**
     * Publish an event. {@code type} is the payload's full message name; the topic
     * defaults to {@code EVENT.<type>}. Completes once the broker has confirmed it.
     * Not mandatory: an event nobody subscribes to is normal.
     */
    public CompletableFuture<Void> publishAsync(String type, MessageOrBuilder content, String topic) {
        if (!connection.isConnected() && !connection.isReconnecting()) {
            return CompletableFuture.failedFuture(new NotConnectedError());
        }
        String t = topic == null || topic.isEmpty() ? "EVENT." + type : topic;
        byte[] event;
        try {
            event = MessageFactory.buildEvent(type, content, t);
        } catch (RuntimeException e) {
            // No payload in the line: events carry personal data too.
            Logger.error("failed building event '" + type + "': " + e.getMessage());
            return CompletableFuture.failedFuture(new InvalidMessageError("failed building event '" + type + "': "
                    + e.getMessage()));
        }
        BasicProperties props = new BasicProperties.Builder()
                .correlationId(UUID.randomUUID().toString())
                .contentType("application/octet-stream")
                .deliveryMode(2)
                .build();
        return connection.whenReadyAsync()
                .thenCompose(v -> connection.publishAsync(publishChannel(), Config.eventsExchangeName(), t, event,
                        Connection.PublishOptions.of(props)))
                .thenApply(mid -> null);
    }

    public void publish(String type, MessageOrBuilder content, String topic) {
        Connection.join(publishAsync(type, content, topic));
    }

    public void close() {
        detachDisconnected.run();
        detachRestorer.run();
        AmqpChannel ch = channel;
        if (ch != null && ch.isOpen()) ch.close();
    }
}
