package io.github.ariellaub.protobus;

import io.github.ariellaub.protobus.amqp.AmqpChannel;
import java.util.Map;

/**
 * Hears stream-cancellation notices and stops the matching in-flight stream in
 * this process.
 *
 * Deliberately not a BaseListener: it needs its own channel and no prefetch
 * bound, because the point is to be heard while the service is busy inside a
 * streaming handler. The queue is exclusive and auto-delete, so each replica has
 * its own; cancels are consumed without acks, since a lost cancel means the stream
 * runs on, the same outcome as never having sent one.
 */
public class CancelListener {
    private final Connection connection;
    private final Object lock = new Object();
    private AmqpChannel channel;
    private String queueName = "";
    private String consumerTag = "";
    private boolean started;
    private final Runnable detachRestorer;

    public CancelListener(Connection connection) {
        this.connection = connection;
        this.detachRestorer = connection.registerRestorer(gen -> restore());
    }

    /**
     * Best effort by design: a deployment whose credentials cannot declare the
     * cancel exchange keeps working without cancellation rather than failing to
     * start.
     */
    public void start() {
        try {
            doStart();
            synchronized (lock) {
                started = true;
            }
        } catch (RuntimeException e) {
            Logger.warn("CancelListener: stream cancellation unavailable (" + e.getMessage()
                    + "). Streams will run to completion; everything else is unaffected.");
            synchronized (lock) {
                started = true;
                channel = null;
            }
        }
    }

    private void doStart() {
        AmqpChannel ch = connection.openChannel();
        connection.declareExchange(ch, Config.cancelExchangeName(), "fanout");
        String queue = connection.declareQueue(ch, "", false, true, true, Map.of());
        connection.bindQueue(ch, queue, Config.cancelExchangeName(), "");
        String tag = ch.consume(queue, "", true, true, delivery -> {
            String correlationId = delivery.properties() == null ? null : delivery.properties().getCorrelationId();
            if (correlationId == null || correlationId.isEmpty()) return;
            // Every replica hears every cancel; only the one running that stream acts.
            connection.cancelStream(correlationId);
        }, null);
        synchronized (lock) {
            channel = ch;
            queueName = queue;
            consumerTag = tag;
        }
        Logger.debug("CancelListener: consuming cancellations on " + queue);
    }

    /** Never throws: cancellation is the one thing a deployment can run without. */
    private void restore() {
        synchronized (lock) {
            if (!started) return;
        }
        start();
        Logger.debug("CancelListener: re-established after reconnection");
    }

    public void close() {
        detachRestorer.run();
        AmqpChannel ch;
        String tag;
        synchronized (lock) {
            ch = channel;
            tag = consumerTag;
            channel = null;
            consumerTag = "";
            queueName = "";
            started = false;
        }
        if (ch != null && connection.isConnected()) {
            try {
                if (!tag.isEmpty()) connection.cancel(ch, tag);
                connection.closeChannel(ch);
            } catch (RuntimeException e) {
                Logger.debug("CancelListener: error during close: " + e.getMessage());
            }
        }
    }

    String queueName() {
        synchronized (lock) {
            return queueName;
        }
    }
}
