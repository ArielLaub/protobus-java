package io.github.ariellaub.protobus.amqp;

import com.rabbitmq.client.AMQP.BasicProperties;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * One confirm-mode channel. Every method that waits for the broker throws
 * {@link AmqpException} on failure, and a channel-level failure closes the
 * channel, as AMQP specifies.
 */
public interface AmqpChannel {
    void declareExchange(String name, String type, boolean durable, boolean autoDelete, boolean internal,
                         Map<String, Object> arguments);

    /** Returns the queue's name: the given one, or the broker's for "". */
    String declareQueue(String name, boolean durable, boolean exclusive, boolean autoDelete,
                        Map<String, Object> arguments);

    void bindQueue(String queue, String exchange, String routingKey, Map<String, Object> arguments);

    void unbindQueue(String queue, String exchange, String routingKey, Map<String, Object> arguments);

    void deleteQueue(String name);

    void purgeQueue(String name);

    /** Per-consumer prefetch (basic.qos, global false). */
    void prefetch(int count);

    /**
     * Start a consumer. {@code onDelivery} runs on a transport thread, one delivery
     * at a time per channel; {@code onCancel} runs when the broker cancels the
     * consumer (its queue was deleted, say).
     */
    String consume(String queue, String consumerTag, boolean noAck, boolean exclusive,
                   Consumer<Delivery> onDelivery, Runnable onCancel);

    void cancel(String consumerTag);

    void ack(long deliveryTag);

    void reject(long deliveryTag, boolean requeue);

    /**
     * Publish. {@code onConfirm} is called exactly once, on a transport thread,
     * with the outcome and a detail string. Throws {@link AmqpException} only when
     * the publish could not be written at all, in which case {@code onConfirm} is
     * never called.
     */
    void publish(String exchange, String routingKey, byte[] body, BasicProperties properties, boolean mandatory,
                 BiConsumer<ConfirmOutcome, String> onConfirm);

    void close();

    boolean isOpen();

    /** Called once when the channel closes for any reason, with the reason; at once if it has. */
    void onClose(Consumer<String> listener);
}
