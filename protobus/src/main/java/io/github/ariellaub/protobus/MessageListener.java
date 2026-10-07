package io.github.ariellaub.protobus;

import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.AmqpException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A service's request queue, with its retry queue, retry exchange and DLQ. */
public class MessageListener extends BaseListener {
    protected final RetryOptions retryConfig;
    private volatile String dlqName = "";
    private volatile String retryQueueName = "";
    /**
     * The topic exchange retries are published to. The retry queue is bound to it
     * with {@code #}, so a message keeps its {@code REQUEST.<service>.<method>}
     * routing key; when the retry queue's TTL expires it dead-letters back to the
     * bus exchange under that key, which is what makes the service queue's binding
     * match on redelivery.
     */
    private volatile String retryExchangeName = "";

    public MessageListener(Connection connection, boolean lateAck, Integer maxConcurrent, RetryOptions retry,
                           Long processingTimeoutMs, Integer maxPriority) {
        super(connection);
        // Validated here, so a bad value fails before any broker I/O.
        this.maxPriority = Priority.validateMaxPriority(maxPriority);
        // Priority can only reorder messages still in the queue, and an early-ack
        // consumer has no prefetch: the broker hands it the whole backlog. Refused,
        // because the failure would otherwise be invisible.
        if (this.maxPriority != null && !lateAck) {
            throw new InvalidPriorityError("maxPriority requires lateAck. With lateAck off the consumer acks on "
                    + "delivery, RabbitMQ applies no prefetch, and the broker hands it the entire backlog, leaving "
                    + "priority nothing to reorder. Enable lateAck, or drop maxPriority.");
        }
        exchangeName = Config.busExchangeName();
        exchangeType = "topic";
        this.lateAck = lateAck;
        this.maxConcurrent = maxConcurrent == null || maxConcurrent <= 0 ? 1 : maxConcurrent;
        this.processingTimeoutMs = processingTimeoutMs;
        this.retryConfig = retry == null ? RetryOptions.defaults() : retry;
        this.messageTtlMs = this.retryConfig.messageTtlMs();
    }

    /** Declare the DLQ, the retry queue and the retry exchange, after the main queue exists. */
    protected void setupRetryQueues() {
        if (retryConfig.maxRetries() <= 0 || isAnonymous()) return;
        AmqpChannel ch = channel();
        String service = configuredQueueName();
        String dlq = service + ".DLQ";
        connection.declareQueue(ch, dlq, true, false, false, Map.of());
        String retryQueue = service + ".Retry";
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("x-message-ttl", retryConfig.retryDelayMs());
        // No x-dead-letter-routing-key: the message's own routing key is kept.
        args.put("x-dead-letter-exchange", exchangeName);
        try {
            connection.declareQueue(ch, retryQueue, true, false, false, args);
        } catch (AmqpException e) {
            if (e.preconditionFailed()) {
                throw new RetryQueueMismatchError("retry queue '" + retryQueue + "' already exists with different "
                        + "arguments (most likely a different retryDelayMs, now " + retryConfig.retryDelayMs()
                        + "ms). RabbitMQ cannot change a queue's x-message-ttl in place: drain and delete the queue, "
                        + "or keep the original retryDelayMs. Original error: " + e.getMessage());
            }
            throw e;
        }
        String retryExchange = service + ".Retry.Exchange";
        connection.declareExchange(ch, retryExchange, "topic");
        connection.bindQueue(ch, retryQueue, retryExchange, "#");
        dlqName = dlq;
        retryQueueName = retryQueue;
        retryExchangeName = retryExchange;
    }

    @Override
    protected void restoreTopology() {
        if (!retryQueueName.isEmpty()) setupRetryQueues();
    }

    public String getRetryQueueName() {
        return retryQueueName;
    }

    public String getDlqName() {
        return dlqName;
    }

    public RetryOptions getRetryConfig() {
        return retryConfig;
    }

    @Override
    protected Connection.ConsumeRetryOptions getRetryOptions() {
        if (retryConfig.maxRetries() <= 0 || retryQueueName.isEmpty() || dlqName.isEmpty()) return null;
        return new Connection.ConsumeRetryOptions(retryConfig.maxRetries(), retryQueueName, retryExchangeName,
                dlqName, Errors::isHandledError);
    }

    /** Bind the queue to these topics, then declare the retry topology. */
    public void subscribe(List<String> topics) {
        for (String topic : topics) bind(topic);
        setupRetryQueues();
    }

    public void subscribe(String topic) {
        subscribe(List.of(topic));
    }
}
