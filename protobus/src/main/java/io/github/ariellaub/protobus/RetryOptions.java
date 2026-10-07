package io.github.ariellaub.protobus;

/**
 * Retry for a service's requests: a failed request is parked on
 * {@code <Service>.Retry} for {@code retryDelayMs}, redelivered, and dead-lettered
 * to {@code <Service>.DLQ} once {@code maxRetries} hops are spent.
 *
 * @param maxRetries retry hops before the DLQ; 0 disables retry (default 3)
 * @param retryDelayMs the delay between hops, as the retry queue's
 *     {@code x-message-ttl} (default 5000). RabbitMQ fixes it at declare time, so
 *     changing it for an existing service means deleting the retry queue.
 * @param messageTtlMs a TTL for the service's own queue, or null for none
 */
public record RetryOptions(int maxRetries, long retryDelayMs, Long messageTtlMs) {
    public static RetryOptions defaults() {
        return new RetryOptions(3, 5000, null);
    }

    public RetryOptions withMaxRetries(int value) {
        return new RetryOptions(value, retryDelayMs, messageTtlMs);
    }

    public RetryOptions withRetryDelayMs(long value) {
        return new RetryOptions(maxRetries, value, messageTtlMs);
    }

    public RetryOptions withMessageTtlMs(Long value) {
        return new RetryOptions(maxRetries, retryDelayMs, value);
    }
}
