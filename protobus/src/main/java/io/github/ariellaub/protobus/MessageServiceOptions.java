package io.github.ariellaub.protobus;

/** How a service consumes. Immutable; each {@code with} method returns a copy. */
public final class MessageServiceOptions {
    public static final MessageServiceOptions DEFAULT =
            new MessageServiceOptions(null, RetryOptions.defaults(), true, null, null, EventRetryOptions.none());

    private final Integer maxConcurrent;
    private final RetryOptions retry;
    private final boolean lateAck;
    private final Long processingTimeoutMs;
    private final Integer maxPriority;
    private final EventRetryOptions eventRetry;

    private MessageServiceOptions(Integer maxConcurrent, RetryOptions retry, boolean lateAck, Long processingTimeoutMs,
                                  Integer maxPriority, EventRetryOptions eventRetry) {
        this.maxConcurrent = maxConcurrent;
        this.retry = retry;
        this.lateAck = lateAck;
        this.processingTimeoutMs = processingTimeoutMs;
        this.maxPriority = maxPriority;
        this.eventRetry = eventRetry;
    }

    /** Requests handled in parallel by this process: the queue's prefetch. Default 1. */
    public Integer maxConcurrent() {
        return maxConcurrent;
    }

    public RetryOptions retry() {
        return retry;
    }

    /**
     * Ack after the handler completes (the default) rather than on delivery. Acking
     * on delivery disables retry, dead-lettering and error replies: set it false
     * only for genuine at-most-once delivery.
     */
    public boolean lateAck() {
        return lateAck;
    }

    /** Per-request processing timeout; null for {@link Config#messageProcessingTimeout()}. */
    public Long processingTimeoutMs() {
        return processingTimeoutMs;
    }

    /** Declare the request queue as a priority queue with this {@code x-max-priority}; null for a plain queue. */
    public Integer maxPriority() {
        return maxPriority;
    }

    /** Retry for this service's EVENT subscriptions; off by default. */
    public EventRetryOptions eventRetry() {
        return eventRetry;
    }

    public MessageServiceOptions withMaxConcurrent(int value) {
        return new MessageServiceOptions(value, retry, lateAck, processingTimeoutMs, maxPriority, eventRetry);
    }

    public MessageServiceOptions withRetry(RetryOptions value) {
        return new MessageServiceOptions(maxConcurrent, value, lateAck, processingTimeoutMs, maxPriority, eventRetry);
    }

    public MessageServiceOptions withLateAck(boolean value) {
        return new MessageServiceOptions(maxConcurrent, retry, value, processingTimeoutMs, maxPriority, eventRetry);
    }

    public MessageServiceOptions withProcessingTimeoutMs(long value) {
        return new MessageServiceOptions(maxConcurrent, retry, lateAck, value, maxPriority, eventRetry);
    }

    public MessageServiceOptions withMaxPriority(int value) {
        return new MessageServiceOptions(maxConcurrent, retry, lateAck, processingTimeoutMs, value, eventRetry);
    }

    public MessageServiceOptions withEventRetry(EventRetryOptions value) {
        return new MessageServiceOptions(maxConcurrent, retry, lateAck, processingTimeoutMs, maxPriority, value);
    }
}
