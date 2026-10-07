package io.github.ariellaub.protobus;

/**
 * Opt-in retry for event handlers. Left at the default (no retries), a handler
 * that throws loses its event: rejecting the delivery is what stops one
 * permanently-failing event from stalling the subscriber behind its own prefetch.
 * Enabled, events climb the ladder requests do, and a retried event re-runs every
 * handler that matched it, including those that already succeeded.
 *
 * @param maxRetries retry hops before {@code <queue>.DLQ}; 0 keeps drop-on-failure
 * @param retryDelayMs the delay between hops (default 5000)
 */
public record EventRetryOptions(int maxRetries, long retryDelayMs) {
    public static EventRetryOptions none() {
        return new EventRetryOptions(0, 5000);
    }

    public static EventRetryOptions of(int maxRetries) {
        return new EventRetryOptions(maxRetries, 5000);
    }

    public EventRetryOptions withRetryDelayMs(long value) {
        return new EventRetryOptions(maxRetries, value);
    }
}
