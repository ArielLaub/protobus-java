package io.github.ariellaub.protobus;

/** Options for a streaming call. Immutable; each {@code with} method returns a copy. */
public final class StreamOptions {
    public static final StreamOptions DEFAULT = new StreamOptions(null, null, null);

    private final String actor;
    private final Long idleTimeoutMs;
    private final AbortSignal signal;

    private StreamOptions(String actor, Long idleTimeoutMs, AbortSignal signal) {
        this.actor = actor;
        this.idleTimeoutMs = idleTimeoutMs;
        this.signal = signal;
    }

    public String actor() {
        return actor;
    }

    /** The longest gap tolerated between chunks; null for {@link Config#streamIdleTimeoutMs()}. */
    public Long idleTimeoutMs() {
        return idleTimeoutMs;
    }

    /** Cancels the stream when it fires, from anywhere. Closing the stream cancels too. */
    public AbortSignal signal() {
        return signal;
    }

    public StreamOptions withActor(String value) {
        return new StreamOptions(value, idleTimeoutMs, signal);
    }

    public StreamOptions withIdleTimeoutMs(long value) {
        return new StreamOptions(actor, value, signal);
    }

    public StreamOptions withSignal(AbortSignal value) {
        return new StreamOptions(actor, idleTimeoutMs, value);
    }
}
