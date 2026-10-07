package io.github.ariellaub.protobus;

/** Owns an {@link AbortSignal} and fires it. */
public final class AbortController {
    private final AbortSignal signal = new AbortSignal();

    public AbortSignal signal() {
        return signal;
    }

    /** Fire the signal. Idempotent. */
    public void abort() {
        signal.fire();
    }
}
