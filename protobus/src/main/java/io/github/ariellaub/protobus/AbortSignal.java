package io.github.ariellaub.protobus;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A one-shot cancellation signal, after the web platform's AbortSignal.
 *
 * A service handler receives one in its {@link CallContext}: it fires when the
 * processing timeout elapses or, for a stream, when the caller cancels. A caller
 * passes one in {@link StreamOptions} to cancel a stream from anywhere.
 * Cancellation is cooperative: protobus never interrupts a handler's thread, so a
 * handler doing long work should check {@link #aborted()}, or wait with
 * {@link #await(Duration)} instead of sleeping.
 */
public final class AbortSignal {
    private static final AtomicLong ids = new AtomicLong();

    private final Object lock = new Object();
    private boolean aborted;
    private final Map<Long, Runnable> listeners = new LinkedHashMap<>();

    AbortSignal() {}

    /** A signal that never fires. */
    public static AbortSignal never() {
        return new AbortSignal();
    }

    public boolean aborted() {
        synchronized (lock) {
            return aborted;
        }
    }

    /**
     * Run {@code listener} once when the signal fires, at once if it already has.
     * It runs on whichever thread aborts, so keep it short. Returns an id for
     * {@link #removeListener}.
     */
    public long addListener(Runnable listener) {
        long id = ids.incrementAndGet();
        synchronized (lock) {
            if (!aborted) {
                listeners.put(id, listener);
                return id;
            }
        }
        listener.run();
        return id;
    }

    public void removeListener(long id) {
        synchronized (lock) {
            listeners.remove(id);
        }
    }

    /**
     * Block until the signal fires or {@code timeout} elapses; returns whether it
     * has fired. Use it in place of {@code Thread.sleep} in a cancellable handler.
     */
    public boolean await(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (lock) {
            while (!aborted) {
                long left = deadline - System.nanoTime();
                if (left <= 0) return false;
                TimeUnit.NANOSECONDS.timedWait(lock, left);
            }
            return true;
        }
    }

    /** Throws {@link ProtobusException} ("aborted") when the signal has fired. */
    public void throwIfAborted() {
        if (aborted()) throw new ProtobusException("aborted", null);
    }

    void fire() {
        Runnable[] run;
        synchronized (lock) {
            if (aborted) return;
            aborted = true;
            run = listeners.values().toArray(new Runnable[0]);
            listeners.clear();
            lock.notifyAll();
        }
        for (Runnable r : run) {
            try {
                r.run();
            } catch (RuntimeException e) {
                Logger.debug("abort listener failed: " + Errors.messageOf(e));
            }
        }
    }
}
