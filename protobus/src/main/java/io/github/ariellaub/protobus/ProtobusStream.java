package io.github.ariellaub.protobus;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * The caller's side of a server stream: an iterator over its decoded chunks.
 *
 * <pre>{@code
 * try (ProtobusStream<Tick> ticks = counter.tick(request)) {
 *     for (Tick tick : ticks) { ... }
 * }
 * }</pre>
 *
 * {@code hasNext()} blocks until a chunk arrives, the stream ends, or it fails,
 * raising the failure: the service's error as a {@link RemoteError}, or a
 * {@link StreamTimeoutError}, {@link StreamBackpressureError},
 * {@link StreamSequenceError} or {@link DisconnectedError}.
 *
 * Closing it before the end cancels the call: the producer's signal fires and the
 * framework stops publishing what it writes. Use try-with-resources, so leaving
 * the loop early cancels at once rather than when the idle timeout notices.
 */
public final class ProtobusStream<T> implements Iterator<T>, Iterable<T>, AutoCloseable {
    private final MessageDispatcher.ChunkStream chunks;
    private final Function<byte[], T> decode;
    private RuntimeException failure;
    private T next;
    private boolean done;

    /** @param decode turns a chunk into a value; null skips the chunk */
    public ProtobusStream(MessageDispatcher.ChunkStream chunks, Function<byte[], T> decode) {
        this.chunks = chunks;
        this.decode = decode;
    }

    /** A stream whose first {@code hasNext()} throws {@code error}. */
    public static <T> ProtobusStream<T> failed(RuntimeException error) {
        ProtobusStream<T> s = new ProtobusStream<>(null, null);
        s.failure = error;
        return s;
    }

    @Override
    public boolean hasNext() {
        if (next != null) return true;
        if (failure != null) {
            RuntimeException f = failure;
            failure = null;
            done = true;
            throw f;
        }
        if (done) return false;
        try {
            for (byte[] chunk = chunks.next(); chunk != null; chunk = chunks.next()) {
                T value = decode.apply(chunk);
                if (value != null) {
                    next = value;
                    return true;
                }
            }
        } catch (RuntimeException e) {
            done = true;
            // A failure detected here (a chunk that does not decode) leaves the
            // producer running: tell it to stop.
            chunks.cancel();
            throw e;
        }
        done = true;
        return false;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T value = next;
        next = null;
        return value;
    }

    /** The stream itself, so it can be used in a for-each loop. Iterable once. */
    @Override
    public Iterator<T> iterator() {
        return this;
    }

    /** Cancel the call when it has not ended. Idempotent. */
    public void cancel() {
        done = true;
        if (chunks != null) chunks.cancel();
    }

    @Override
    public void close() {
        cancel();
    }
}
