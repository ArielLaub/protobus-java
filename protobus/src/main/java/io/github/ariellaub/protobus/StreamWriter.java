package io.github.ariellaub.protobus;

/**
 * Where a server-streaming handler writes its chunks.
 *
 * Each {@link #write} publishes the previous chunk and holds this one until the
 * next is written or the handler returns, so the last chunk can be marked final.
 * Returning ends the stream; throwing ends it with the error, which the caller's
 * iteration raises. Once the caller has cancelled, {@code write} throws to unwind
 * the handler, and {@link #cancelled()} reports it.
 */
public interface StreamWriter<T> {
    void write(T chunk);

    boolean cancelled();
}
