package io.github.ariellaub.protobus;

/**
 * A handler exceeded its processing timeout. The attempt is failed and retried like any
 * unhandled error; the caller is answered with this code once the retries are spent.
 *
 * The handler is not interrupted: its {@link AbortSignal} fires, and a handler doing long
 * work should watch it.
 */
public class TimeoutError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public TimeoutError(String message) {
        super(message, "PROCESSING_TIMEOUT");
    }
}
