package io.github.ariellaub.protobus.amqp;

import java.util.function.Consumer;

/** One broker connection. */
public interface AmqpConnection {
    AmqpChannel openChannel();

    /** A graceful close. The close listener then reports no error. */
    void close();

    boolean isOpen();

    /**
     * Called once when the connection closes: with null after {@link #close()},
     * with the reason when it was lost. At once if it already has.
     */
    void onClose(Consumer<String> listener);
}
