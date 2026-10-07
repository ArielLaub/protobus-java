package io.github.ariellaub.protobus.amqp;

import io.github.ariellaub.protobus.ProtobusException;

/**
 * A broker-reported failure: a channel or connection exception ({@link #replyCode()}
 * 404, 406, ...) or a client-side transport failure (0).
 */
public class AmqpException extends ProtobusException {
    private static final long serialVersionUID = 1L;

    private final int replyCode;

    public AmqpException(String message, int replyCode) {
        super(message, null);
        this.replyCode = replyCode;
    }

    public AmqpException(String message, int replyCode, Throwable cause) {
        super(message, null, cause);
        this.replyCode = replyCode;
    }

    public int replyCode() {
        return replyCode;
    }

    /** 406 PRECONDITION_FAILED: a declare whose arguments disagree with the existing object's. */
    public boolean preconditionFailed() {
        return replyCode == 406;
    }
}
