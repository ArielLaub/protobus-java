package io.github.ariellaub.protobus.amqp;

/** How the broker answered a publish on a confirm channel. */
public enum ConfirmOutcome {
    /** Stored. */
    ACK,
    /** Refused (basic.nack). */
    NACK,
    /** A mandatory publish matched no queue: returned, then acked. */
    RETURNED,
    /** The channel closed with the publish unconfirmed: the outcome is UNKNOWN. */
    CLOSED
}
