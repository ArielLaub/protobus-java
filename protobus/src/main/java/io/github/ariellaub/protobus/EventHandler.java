package io.github.ariellaub.protobus;

import com.google.protobuf.Message;

/**
 * Handles one event. A handler that throws fails the delivery: the event is
 * dropped, or retried when the service enabled {@link EventRetryOptions}.
 *
 * @param <T> the event's message type; {@link Message} receives a
 *     {@link com.google.protobuf.DynamicMessage} of whatever type arrived
 */
@FunctionalInterface
public interface EventHandler<T extends Message> {
    /**
     * @param event the decoded event
     * @param type the event's full message name
     * @param topic the topic it was published under
     */
    void handle(T event, String type, String topic) throws Exception;
}
