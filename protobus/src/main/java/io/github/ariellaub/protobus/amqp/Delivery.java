package io.github.ariellaub.protobus.amqp;

import com.rabbitmq.client.AMQP.BasicProperties;

/** One message as the broker delivered it. */
public record Delivery(
        byte[] body,
        BasicProperties properties,
        String exchange,
        String routingKey,
        String consumerTag,
        long deliveryTag,
        boolean redelivered) {}
