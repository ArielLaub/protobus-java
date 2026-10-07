/**
 * The seam between protobus and an AMQP 0-9-1 client.
 *
 * protobus talks to the broker only through the interfaces here. The production
 * implementation is {@link io.github.ariellaub.protobus.amqp.RabbitTransport},
 * over the RabbitMQ Java client; tests substitute
 * {@link io.github.ariellaub.protobus.testing.MemoryBroker}, which has the same
 * semantics and lets settlement, retry, reconnection and streaming be tested
 * deterministically without RabbitMQ.
 *
 * <p>Threading: an implementation delivers callbacks (deliveries, confirms,
 * closes) on threads of its own, never the caller's. Callbacks must not block.
 * Channel methods that wait for a broker reply (declare, bind, consume, ...) must
 * not be called from inside a callback; publish, ack and reject may be.
 */
package io.github.ariellaub.protobus.amqp;
