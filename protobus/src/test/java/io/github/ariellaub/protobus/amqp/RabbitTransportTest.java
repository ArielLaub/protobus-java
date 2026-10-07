package io.github.ariellaub.protobus.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

class RabbitTransportTest {
    @Test
    void aTrailingSlashIsTheDefaultVhostAsInEveryPort() {
        // The AMQP URI spec reads "amqp://host/" as the empty vhost; amqplib,
        // pika, the Go client and rabbitmq-c as protobus uses them all reach "/".
        assertEquals("/", RabbitTransport.configure("amqp://guest:guest@127.0.0.1:25672/", 30).getVirtualHost());
        assertEquals("/", RabbitTransport.configure("amqp://127.0.0.1", 30).getVirtualHost());
        assertEquals("/", RabbitTransport.configure("amqp://h/%2f", 30).getVirtualHost());
        assertEquals("prod", RabbitTransport.configure("amqp://h/prod", 30).getVirtualHost());
    }

    @Test
    void credentialsHostAndPortComeFromTheUrl() {
        ConnectionFactory f = RabbitTransport.configure("amqp://user:p%40ss@broker:5673/v", 30);
        assertEquals("user", f.getUsername());
        assertEquals("p@ss", f.getPassword());
        assertEquals("broker", f.getHost());
        assertEquals(5673, f.getPort());
    }

    @Test
    void theUrlsHeartbeatWinsAndZeroDisablesIt() {
        assertEquals(30, RabbitTransport.configure("amqp://h/", 30).getRequestedHeartbeat());
        assertEquals(5, RabbitTransport.configure("amqp://h/?heartbeat=5", 30).getRequestedHeartbeat());
        assertEquals(0, RabbitTransport.configure("amqp://h/?heartbeat=0", 30).getRequestedHeartbeat());
        assertThrows(AmqpException.class, () -> RabbitTransport.configure("amqp://h/?heartbeat=x", 30));
    }

    @Test
    void amqpsVerifiesTheBrokerUnlessToldNotTo() {
        assertTrue(RabbitTransport.configure("amqps://h/", 30).isSSL());
        assertEquals(5671, RabbitTransport.configure("amqps://h/", 30).getPort());
        assertFalse(RabbitTransport.configure("amqp://h/", 30).isSSL());
    }

    @Test
    void protobusDoesItsOwnRecovery() {
        assertFalse(RabbitTransport.configure("amqp://h/", 30).isAutomaticRecoveryEnabled());
        assertFalse(RabbitTransport.configure("amqp://h/", 30).isTopologyRecoveryEnabled());
    }

    @Test
    void anUnparseableUrlNeverEchoesItsPassword() {
        AmqpException e = assertThrows(AmqpException.class,
                () -> RabbitTransport.configure("amqp://svc:p%ss word@host/", 30));
        assertFalse(e.getMessage().contains("p%ss"), e.getMessage());
    }
}
