package io.github.ariellaub.protobus.amqp;

/** Opens connections. */
public interface Transport {
    /**
     * Connect and log in. {@code heartbeatSeconds} applies unless the URL carries a
     * {@code heartbeat} parameter, which wins (0 disables heartbeats).
     *
     * @throws AmqpException when the broker cannot be reached or refuses the login
     */
    AmqpConnection connect(String url, int heartbeatSeconds);
}
