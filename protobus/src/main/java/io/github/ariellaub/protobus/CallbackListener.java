package io.github.ariellaub.protobus;

/**
 * The reply queue of a context: exclusive, server-named, bound to the callbacks
 * exchange under its own name. Replies are handled in delivery order, so a
 * stream's chunks are seen in the order the broker delivered them.
 */
public class CallbackListener extends BaseListener {
    public CallbackListener(Connection connection) {
        super(connection);
        exchangeName = Config.callbacksExchangeName();
        exchangeType = "direct";
        orderedDelivery = true;
    }

    public String callbackQueue() {
        return queueName();
    }
}
