package io.github.ariellaub.protobus.testing;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.AmqpConnection;
import io.github.ariellaub.protobus.amqp.AmqpException;
import io.github.ariellaub.protobus.amqp.ConfirmOutcome;
import io.github.ariellaub.protobus.amqp.Delivery;
import io.github.ariellaub.protobus.amqp.Transport;
import io.github.ariellaub.protobus.internal.Threads;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * An in-memory AMQP broker, for testing services and clients without RabbitMQ.
 *
 * <pre>{@code
 * MemoryBroker broker = new MemoryBroker();
 * Context ctx = new Context(ContextOptions.DEFAULT.withTransport(broker));
 * ctx.init("amqp://memory/");
 * }</pre>
 *
 * It implements the RabbitMQ behaviour protobus depends on: direct, topic and
 * fanout exchanges and the default exchange; durable, exclusive, auto-delete and
 * server-named queues; per-queue message TTL with dead-lettering; priority queues;
 * per-consumer prefetch with acknowledgement, rejection and redelivery; publisher
 * confirms with mandatory returns; and channel-closing errors for a missing
 * exchange or queue (404), a redeclaration with different arguments (406), and an
 * exclusive queue owned elsewhere (405).
 *
 * Callbacks run on one dispatcher thread of the broker's own, as a real client's
 * threads would run them, and a blocking channel call made from one throws
 * {@link IllegalStateException}: code that would deadlock against RabbitMQ fails
 * here too.
 *
 * Fault injection: {@link #killConnections} drops every connection as a network
 * failure would, {@link #refuseConnections} makes connect fail, and
 * {@link #setConfirmMode} makes the broker nack or never confirm publishes.
 */
public final class MemoryBroker implements Transport, AutoCloseable {
    public enum ConfirmMode {
        /** Confirm every publish (the default). */
        ACK,
        /** Refuse every publish. */
        NACK,
        /** Never confirm: publishes wait out their confirm timeout. */
        DROP
    }

    private final Object lock = new Object();
    private final ExecutorService dispatcher;
    private volatile Thread dispatcherThread;
    private final ScheduledExecutorService timers = Threads.scheduler("memory-broker-ttl");

    private final Map<String, Exchange> exchanges = new LinkedHashMap<>();
    private final Map<String, Queue> queues = new LinkedHashMap<>();
    private final List<Conn> connections = new ArrayList<>();
    private boolean refuse;
    private ConfirmMode confirmMode = ConfirmMode.ACK;
    private int connectAttempts;
    private long nextId;

    public MemoryBroker() {
        dispatcher = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "memory-broker");
            t.setDaemon(true);
            dispatcherThread = t;
            return t;
        });
        // RabbitMQ's predeclared default exchange.
        exchanges.put("", new Exchange("", "direct", true, false, false, Map.of()));
    }

    // ---- model -------------------------------------------------------------------------

    private static final class Exchange {
        final String name;
        final String type;
        final boolean durable;
        final boolean autoDelete;
        final boolean internal;
        final Map<String, Object> arguments;
        final List<String[]> bindings = new ArrayList<>(); // {queue, routingKey}

        Exchange(String name, String type, boolean durable, boolean autoDelete, boolean internal,
                 Map<String, Object> arguments) {
            this.name = name;
            this.type = type;
            this.durable = durable;
            this.autoDelete = autoDelete;
            this.internal = internal;
            this.arguments = arguments;
        }
    }

    private static final class Message {
        final byte[] body;
        final BasicProperties properties;
        final String exchange;
        final String routingKey;
        boolean redelivered;
        final long seq;

        Message(byte[] body, BasicProperties properties, String exchange, String routingKey, long seq) {
            this.body = body;
            this.properties = properties;
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.seq = seq;
        }
    }

    private final class Queue {
        final String name;
        final boolean durable;
        final boolean exclusive;
        final boolean autoDelete;
        final Map<String, Object> arguments;
        final Conn owner;
        final List<Message> messages = new ArrayList<>();
        final List<ConsumerEntry> consumers = new ArrayList<>();
        int roundRobin;
        boolean hadConsumer;
        int unacked;

        Queue(String name, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments,
              Conn owner) {
            this.name = name;
            this.durable = durable;
            this.exclusive = exclusive;
            this.autoDelete = autoDelete;
            this.arguments = arguments;
            this.owner = owner;
        }

        Long ttl() {
            Object v = arguments.get("x-message-ttl");
            return v instanceof Number ? ((Number) v).longValue() : null;
        }

        Integer maxPriority() {
            Object v = arguments.get("x-max-priority");
            return v instanceof Number ? ((Number) v).intValue() : null;
        }

        int priorityOf(Message m) {
            Integer max = maxPriority();
            if (max == null) return 0;
            Integer p = m.properties == null ? null : m.properties.getPriority();
            return Math.min(p == null ? 0 : p, max);
        }

        /** Insert keeping higher priorities first and FIFO within one. Requeued messages go first in their level. */
        void enqueue(Message m, boolean atHead) {
            int p = priorityOf(m);
            int i = 0;
            if (atHead) {
                while (i < messages.size() && priorityOf(messages.get(i)) > p) i++;
            } else {
                while (i < messages.size() && priorityOf(messages.get(i)) >= p) i++;
            }
            messages.add(i, m);
        }
    }

    private static final class ConsumerEntry {
        final String tag;
        final Chan channel;
        final Queue queue;
        final boolean noAck;
        final int prefetch;
        final Consumer<Delivery> onDelivery;
        final Runnable onCancel;
        int unacked;

        ConsumerEntry(String tag, Chan channel, Queue queue, boolean noAck, int prefetch,
                      Consumer<Delivery> onDelivery, Runnable onCancel) {
            this.tag = tag;
            this.channel = channel;
            this.queue = queue;
            this.noAck = noAck;
            this.prefetch = prefetch;
            this.onDelivery = onDelivery;
            this.onCancel = onCancel;
        }
    }

    private static final class Unacked {
        final Queue queue;
        final Message message;
        final ConsumerEntry consumer;

        Unacked(Queue queue, Message message, ConsumerEntry consumer) {
            this.queue = queue;
            this.message = message;
            this.consumer = consumer;
        }
    }

    // ---- transport ---------------------------------------------------------------------

    @Override
    public AmqpConnection connect(String url, int heartbeatSeconds) {
        synchronized (lock) {
            connectAttempts++;
            if (refuse) throw new AmqpException("connect failed: connection refused (memory broker)", 0);
            Conn c = new Conn();
            connections.add(c);
            return c;
        }
    }

    private final class Conn implements AmqpConnection {
        boolean open = true;
        final List<Chan> channels = new ArrayList<>();
        final List<Consumer<String>> closeListeners = new ArrayList<>();
        String closeReason;
        boolean closedGracefully;

        @Override
        public AmqpChannel openChannel() {
            requireNotDispatcher("openChannel");
            synchronized (lock) {
                if (!open) throw new AmqpException("open channel failed: connection is closed", 320);
                Chan ch = new Chan(this);
                channels.add(ch);
                return ch;
            }
        }

        @Override
        public void close() {
            List<Runnable> callbacks = new ArrayList<>();
            synchronized (lock) {
                if (!open) return;
                closedGracefully = true;
                closeConnection(this, null, callbacks);
            }
            post(callbacks);
        }

        @Override
        public boolean isOpen() {
            synchronized (lock) {
                return open;
            }
        }

        @Override
        public void onClose(Consumer<String> listener) {
            boolean now;
            String reason;
            synchronized (lock) {
                now = !open;
                reason = closedGracefully ? null : closeReason;
                if (!now) closeListeners.add(listener);
            }
            if (now) post(List.of(() -> listener.accept(reason)));
        }
    }

    private final class Chan implements AmqpChannel {
        final Conn conn;
        boolean open = true;
        int prefetch;
        long nextDeliveryTag = 1;
        final Map<Long, Unacked> unacked = new LinkedHashMap<>();
        final Map<String, ConsumerEntry> consumers = new LinkedHashMap<>();
        final List<Consumer<String>> closeListeners = new ArrayList<>();
        final Deque<BiConsumer<ConfirmOutcome, String>> held = new ArrayDeque<>();
        String closeReason;

        Chan(Conn conn) {
            this.conn = conn;
        }

        private void requireOpen(String what) {
            if (!open) throw new AmqpException(what + " failed: channel is closed (" + closeReason + ")", 504);
        }

        /** A channel exception: closes this channel, then throws. */
        private AmqpException fail(int code, String text, List<Runnable> callbacks) {
            String reason = code + " " + text;
            closeChannel(this, reason, callbacks);
            return new AmqpException(text, code);
        }

        private <T> T blocking(String what, java.util.function.Function<List<Runnable>, T> op) {
            requireNotDispatcher(what);
            List<Runnable> callbacks = new ArrayList<>();
            try {
                synchronized (lock) {
                    requireOpen(what);
                    return op.apply(callbacks);
                }
            } finally {
                post(callbacks);
            }
        }

        @Override
        public void declareExchange(String name, String type, boolean durable, boolean autoDelete, boolean internal,
                                    Map<String, Object> arguments) {
            blocking("exchange.declare", cb -> {
                Exchange existing = exchanges.get(name);
                if (existing != null) {
                    if (!existing.type.equals(type) || existing.durable != durable || existing.autoDelete != autoDelete) {
                        throw fail(406, "PRECONDITION_FAILED - inequivalent arg 'type' for exchange '" + name + "'", cb);
                    }
                    return null;
                }
                exchanges.put(name, new Exchange(name, type, durable, autoDelete, internal, copy(arguments)));
                return null;
            });
        }

        @Override
        public String declareQueue(String name, boolean durable, boolean exclusive, boolean autoDelete,
                                   Map<String, Object> arguments) {
            return blocking("queue.declare", cb -> {
                String actual = name == null || name.isEmpty() ? "amq.gen-" + UUID.randomUUID() : name;
                Queue existing = queues.get(actual);
                if (existing != null) {
                    if (existing.exclusive && existing.owner != conn) {
                        throw fail(405, "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '"
                                + actual + "'", cb);
                    }
                    if (existing.durable != durable || existing.exclusive != exclusive
                            || existing.autoDelete != autoDelete || !sameArguments(existing.arguments, arguments)) {
                        throw fail(406, "PRECONDITION_FAILED - inequivalent arg for queue '" + actual + "'", cb);
                    }
                    return actual;
                }
                queues.put(actual, new Queue(actual, durable, exclusive, autoDelete, copy(arguments),
                        exclusive ? conn : null));
                // Every queue is bound to the default exchange under its name.
                return actual;
            });
        }

        @Override
        public void bindQueue(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
            blocking("queue.bind", cb -> {
                Exchange x = exchanges.get(exchange);
                if (x == null) throw fail(404, "NOT_FOUND - no exchange '" + exchange + "'", cb);
                if (!queues.containsKey(queue)) throw fail(404, "NOT_FOUND - no queue '" + queue + "'", cb);
                if (exchange.isEmpty()) throw fail(403, "ACCESS_REFUSED - cannot bind to the default exchange", cb);
                for (String[] b : x.bindings) {
                    if (b[0].equals(queue) && b[1].equals(routingKey)) return null;
                }
                x.bindings.add(new String[] {queue, routingKey});
                return null;
            });
        }

        @Override
        public void unbindQueue(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
            blocking("queue.unbind", cb -> {
                Exchange x = exchanges.get(exchange);
                if (x != null) x.bindings.removeIf(b -> b[0].equals(queue) && b[1].equals(routingKey));
                return null;
            });
        }

        @Override
        public void deleteQueue(String name) {
            blocking("queue.delete", cb -> {
                Queue q = queues.get(name);
                if (q != null) deleteQueueLocked(q, cb);
                return null;
            });
        }

        @Override
        public void purgeQueue(String name) {
            blocking("queue.purge", cb -> {
                Queue q = queues.get(name);
                if (q == null) throw fail(404, "NOT_FOUND - no queue '" + name + "'", cb);
                q.messages.clear();
                return null;
            });
        }

        @Override
        public void prefetch(int count) {
            blocking("basic.qos", cb -> {
                prefetch = count;
                return null;
            });
        }

        @Override
        public String consume(String queue, String consumerTag, boolean noAck, boolean exclusive,
                              Consumer<Delivery> onDelivery, Runnable onCancel) {
            return blocking("basic.consume", cb -> {
                Queue q = queues.get(queue);
                if (q == null) throw fail(404, "NOT_FOUND - no queue '" + queue + "'", cb);
                if (q.exclusive && q.owner != conn) {
                    throw fail(405, "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '" + queue
                            + "'", cb);
                }
                if (exclusive && !q.consumers.isEmpty()) {
                    throw fail(403, "ACCESS_REFUSED - queue '" + queue + "' in use", cb);
                }
                String tag = consumerTag == null || consumerTag.isEmpty() ? "amq.ctag-" + UUID.randomUUID() : consumerTag;
                if (consumers.containsKey(tag)) throw fail(530, "NOT_ALLOWED - attempt to reuse consumer tag", cb);
                ConsumerEntry c = new ConsumerEntry(tag, this, q, noAck, prefetch, onDelivery, onCancel);
                consumers.put(tag, c);
                q.consumers.add(c);
                q.hadConsumer = true;
                pump(q, cb);
                return tag;
            });
        }

        @Override
        public void cancel(String consumerTag) {
            blocking("basic.cancel", cb -> {
                ConsumerEntry c = consumers.remove(consumerTag);
                if (c != null) removeConsumer(c, cb);
                return null;
            });
        }

        @Override
        public void ack(long deliveryTag) {
            List<Runnable> cb = new ArrayList<>();
            try {
                synchronized (lock) {
                    requireOpen("basic.ack");
                    Unacked u = unacked.remove(deliveryTag);
                    if (u == null) throw fail(406, "PRECONDITION_FAILED - unknown delivery tag " + deliveryTag, cb);
                    settle(u, cb);
                }
            } finally {
                post(cb);
            }
        }

        @Override
        public void reject(long deliveryTag, boolean requeue) {
            List<Runnable> cb = new ArrayList<>();
            try {
                synchronized (lock) {
                    requireOpen("basic.reject");
                    Unacked u = unacked.remove(deliveryTag);
                    if (u == null) throw fail(406, "PRECONDITION_FAILED - unknown delivery tag " + deliveryTag, cb);
                    settle(u, cb);
                    if (requeue && queues.get(u.queue.name) == u.queue) {
                        u.message.redelivered = true;
                        u.queue.enqueue(u.message, true);
                        pump(u.queue, cb);
                    } else {
                        deadLetter(u.queue, u.message, "rejected", cb);
                    }
                }
            } finally {
                post(cb);
            }
        }

        @Override
        public void publish(String exchange, String routingKey, byte[] body, BasicProperties properties,
                            boolean mandatory, BiConsumer<ConfirmOutcome, String> onConfirm) {
            List<Runnable> cb = new ArrayList<>();
            try {
                synchronized (lock) {
                    requireOpen("basic.publish");
                    Exchange x = exchanges.get(exchange == null ? "" : exchange);
                    if (x == null) {
                        // Asynchronous in AMQP: the channel closes, and the publish's
                        // confirm never comes: it is reported closed.
                        cb.add(() -> onConfirm.accept(ConfirmOutcome.CLOSED, "404 NOT_FOUND - no exchange '"
                                + exchange + "'"));
                        closeChannel(this, "404 NOT_FOUND - no exchange '" + exchange + "'", cb);
                        return;
                    }
                    Message m = new Message(body.clone(), properties, x.name, routingKey, ++nextId);
                    int routed = route(x, m, cb);
                    ConfirmOutcome outcome;
                    switch (confirmMode) {
                        case NACK: outcome = ConfirmOutcome.NACK; break;
                        case DROP: outcome = null; break;
                        default: outcome = routed == 0 && mandatory ? ConfirmOutcome.RETURNED : ConfirmOutcome.ACK;
                    }
                    if (outcome == null) {
                        held.add(onConfirm);
                    } else {
                        cb.add(() -> onConfirm.accept(outcome, outcome == ConfirmOutcome.NACK ? "basic.nack" : ""));
                    }
                }
            } finally {
                post(cb);
            }
        }

        @Override
        public void close() {
            List<Runnable> cb = new ArrayList<>();
            synchronized (lock) {
                if (!open) return;
                closeChannel(this, "200 OK (closed by the client)", cb);
            }
            post(cb);
        }

        @Override
        public boolean isOpen() {
            synchronized (lock) {
                return open;
            }
        }

        @Override
        public void onClose(Consumer<String> listener) {
            boolean now;
            String reason;
            synchronized (lock) {
                now = !open;
                reason = closeReason;
                if (!now) closeListeners.add(listener);
            }
            if (now) post(List.of(() -> listener.accept(reason)));
        }
    }

    // ---- broker internals (all under lock) ---------------------------------------------

    private static Map<String, Object> copy(Map<String, Object> m) {
        return m == null ? Map.of() : new LinkedHashMap<>(m);
    }

    /** RabbitMQ compares integer arguments by value, whatever their width. */
    private static boolean sameArguments(Map<String, Object> a, Map<String, Object> b) {
        Map<String, Object> x = a == null ? Map.of() : a;
        Map<String, Object> y = b == null ? Map.of() : b;
        if (!x.keySet().equals(y.keySet())) return false;
        for (String k : x.keySet()) {
            Object u = x.get(k);
            Object v = y.get(k);
            if (u instanceof Number && v instanceof Number) {
                if (((Number) u).longValue() != ((Number) v).longValue()) return false;
            } else if (!Objects.equals(String.valueOf(u), String.valueOf(v))) {
                return false;
            }
        }
        return true;
    }

    static boolean topicMatches(String pattern, String key) {
        return match(pattern.split("\\.", -1), 0, key.split("\\.", -1), 0);
    }

    private static boolean match(String[] p, int i, String[] k, int j) {
        if (i == p.length) return j == k.length;
        if (p[i].equals("#")) {
            for (int n = j; n <= k.length; n++) {
                if (match(p, i + 1, k, n)) return true;
            }
            return false;
        }
        if (j == k.length) return false;
        if (p[i].equals("*") || p[i].equals(k[j])) return match(p, i + 1, k, j + 1);
        return false;
    }

    /** Route to every matching queue; returns how many received it. */
    private int route(Exchange x, Message m, List<Runnable> cb) {
        List<Queue> targets = new ArrayList<>();
        if (x.name.isEmpty()) {
            Queue q = queues.get(m.routingKey);
            if (q != null) targets.add(q);
        } else {
            for (String[] b : x.bindings) {
                boolean hit;
                switch (x.type) {
                    case "fanout": hit = true; break;
                    case "topic": hit = topicMatches(b[1], m.routingKey); break;
                    default: hit = b[1].equals(m.routingKey);
                }
                Queue q = queues.get(b[0]);
                if (hit && q != null && !targets.contains(q)) targets.add(q);
            }
        }
        for (Queue q : targets) {
            Message copy = new Message(m.body, m.properties, m.exchange, m.routingKey, ++nextId);
            q.enqueue(copy, false);
            scheduleExpiry(q, copy);
            pump(q, cb);
        }
        return targets.size();
    }

    private void scheduleExpiry(Queue q, Message m) {
        Long ttl = q.ttl();
        if (ttl == null) return;
        timers.schedule(() -> {
            List<Runnable> cb = new ArrayList<>();
            synchronized (lock) {
                if (queues.get(q.name) != q || !q.messages.remove(m)) return;
                deadLetter(q, m, "expired", cb);
            }
            post(cb);
        }, ttl, TimeUnit.MILLISECONDS);
    }

    private void deadLetter(Queue q, Message m, String reason, List<Runnable> cb) {
        Object dlx = q.arguments.get("x-dead-letter-exchange");
        if (dlx == null) return;
        Exchange x = exchanges.get(String.valueOf(dlx));
        if (x == null) return;
        Object dlrk = q.arguments.get("x-dead-letter-routing-key");
        String key = dlrk == null ? m.routingKey : String.valueOf(dlrk);
        Map<String, Object> headers = new LinkedHashMap<>(
                m.properties == null || m.properties.getHeaders() == null ? Map.of() : m.properties.getHeaders());
        headers.put("x-first-death-reason", reason);
        headers.put("x-first-death-queue", q.name);
        BasicProperties props = (m.properties == null ? new BasicProperties() : m.properties).builder()
                .headers(headers).expiration(null).build();
        route(x, new Message(m.body, props, x.name, key, ++nextId), cb);
    }

    private void pump(Queue q, List<Runnable> cb) {
        while (!q.messages.isEmpty() && !q.consumers.isEmpty()) {
            ConsumerEntry target = null;
            int n = q.consumers.size();
            for (int i = 0; i < n; i++) {
                ConsumerEntry c = q.consumers.get((q.roundRobin + i) % n);
                if (c.noAck || c.prefetch == 0 || c.unacked < c.prefetch) {
                    target = c;
                    q.roundRobin = (q.roundRobin + i + 1) % n;
                    break;
                }
            }
            if (target == null) return;
            Message m = q.messages.remove(0);
            Chan ch = target.channel;
            long tag = ch.nextDeliveryTag++;
            if (!target.noAck) {
                ch.unacked.put(tag, new Unacked(q, m, target));
                target.unacked++;
                q.unacked++;
            }
            Delivery d = new Delivery(m.body.clone(), m.properties, m.exchange, m.routingKey, target.tag, tag,
                    m.redelivered);
            ConsumerEntry c = target;
            cb.add(() -> {
                synchronized (lock) {
                    // A consumer cancelled, or a channel closed, after the delivery
                    // was queued: real brokers can still deliver it, but nobody
                    // would ack it here. Its requeue already happened on close.
                    if (!ch.open) return;
                }
                c.onDelivery.accept(d);
            });
        }
    }

    private void settle(Unacked u, List<Runnable> cb) {
        u.consumer.unacked--;
        u.queue.unacked--;
        pump(u.queue, cb);
    }

    private void removeConsumer(ConsumerEntry c, List<Runnable> cb) {
        c.queue.consumers.remove(c);
        if (c.queue.autoDelete && c.queue.hadConsumer && c.queue.consumers.isEmpty()) {
            deleteQueueLocked(c.queue, cb);
        }
    }

    private void deleteQueueLocked(Queue q, List<Runnable> cb) {
        if (queues.get(q.name) != q) return;
        queues.remove(q.name);
        for (Exchange x : exchanges.values()) x.bindings.removeIf(b -> b[0].equals(q.name));
        for (ConsumerEntry c : new ArrayList<>(q.consumers)) {
            c.channel.consumers.remove(c.tag);
            q.consumers.remove(c);
            if (c.onCancel != null) cb.add(c.onCancel);
        }
    }

    private void closeChannel(Chan ch, String reason, List<Runnable> cb) {
        if (!ch.open) return;
        ch.open = false;
        ch.closeReason = reason;
        ch.conn.channels.remove(ch);
        // Unacknowledged deliveries go back to their queues, marked redelivered.
        List<Unacked> back = new ArrayList<>(ch.unacked.values());
        ch.unacked.clear();
        for (ConsumerEntry c : new ArrayList<>(ch.consumers.values())) removeConsumer(c, cb);
        ch.consumers.clear();
        for (Unacked u : back) {
            u.consumer.unacked--;
            u.queue.unacked--;
            if (queues.get(u.queue.name) == u.queue) {
                u.message.redelivered = true;
                u.queue.enqueue(u.message, true);
            }
        }
        for (Unacked u : back) {
            if (queues.get(u.queue.name) == u.queue) pump(u.queue, cb);
        }
        while (!ch.held.isEmpty()) {
            BiConsumer<ConfirmOutcome, String> c = ch.held.poll();
            cb.add(() -> c.accept(ConfirmOutcome.CLOSED, reason));
        }
        for (Consumer<String> l : ch.closeListeners) cb.add(() -> l.accept(reason));
        ch.closeListeners.clear();
    }

    private void closeConnection(Conn c, String reason, List<Runnable> cb) {
        c.open = false;
        c.closeReason = reason;
        connections.remove(c);
        for (Chan ch : new ArrayList<>(c.channels)) closeChannel(ch, reason == null ? "320 CONNECTION_FORCED" : reason, cb);
        // Exclusive queues die with the connection that owns them.
        for (Queue q : new ArrayList<>(queues.values())) {
            if (q.exclusive && q.owner == c) deleteQueueLocked(q, cb);
        }
        String reported = c.closedGracefully ? null : reason;
        for (Consumer<String> l : c.closeListeners) cb.add(() -> l.accept(reported));
        c.closeListeners.clear();
    }

    private void post(List<Runnable> callbacks) {
        if (callbacks.isEmpty()) return;
        List<Runnable> run = new ArrayList<>(callbacks);
        try {
            dispatcher.execute(() -> {
                for (Runnable r : run) {
                    try {
                        r.run();
                    } catch (RuntimeException e) {
                        System.err.println("memory-broker: callback failed: " + e);
                        e.printStackTrace();
                    }
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            // The broker was closed.
        }
    }

    private void requireNotDispatcher(String what) {
        if (Thread.currentThread() == dispatcherThread) {
            throw new IllegalStateException(what + " is a blocking channel call, made from a transport callback: "
                    + "against RabbitMQ this would deadlock");
        }
    }

    // ---- fault injection ---------------------------------------------------------------

    /**
     * Drop every open connection as a lost socket would: unacknowledged deliveries
     * are requeued, exclusive queues deleted, and each connection's close listeners
     * receive {@code reason}.
     */
    public void killConnections(String reason) {
        List<Runnable> cb = new ArrayList<>();
        synchronized (lock) {
            for (Conn c : new ArrayList<>(connections)) closeConnection(c, reason, cb);
        }
        post(cb);
    }

    public void killConnections() {
        killConnections("connection reset by peer");
    }

    /** While set, connect throws. */
    public void refuseConnections(boolean value) {
        synchronized (lock) {
            refuse = value;
        }
    }

    public void setConfirmMode(ConfirmMode mode) {
        synchronized (lock) {
            confirmMode = mode;
        }
    }

    /** The publishes {@link ConfirmMode#DROP} is holding, across every open channel. */
    public int heldConfirms() {
        synchronized (lock) {
            int n = 0;
            for (Conn c : connections) for (Chan ch : c.channels) n += ch.held.size();
            return n;
        }
    }

    /** Deliver every held confirm now, as {@code outcome}: a broker confirming late. */
    public int releaseHeldConfirms(ConfirmOutcome outcome) {
        List<Runnable> cb = new ArrayList<>();
        synchronized (lock) {
            for (Conn c : connections) {
                for (Chan ch : c.channels) {
                    while (!ch.held.isEmpty()) {
                        BiConsumer<ConfirmOutcome, String> h = ch.held.poll();
                        cb.add(() -> h.accept(outcome, ""));
                    }
                }
            }
        }
        post(cb);
        return cb.size();
    }

    /**
     * Close every channel consuming {@code queue}, as a broker closes a channel over
     * a consumer error, leaving the connection up.
     */
    public void closeChannelsConsuming(String queue, String reason) {
        List<Runnable> cb = new ArrayList<>();
        synchronized (lock) {
            Queue q = queues.get(queue);
            if (q == null) return;
            List<Chan> chans = new ArrayList<>();
            for (ConsumerEntry c : q.consumers) if (!chans.contains(c.channel)) chans.add(c.channel);
            for (Chan ch : chans) closeChannel(ch, reason, cb);
        }
        post(cb);
    }

    /** Cancel every consumer of {@code queue} from the broker side (basic.cancel), leaving channels open. */
    public void cancelConsumers(String queue) {
        List<Runnable> cb = new ArrayList<>();
        synchronized (lock) {
            Queue q = queues.get(queue);
            if (q == null) return;
            for (ConsumerEntry c : new ArrayList<>(q.consumers)) {
                c.channel.consumers.remove(c.tag);
                q.consumers.remove(c);
                if (c.onCancel != null) cb.add(c.onCancel);
            }
        }
        post(cb);
    }

    // ---- inspection --------------------------------------------------------------------

    public boolean queueExists(String name) {
        synchronized (lock) {
            return queues.containsKey(name);
        }
    }

    public boolean exchangeExists(String name) {
        synchronized (lock) {
            return exchanges.containsKey(name);
        }
    }

    /** Messages ready in the queue (not counting unacknowledged ones). */
    public int queueDepth(String name) {
        synchronized (lock) {
            Queue q = queues.get(name);
            return q == null ? 0 : q.messages.size();
        }
    }

    public int unackedCount(String name) {
        synchronized (lock) {
            Queue q = queues.get(name);
            return q == null ? 0 : q.unacked;
        }
    }

    public int consumerCount(String name) {
        synchronized (lock) {
            Queue q = queues.get(name);
            return q == null ? 0 : q.consumers.size();
        }
    }

    public Map<String, Object> queueArguments(String name) {
        synchronized (lock) {
            Queue q = queues.get(name);
            return q == null ? null : new HashMap<>(q.arguments);
        }
    }

    /** The messages waiting in a queue, in delivery order, without removing them. */
    public List<Delivery> peek(String name) {
        synchronized (lock) {
            Queue q = queues.get(name);
            List<Delivery> out = new ArrayList<>();
            if (q == null) return out;
            for (Message m : q.messages) {
                out.add(new Delivery(m.body.clone(), m.properties, m.exchange, m.routingKey, "", 0, m.redelivered));
            }
            return out;
        }
    }

    /** The routing keys bound from {@code exchange} to {@code queue}. */
    public List<String> bindings(String queue, String exchange) {
        synchronized (lock) {
            Exchange x = exchanges.get(exchange);
            List<String> out = new ArrayList<>();
            if (x == null) return out;
            for (String[] b : x.bindings) if (b[0].equals(queue)) out.add(b[1]);
            return out;
        }
    }

    public List<String> queueNames() {
        synchronized (lock) {
            return new ArrayList<>(queues.keySet());
        }
    }

    public int openConnections() {
        synchronized (lock) {
            return connections.size();
        }
    }

    public int connectAttempts() {
        synchronized (lock) {
            return connectAttempts;
        }
    }

    /** Block until every callback queued so far has run. */
    public void flush() {
        CountDownLatch latch = new CountDownLatch(1);
        dispatcher.execute(latch::countDown);
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Wait until {@code predicate} holds, polling, up to {@code timeout}. */
    public static boolean waitFor(BooleanSupplier predicate, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!predicate.getAsBoolean()) {
            if (System.nanoTime() >= deadline) return false;
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    public static boolean waitFor(BooleanSupplier predicate) {
        return waitFor(predicate, Duration.ofSeconds(5));
    }

    /** Stop the broker's threads. Connections are dropped first. */
    @Override
    public void close() {
        killConnections("broker shut down");
        flush();
        dispatcher.shutdown();
        timers.shutdownNow();
    }
}
