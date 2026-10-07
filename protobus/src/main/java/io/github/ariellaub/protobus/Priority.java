package io.github.ariellaub.protobus;

/** Validation of queue-level and per-message priorities, before anything reaches the broker. */
public final class Priority {
    private Priority() {}

    /** AMQP carries the priority in one byte. */
    static final int MIN_PRIORITY = 0;
    static final int MAX_PRIORITY = 255;

    /**
     * Validate the queue-level {@code maxPriority}, which becomes
     * {@code x-max-priority} on {@code queue.declare}.
     *
     * The floor is 1, not 0: {@code x-max-priority: 0} is accepted by RabbitMQ but
     * gives a single level, a plain queue with a priority queue's overhead and an
     * argument set that no longer matches the queue it replaced.
     */
    public static Integer validateMaxPriority(Integer value) {
        return require(value, "maxPriority", 1, MAX_PRIORITY,
                "RabbitMQ maintains internal structures per priority level, so keep the range small: "
                        + Config.RECOMMENDED_MAX_PRIORITY + " is the recommended value and gives "
                        + (Config.RECOMMENDED_MAX_PRIORITY + 1) + " levels.");
    }

    /** Validate a per-message priority. 0 is valid and is RabbitMQ's default. */
    public static Integer validatePriority(Integer value) {
        return require(value, "priority", MIN_PRIORITY, MAX_PRIORITY,
                "A priority above the queue's x-max-priority is clamped by the broker, not rejected.");
    }

    private static Integer require(Integer value, String label, int min, int max, String extra) {
        if (value == null) return null;
        if (value < min || value > max) {
            throw new InvalidPriorityError(label + " must be between " + min + " and " + max + ", got " + value
                    + ". " + extra);
        }
        return value;
    }
}
