package io.github.ariellaub.protobus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A MessageService with process lifecycle.
 *
 * <pre>{@code
 * public static void main(String[] args) {
 *     Context ctx = new Context();
 *     ctx.init(System.getenv("AMQP_URL"));
 *     RunnableService.start(ctx, CalculatorService::new);
 *     System.exit(RunnableService.awaitShutdown());
 * }
 * }</pre>
 *
 * {@link #start} initialises the service and registers it for graceful shutdown,
 * which runs on SIGINT or SIGTERM (a JVM shutdown hook) or on
 * {@link #requestShutdown()}: every started service stops taking new work,
 * in-flight work drains (up to {@code SHUTDOWN_DRAIN_TIMEOUT_MS}), each service's
 * {@link #cleanup()} runs, and the contexts close. {@link #cleanup()} never runs
 * while a delivery is still being handled, unless the drain deadline passed.
 */
public abstract class RunnableService extends MessageService {
    protected RunnableService(Context context) {
        super(context);
    }

    protected RunnableService(Context context, MessageServiceOptions options) {
        super(context, options);
    }

    /** Release the service's own resources at shutdown, once its work has drained. Default: nothing. */
    protected void cleanup() throws Exception {}

    private record Started(Context context, RunnableService service) {}

    private static final Object lock = new Object();
    private static final List<Started> started = new ArrayList<>();
    private static boolean hookInstalled;
    private static boolean shuttingDown;
    private static CountDownLatch done = new CountDownLatch(1);
    private static volatile int exitCode;

    /** Construct, initialise and register a service with default options. */
    public static <T extends RunnableService> T start(Context context, BiFunction<Context, MessageServiceOptions, T>
            constructor) {
        return start(context, constructor, MessageServiceOptions.DEFAULT, null);
    }

    public static <T extends RunnableService> T start(Context context,
            java.util.function.Function<Context, T> constructor) {
        return start(context, (c, o) -> constructor.apply(c), MessageServiceOptions.DEFAULT, null);
    }

    /**
     * Construct {@code constructor(context, options)}, initialise it, run
     * {@code postInit}, and register it for graceful shutdown. On a startup failure
     * the service is stopped, the context closed, and the error rethrown.
     */
    public static <T extends RunnableService> T start(Context context,
            BiFunction<Context, MessageServiceOptions, T> constructor, MessageServiceOptions options,
            Consumer<T> postInit) {
        installHook();
        T service = null;
        try {
            service = constructor.apply(context, options);
            Logger.info("Starting service: " + service.serviceName());
            service.init();
            if (postInit != null) postInit.accept(service);
            synchronized (lock) {
                if (shuttingDown) throw new NotReadyError("a shutdown is in progress");
                started.add(new Started(context, service));
            }
            Logger.info("Service ready: " + service.serviceName());
            return service;
        } catch (RuntimeException e) {
            Logger.error("Service startup failed: " + e);
            if (service != null) {
                try {
                    service.close();
                } catch (RuntimeException closeError) {
                    Logger.debug("closing a service that failed to start: " + closeError.getMessage());
                }
            }
            context.close();
            exitCode = 1;
            throw e;
        }
    }

    private static void installHook() {
        synchronized (lock) {
            if (hookInstalled) return;
            hookInstalled = true;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown("signal"), "protobus-shutdown"));
    }

    /** Shut every started service down, as a signal would. Returns at once. */
    public static void requestShutdown() {
        Thread t = new Thread(() -> shutdown("requested"), "protobus-shutdown");
        t.setDaemon(true);
        t.start();
    }

    /** Block until a shutdown has completed; returns the exit code (0, or 1 after a startup failure). */
    public static int awaitShutdown() throws InterruptedException {
        CountDownLatch latch;
        synchronized (lock) {
            latch = done;
        }
        latch.await();
        return exitCode;
    }

    /** For tests that start services in-process: allow another start/shutdown cycle. */
    static void resetForTests() {
        synchronized (lock) {
            started.clear();
            shuttingDown = false;
            done = new CountDownLatch(1);
            exitCode = 0;
        }
    }

    private static void shutdown(String reason) {
        List<Started> services;
        CountDownLatch latch;
        synchronized (lock) {
            if (shuttingDown) return;
            shuttingDown = true;
            services = new ArrayList<>(started);
            latch = done;
        }
        Logger.info("Shutdown initiated (" + reason + ")");
        try {
            // 1. Stop taking new work, keeping channels open: cleanup must not run
            //    while consumers still deliver.
            for (Started s : services) {
                try {
                    s.service.stopConsuming();
                } catch (RuntimeException e) {
                    Logger.error("Failed to stop consumers of " + s.service.serviceName() + ": " + e);
                }
            }
            Logger.info("Stopped accepting new messages");
            // 2. Let work in hand finish, including the reply, retry or DLQ publish
            //    that settles it.
            long budget = Config.shutdownDrainTimeoutMs();
            List<Connection> drained = new ArrayList<>();
            for (Started s : services) {
                Connection c = s.context.connection();
                if (drained.contains(c)) continue;
                drained.add(c);
                long inFlight = c.inFlightDeliveries();
                if (inFlight == 0) continue;
                Logger.info("Draining " + inFlight + " in-flight message(s), up to " + budget + "ms");
                Logger.info(c.drainInFlight(budget) ? "In-flight messages drained"
                        : "Drain deadline reached with " + c.inFlightDeliveries() + " still running; they stay "
                                + "unacknowledged and will be redelivered");
            }
            // 3. Only now is it safe to release the services' own resources.
            for (Started s : services) {
                try {
                    s.service.cleanup();
                } catch (Exception e) {
                    Logger.error("Service cleanup failed for " + s.service.serviceName() + ": " + e);
                }
            }
            List<Context> closed = new ArrayList<>();
            for (Started s : services) {
                if (closed.contains(s.context)) continue;
                closed.add(s.context);
                try {
                    s.context.close();
                } catch (RuntimeException e) {
                    Logger.error("Connection close failed: " + e);
                }
            }
            Logger.info("Connection closed");
        } finally {
            latch.countDown();
        }
    }
}
