package io.github.ariellaub.protobus.internal;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** The library's own threads: named, and daemon, so they never hold a JVM open. */
public final class Threads {
    private Threads() {}

    public static ThreadFactory factory(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    public static ExecutorService cachedPool(String prefix) {
        return Executors.newCachedThreadPool(factory(prefix));
    }

    public static ScheduledExecutorService scheduler(String prefix) {
        ScheduledThreadPoolExecutor s = new ScheduledThreadPoolExecutor(1, factory(prefix));
        // Cancelled timeouts are the common case; do not keep them queued.
        s.setRemoveOnCancelPolicy(true);
        return s;
    }

    public static String stackTrace(Throwable t) {
        if (t == null) return "";
        StringWriter out = new StringWriter();
        t.printStackTrace(new PrintWriter(out));
        return out.toString();
    }
}
