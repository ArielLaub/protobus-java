package io.github.ariellaub.protobus;

/**
 * A service that also holds a typed proxy to its own contract, for calling other
 * replicas of itself.
 *
 * <pre>{@code
 * class Worker extends ProxiedService<WorkerProtobus.Proxy> {
 *     Worker(Context ctx) { super(ctx); }
 *     protected WorkerProtobus.Proxy newProxy(Context ctx, String name) { return new WorkerProtobus.Proxy(ctx, name); }
 *     ...
 * }
 * }</pre>
 */
public abstract class ProxiedService<P> extends RunnableService {
    private volatile P proxy;

    protected ProxiedService(Context context) {
        super(context);
    }

    protected ProxiedService(Context context, MessageServiceOptions options) {
        super(context, options);
    }

    /** Build the proxy; it is initialised by the caller of {@link #init()} only through {@link #initProxy}. */
    protected abstract P newProxy(Context context, String serviceName);

    /** Initialise the proxy built by {@link #newProxy}. Generated proxies initialise themselves. */
    protected void initProxy(P proxy) {
        if (proxy instanceof ServiceProxy) ((ServiceProxy) proxy).init();
    }

    public P proxy() {
        return proxy;
    }

    @Override
    public void init() {
        super.init();
        P p = newProxy(context, serviceName());
        initProxy(p);
        proxy = p;
    }
}
