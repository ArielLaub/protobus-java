package io.github.ariellaub.protobus;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Parser;
import io.github.ariellaub.protobus.internal.Envelopes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Calls a service on the bus by name.
 *
 * Generated {@code <Service>Protobus.Proxy} classes wrap one with a typed method
 * per rpc. Used directly, it calls methods by name with dynamic messages:
 *
 * <pre>{@code
 * ServiceProxy calc = new ServiceProxy(ctx, "Calculator.Service");
 * calc.init();
 * Message reply = calc.call("add", ctx.factory().newMessage("Calculator.AddRequest")
 *         .setField(...).build());
 * }</pre>
 *
 * The proxy may be constructed with an instance name ({@code Combat.Player.player6})
 * for a service registered under one: requests route to
 * {@code REQUEST.<instance name>.<method>}, and the envelope names the contract
 * method, which is what the service validates against.
 */
public class ServiceProxy {
    private final Context context;
    private final String serviceName;
    private volatile String contract;
    private volatile Set<String> methods = Set.of();
    private volatile Set<String> streaming = Set.of();
    private volatile boolean initialized;

    public ServiceProxy(Context context, String serviceName) {
        this.context = context;
        this.serviceName = serviceName;
    }

    public String serviceName() {
        return serviceName;
    }

    public String contractServiceName() {
        return contract;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public List<String> methods() {
        return new ArrayList<>(methods);
    }

    public boolean isStreaming(String method) {
        return streaming.contains(method);
    }

    private String resolveContract() {
        MessageFactory factory = context.factory();
        String candidate = serviceName;
        while (true) {
            if (factory.hasService(candidate)) {
                if (!candidate.equals(serviceName)) {
                    // Said out loud, because trimming is a guess.
                    Logger.info("service proxy '" + serviceName + "' resolved to contract '" + candidate
                            + "'; requests will route to REQUEST." + serviceName + ".*");
                }
                return candidate;
            }
            int cut = candidate.lastIndexOf('.');
            if (cut <= 0) {
                throw new InvalidServiceNameError("no service in the schema matches '" + serviceName
                        + "' or any prefix of it; the schema must declare the service this proxy addresses");
            }
            candidate = candidate.substring(0, cut);
        }
    }

    /**
     * Resolve the contract and the methods it declares.
     *
     * @throws InvalidServiceNameError when no known service matches the name or a prefix of it
     * @throws AlreadyInitializedError on a second call
     */
    public synchronized void init() {
        if (initialized) {
            Logger.error("already initialized service proxy " + serviceName);
            throw new AlreadyInitializedError();
        }
        String resolved = resolveContract();
        ServiceDescriptor service = context.factory().service(resolved);
        Set<String> all = new HashSet<>();
        Set<String> streams = new HashSet<>();
        for (MethodDescriptor m : service.getMethods()) {
            all.add(m.getName());
            if (m.isServerStreaming()) streams.add(m.getName());
        }
        contract = resolved;
        methods = all;
        streaming = streams;
        initialized = true;
    }

    private void requireMethod(String method, boolean stream) {
        if (!initialized) throw new NotInitializedError("service proxy " + serviceName + " is not initialized");
        if (!methods.contains(method)) {
            throw new UnknownMethodError("service '" + contract + "' declares no method '" + method + "'");
        }
        if (stream != streaming.contains(method)) {
            throw new InvalidRequestError(contract + "." + method + " is "
                    + (stream ? "unary; use call()" : "server-streaming; use callStream()"));
        }
    }

    private byte[] buildRequest(String fullMethod, MessageOrBuilder request, String actor) {
        try {
            return context.factory().buildRequest(fullMethod, request, actor);
        } catch (RuntimeException e) {
            // Type and error only: the request is application data.
            Logger.error("failed building request for " + fullMethod + ": " + e.getMessage());
            throw new InvalidRequestError("failed parsing message: " + e.getMessage());
        }
    }

    // ---- unary -------------------------------------------------------------------------

    /** Call a unary method; the reply is a dynamic message of the method's response type. */
    public Message call(String method, MessageOrBuilder request) {
        return call(method, request, CallOptions.DEFAULT);
    }

    public Message call(String method, MessageOrBuilder request, CallOptions options) {
        return Connection.join(callAsync(method, request, options));
    }

    public CompletableFuture<Message> callAsync(String method, MessageOrBuilder request, CallOptions options) {
        String full = contract + "." + method;
        return callTypedAsync(method, request, options, data -> context.factory().decodeMessage(
                context.factory().method(full).getOutputType().getFullName(), data),
                () -> context.factory().newMessage(context.factory().method(full).getOutputType().getFullName())
                        .build());
    }

    /**
     * Call a unary method, decoding the reply with {@code decode}. A service error
     * fails the future with {@link RemoteError}; a delivery failure with its
     * {@link PublishError} unchanged. With {@code rpc} false the future completes
     * with {@code empty} once the request is confirmed.
     */
    public <R> CompletableFuture<R> callTypedAsync(String method, MessageOrBuilder request, CallOptions options,
                                                   Function<byte[], R> decode, java.util.function.Supplier<R> empty) {
        CallOptions o = options == null ? CallOptions.DEFAULT : options;
        byte[] content;
        try {
            requireMethod(method, false);
            content = buildRequest(contract + "." + method, request, o.actor());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        String full = contract + "." + method;
        String routingKey = "REQUEST." + serviceName + "." + method;
        return context.publishMessageAsync(content, routingKey, o).thenApply(reply -> {
            if (!o.rpc()) {
                Logger.debug("non-rpc call to " + full + " confirmed");
                return empty.get();
            }
            return decode.apply(resultPayload(full, reply));
        });
    }

    /** Wait for a call, rethrowing its failure as it was raised. For generated proxies' blocking methods. */
    protected static <T> T await(CompletableFuture<T> future) {
        return Connection.join(future);
    }

    /** Typed decode with a generated message's parser, checking its custom-type values. */
    public static <R extends Message> Function<byte[], R> parserDecoder(String fullMethod, Parser<R> parser) {
        return data -> {
            try {
                R value = parser.parseFrom(data);
                CustomTypes.validate(value);
                return value;
            } catch (InvalidProtocolBufferException | CustomTypeRangeError e) {
                throw new InvalidResponseError("failed parsing result for " + fullMethod + ": " + e.getMessage());
            }
        };
    }

    /**
     * The result payload of a reply, or the service's error as a {@link RemoteError}.
     */
    static byte[] resultPayload(String fullMethod, byte[] reply) {
        Envelopes.Response response;
        try {
            response = MessageFactory.decodeResponse(reply);
        } catch (Envelopes.MalformedException e) {
            throw new InvalidResponseError("failed parsing result for " + fullMethod);
        }
        if (response.error() != null) {
            Envelopes.Error e = response.error();
            throw new RemoteError(e.message(), e.code(), e.method());
        }
        return response.result().data();
    }

    // ---- streaming ---------------------------------------------------------------------

    /** Call a server-streaming method; each chunk is a dynamic message of the response type. */
    public ProtobusStream<Message> callStream(String method, MessageOrBuilder request, StreamOptions options) {
        String full = contract + "." + method;
        return callStreamTyped(method, request, options, data -> context.factory().decodeMessage(
                context.factory().method(full).getOutputType().getFullName(), data));
    }

    public <R> ProtobusStream<R> callStreamTyped(String method, MessageOrBuilder request, StreamOptions options,
                                                 Function<byte[], R> decode) {
        StreamOptions o = options == null ? StreamOptions.DEFAULT : options;
        String full = contract + "." + method;
        byte[] content;
        try {
            requireMethod(method, true);
            content = buildRequest(full, request, o.actor());
        } catch (InvalidRequestError e) {
            // Surfaced from the first hasNext(), inside the caller's try around the loop.
            return ProtobusStream.failed(e);
        }
        MessageDispatcher.ChunkStream chunks =
                context.publishStreamingMessage(content, "REQUEST." + serviceName + "." + method, o);
        return new ProtobusStream<>(chunks, chunk -> {
            Envelopes.Response response;
            try {
                response = MessageFactory.decodeResponse(chunk);
            } catch (Envelopes.MalformedException e) {
                throw new InvalidResponseError("failed parsing streaming chunk for " + full);
            }
            // A terminal chunk may carry an error instead of a result.
            if (response.error() != null) {
                Envelopes.Error e = response.error();
                throw new RemoteError(e.message(), e.code(), e.method());
            }
            return decode.apply(response.result().data());
        });
    }
}
