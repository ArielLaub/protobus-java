package io.github.ariellaub.protobus;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Internal;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import io.github.ariellaub.protobus.internal.Envelopes;
import io.github.ariellaub.protobus.internal.Threads;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The base class every RPC service extends.
 *
 * A service is named on the bus by {@link #serviceName()}, normally its .proto
 * {@code <package>.<Service>}. It owns one durable queue of that name, bound to
 * {@code REQUEST.<serviceName>.*}, which every replica consumes from, so the
 * broker balances load and fails over. It also owns {@code <serviceName>.Events}
 * for the events it subscribes to.
 *
 * The protobus code generator derives a {@code <Service>Protobus.Base} class from
 * this one with a method per rpc; implement those. Without codegen, extend this
 * class directly, return the schema from {@link #schema()} (or load it into the
 * context's factory), and register handlers over dynamic messages with
 * {@link #registerMethod}.
 *
 * A handler's exception decides what happens to the request. A
 * {@link HandledError} is an answer: it reaches the caller with its code and is
 * never retried. Anything else, or a processing timeout, is a failure: the
 * request is retried through the service's retry queue and dead-lettered once
 * retries are spent, and the caller is answered then.
 */
public abstract class MessageService {
    /** A unary handler. */
    @FunctionalInterface
    public interface UnaryMethod<Q extends Message, R extends MessageOrBuilder> {
        R call(Q request, CallContext context) throws Exception;
    }

    /** A server-streaming handler. */
    @FunctionalInterface
    public interface StreamMethod<Q extends Message, R extends MessageOrBuilder> {
        void call(Q request, StreamWriter<R> out, CallContext context) throws Exception;
    }

    private static final class MethodEntry {
        boolean streaming;
        /** Decodes the payload; null decodes dynamically against the contract. */
        Message prototype;
        UnaryMethod<Message, MessageOrBuilder> unary;
        StreamMethod<Message, MessageOrBuilder> stream;
    }

    protected final Context context;
    private final MessageServiceOptions options;
    private final MessageListener listener;
    private final EventListener eventListener;
    private final CancelListener cancelListener;

    private final Object lock = new Object();
    private final Map<String, MethodEntry> methods = new HashMap<>();
    /**
     * The service as its .proto declares it, which is not always serviceName():
     * instances sharing one schema are addressed under distinct runtime names
     * ({@code Combat.Player.player6} serving the contract {@code Combat.Player}).
     */
    private volatile String contractServiceName;
    private volatile Set<String> declaredMethods = Set.of();

    protected MessageService(Context context) {
        this(context, MessageServiceOptions.DEFAULT);
    }

    protected MessageService(Context context, MessageServiceOptions options) {
        this.context = context;
        this.options = options == null ? MessageServiceOptions.DEFAULT : options;
        this.listener = new MessageListener(context.connection(), this.options.lateAck(),
                this.options.maxConcurrent(), this.options.retry(), this.options.processingTimeoutMs(),
                this.options.maxPriority());
        this.listener.buildErrorReply = this::buildTimeoutReply;
        this.eventListener = new EventListener(context.connection(), context.factory(), this.options.eventRetry());
        this.cancelListener = new CancelListener(context.connection());
    }

    /**
     * The service's name on the bus. Several instances can share one schema under
     * distinct names: the contract is found by trimming trailing segments until one
     * names a known service.
     */
    public abstract String serviceName();

    /** The compiled schema declaring the service, registered on init. Generated bases return theirs. */
    protected FileDescriptor schema() {
        return null;
    }

    public Context context() {
        return context;
    }

    public MessageServiceOptions options() {
        return options;
    }

    /** The contract this service serves, once {@link #init()} has resolved it. */
    public String contractServiceName() {
        return contractServiceName;
    }

    // ---- registration ------------------------------------------------------------------

    /** Register a typed unary handler, as generated code does. */
    @SuppressWarnings("unchecked")
    protected <Q extends Message, R extends MessageOrBuilder> void registerUnary(
            String name, Q requestPrototype, UnaryMethod<Q, R> handler) {
        MethodEntry e = new MethodEntry();
        e.prototype = requestPrototype;
        e.unary = (UnaryMethod<Message, MessageOrBuilder>) (UnaryMethod<?, ?>) handler;
        add(name, e);
    }

    /** Register a typed server-streaming handler, as generated code does. */
    @SuppressWarnings("unchecked")
    protected <Q extends Message, R extends MessageOrBuilder> void registerStream(
            String name, Q requestPrototype, StreamMethod<Q, R> handler) {
        MethodEntry e = new MethodEntry();
        e.streaming = true;
        e.prototype = requestPrototype;
        e.stream = (StreamMethod<Message, MessageOrBuilder>) (StreamMethod<?, ?>) handler;
        add(name, e);
    }

    /**
     * Register a dynamic unary handler: the request is a {@link DynamicMessage} of
     * the contract's request type, and the result must be of its response type.
     */
    @SuppressWarnings("unchecked")
    protected void registerMethod(String name, UnaryMethod<DynamicMessage, ? extends MessageOrBuilder> handler) {
        MethodEntry e = new MethodEntry();
        e.unary = (UnaryMethod<Message, MessageOrBuilder>) (UnaryMethod<?, ?>) handler;
        add(name, e);
    }

    @SuppressWarnings("unchecked")
    protected void registerStreamingMethod(String name,
                                           StreamMethod<DynamicMessage, ? extends MessageOrBuilder> handler) {
        MethodEntry e = new MethodEntry();
        e.streaming = true;
        e.stream = (StreamMethod<Message, MessageOrBuilder>) (StreamMethod<?, ?>) handler;
        add(name, e);
    }

    private void add(String name, MethodEntry entry) {
        synchronized (lock) {
            methods.put(name, entry);
        }
    }

    // ---- lifecycle ---------------------------------------------------------------------

    private void registerSchema() {
        FileDescriptor schema = schema();
        if (schema != null) context.factory().register(schema);
    }

    private boolean tryResolveContract() {
        if (contractServiceName != null) return true;
        MessageFactory factory = context.factory();
        String candidate = serviceName();
        while (true) {
            if (factory.hasService(candidate)) {
                declaredMethods = new HashSet<>(factory.getServiceMethodNames(candidate));
                contractServiceName = candidate;
                return true;
            }
            int cut = candidate.lastIndexOf('.');
            if (cut <= 0) return false;
            candidate = candidate.substring(0, cut);
        }
    }

    private void resolveContract() {
        if (!tryResolveContract()) {
            throw new MissingProto("no service in the schema matches '" + serviceName()
                    + "' or any prefix of it; the schema must declare the service this class serves");
        }
    }

    /** Register the schema if needed, declare the queues and start consuming. */
    public void init() {
        try {
            registerSchema();
            resolveContract();
            listener.init(this::onMessage, serviceName());
            eventListener.init(null, serviceName() + ".Events");
            listener.subscribe("REQUEST." + serviceName() + ".*");
            listener.start();
            eventListener.start();
            // Started last: it only matters once requests can arrive.
            cancelListener.start();
        } catch (RuntimeException e) {
            Logger.error("error initializing service " + serviceName() + " - " + e + "\n" + Threads.stackTrace(e));
            closeQuietly();
            throw e;
        }
    }

    /**
     * Stop accepting new requests and events, leaving channels open so work in hand
     * can finish: the first step of a graceful shutdown. Pair it with
     * {@code context.connection().drainInFlight()} before closing anything.
     */
    public void stopConsuming() {
        listener.stopConsuming();
        eventListener.stopConsuming();
        // A drained service has no stream left to cancel.
        cancelListener.close();
    }

    /** Stop consuming and close the service's channels. */
    public void close() {
        stopConsuming();
        closeQuietly();
    }

    private void closeQuietly() {
        for (BaseListener l : new BaseListener[] {listener, eventListener}) {
            try {
                if (l.isInitialized()) l.close();
            } catch (RuntimeException e) {
                Logger.debug("closing " + l.listenerName() + ": " + e.getMessage());
            }
        }
        cancelListener.close();
    }

    // ---- events ------------------------------------------------------------------------

    /** Publish an event of the message's own type, on {@code EVENT.<type>}. */
    public void publishEvent(MessageOrBuilder content) {
        context.publishEvent(content);
    }

    public void publishEvent(String type, MessageOrBuilder content, String topic) {
        context.publishEvent(type, content, topic);
    }

    /**
     * Subscribe this service to events of {@code type} on {@code EVENT.<type>}.
     * Call after {@link #init()}.
     */
    public <T extends Message> void subscribeEvent(Class<T> type, EventHandler<T> handler) {
        subscribeEvent(type, handler, null);
    }

    /** As above, on a topic or pattern of your choosing. */
    public <T extends Message> void subscribeEvent(Class<T> type, EventHandler<T> handler, String topic) {
        eventListener.subscribe(Internal.getDefaultInstance(type), handler, topic);
    }

    /** Subscribe a dynamic handler to events of a type known to the context's factory. */
    public void subscribeEvent(String type, EventHandler<Message> handler, String topic) {
        eventListener.subscribe(type, handler, topic);
    }

    // ---- dispatch ----------------------------------------------------------------------

    private static String lastSegment(String value) {
        int i = value.lastIndexOf('.');
        return i < 0 ? value : value.substring(i + 1);
    }

    /** The core handler for requests made to {@code REQUEST.<serviceName>.*}. */
    private Connection.HandlerResult onMessage(byte[] data, String id, Connection.MessageHandlerContext delivery) {
        resolveContract();
        MessageFactory factory = context.factory();
        String routingKey = delivery.routingKey;

        // Envelope first, payload later: the envelope names the method, and that
        // name selects the schema the payload is read with, so it is checked
        // against this service's contract before the bytes are interpreted.
        Envelopes.Request envelope;
        try {
            envelope = factory.decodeRequestEnvelope(data);
        } catch (Envelopes.MalformedException e) {
            Logger.error("unparseable request envelope on " + serviceName() + " (" + data.length + " bytes, " + id + ")");
            return protocolError(routingKey, "request envelope did not decode");
        }
        Logger.debug("received request " + envelope.method() + " (" + id + ")");

        // A rejection is reported against the method the ROUTING KEY names: the
        // body's name is exactly what is in dispute.
        String contractMethod = contractServiceName + "."
                + (routingKey != null ? lastSegment(routingKey) : lastSegment(envelope.method()));

        // 1. The delivery belongs to THIS service, by the key the broker used.
        // 2. The body asks for the method the routing key names, so a client that
        //    can publish cannot route to one method and have another executed.
        if (routingKey != null) {
            if (!routingKey.startsWith("REQUEST." + serviceName() + ".")) {
                return rejectDispatch(contractMethod,
                        "routing key " + routingKey + " does not belong to service " + serviceName());
            }
            if (!lastSegment(routingKey).equals(lastSegment(envelope.method()))) {
                return rejectDispatch(contractMethod,
                        "request method " + envelope.method() + " contradicts routing key " + routingKey);
            }
        }
        // 3. The body names a method of THIS contract, spelled in full.
        String method;
        try {
            MessageFactory.MethodName parsed = MessageFactory.splitMethodName(envelope.method());
            if (!parsed.serviceName().equals(contractServiceName)) {
                return rejectDispatch(contractMethod,
                        "request method " + envelope.method() + " is not a method of " + contractServiceName);
            }
            method = parsed.methodName();
        } catch (InvalidMethodNameError e) {
            return rejectDispatch(contractMethod,
                    "request method " + envelope.method() + " is not a qualified method name");
        }
        if (!declaredMethods.contains(method)) {
            return rejectDispatch(contractMethod, contractServiceName + " declares no method " + method);
        }

        MethodEntry entry;
        synchronized (lock) {
            entry = methods.get(method);
        }
        String fullMethod = envelope.method();
        if (entry == null) {
            InvalidMethodError error = new InvalidMethodError("invalid service method " + method);
            Logger.error(error.getMessage());
            return Connection.HandlerResult.reply(MessageFactory.buildErrorResponse(fullMethod, error));
        }
        MethodDescriptor descriptor = factory.method(fullMethod);
        if (entry.streaming != descriptor.isServerStreaming()) {
            InvalidMethodError error = new InvalidMethodError("method " + method + " is registered as "
                    + (entry.streaming ? "streaming" : "unary") + " but declared otherwise");
            Logger.error(error.getMessage());
            return Connection.HandlerResult.reply(MessageFactory.buildErrorResponse(fullMethod, error));
        }

        // Validated: the payload can now be read against the contract's schema.
        Message request;
        try {
            request = decodePayload(entry, fullMethod, envelope.data());
        } catch (InvalidProtocolBufferException | SchemaError | CustomTypeRangeError e) {
            // Type name and size only: a payload that failed to decode is still a payload.
            Logger.error("unparseable request payload for " + fullMethod + " (" + envelope.data().length + " bytes, "
                    + id + ")");
            return protocolError(fullMethod, "payload did not decode as the request type of " + fullMethod
                    + (e instanceof CustomTypeRangeError ? ": " + e.getMessage() : ""));
        }
        CallContext call = new CallContext(envelope.actor(), id, fullMethod, delivery);

        if (entry.streaming) {
            return Connection.HandlerResult.stream(sink -> runStream(entry, fullMethod, descriptor, request, call, sink));
        }
        MessageOrBuilder result;
        try {
            result = entry.unary.call(request, call);
        } catch (Throwable error) {
            return handleUnaryError(fullMethod, error, id);
        }
        try {
            checkResult(descriptor, result);
            Logger.debug("sending result " + fullMethod + " (" + id + ")");
            return Connection.HandlerResult.reply(
                    MessageFactory.buildEncodedResponse(fullMethod, MessageFactory.encodeMessage(result)));
        } catch (RuntimeException error) {
            return handleUnaryError(fullMethod, error, id);
        }
    }

    private Message decodePayload(MethodEntry entry, String fullMethod, byte[] payload)
            throws InvalidProtocolBufferException {
        if (entry.prototype == null) return context.factory().decodeRequestPayload(fullMethod, payload);
        Message m = entry.prototype.getParserForType().parseFrom(payload);
        CustomTypes.validate(m);
        return m;
    }

    private static void checkResult(MethodDescriptor descriptor, MessageOrBuilder result) {
        if (result == null) {
            throw new InvalidResultError(descriptor.getFullName() + " returned null");
        }
        String expected = descriptor.getOutputType().getFullName();
        String actual = result.getDescriptorForType().getFullName();
        if (!expected.equals(actual)) {
            throw new InvalidResultError(descriptor.getFullName() + " must return " + expected + ", not " + actual);
        }
    }

    /**
     * Produce a streaming reply. A chunk is a ResponseContainer; a failure, at any
     * point, ends the stream with a terminal error chunk, sanitised like a unary
     * error, which the caller's iteration raises. Streams are not retried.
     */
    private void runStream(MethodEntry entry, String fullMethod, MethodDescriptor descriptor, Message request,
                           CallContext call, Connection.ChunkSink sink) {
        StreamWriter<MessageOrBuilder> writer = new StreamWriter<>() {
            @Override
            public void write(MessageOrBuilder chunk) {
                checkResult(descriptor, chunk);
                sink.write(MessageFactory.buildEncodedResponse(fullMethod, MessageFactory.encodeMessage(chunk)));
            }

            @Override
            public boolean cancelled() {
                return sink.cancelled();
            }
        };
        try {
            entry.stream.call(request, writer, call);
        } catch (Connection.StreamCancelledException e) {
            throw e;
        } catch (Throwable error) {
            if (sink.cancelled()) throw new Connection.StreamCancelledException();
            if (Errors.isHandledError(error)) {
                Logger.warn("handled error in stream " + fullMethod + ": " + Errors.messageOf(error));
            } else {
                Logger.error(Threads.stackTrace(error));
            }
            sink.write(MessageFactory.buildErrorResponse(fullMethod,
                    Errors.sanitizeErrorForClient(error, call.correlationId())));
        }
    }

    /**
     * Answer a message this service could not understand. A ProtocolError, so it is
     * replied to and rejected instead of retried: the same bytes fail the same way
     * on every redelivery.
     */
    private static Connection.HandlerResult protocolError(String label, String reason) {
        return Connection.HandlerResult.reply(MessageFactory.buildErrorResponse(
                label == null || label.isEmpty() ? "unknown" : label, new ProtocolError(reason)));
    }

    private static Connection.HandlerResult rejectDispatch(String contractMethod, String reason) {
        InvalidMethodError error = new InvalidMethodError(reason);
        Logger.error(reason);
        return Connection.HandlerResult.reply(MessageFactory.buildErrorResponse(contractMethod, error));
    }

    /**
     * A {@link HandledError} is expected: it is answered at once, never retried.
     * Anything else is an infrastructure failure, rethrown for the retry ladder with
     * the caller's (sanitised) reply pre-encoded, for the connection to send once
     * the retries are spent.
     */
    private static Connection.HandlerResult handleUnaryError(String method, Throwable error, String correlationId) {
        if (Errors.isHandledError(error)) {
            Logger.warn("handled error in " + method + ": " + Errors.messageOf(error));
            return Connection.HandlerResult.reply(MessageFactory.buildErrorResponse(method, error));
        }
        Logger.error(Threads.stackTrace(error));
        byte[] reply = MessageFactory.buildErrorResponse(method, Errors.sanitizeErrorForClient(error, correlationId));
        throw new Connection.ErrorWithReply(error, reply);
    }

    /** The caller's answer to a processing timeout: PROCESSING_TIMEOUT, once the retries are spent. */
    private byte[] buildTimeoutReply(byte[] content, Throwable error) {
        if (!(error instanceof TimeoutError)) return null;
        String method;
        try {
            method = Envelopes.decodeRequest(content).method();
        } catch (RuntimeException e) {
            method = "unknown";
        }
        return MessageFactory.buildErrorResponse(method, error);
    }

    /** Package-private access for {@link RunnableService}. */
    MessageListener requestListener() {
        return listener;
    }
}
