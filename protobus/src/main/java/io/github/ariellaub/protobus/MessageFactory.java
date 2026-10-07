package io.github.ariellaub.protobus;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import io.github.ariellaub.protobus.internal.Envelopes;
import io.github.ariellaub.protobus.types.ProtobusTypes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The schemas a context knows, and the encoding of requests, replies and events.
 *
 * Generated code registers its own schema ({@link #register(FileDescriptor)}), so
 * a program built with the protobus code generator needs nothing loaded at
 * runtime. For the dynamic API ({@link ServiceProxy} by name, with
 * {@link DynamicMessage}s), load compiled descriptor sets: {@code protoc
 * --include_imports --descriptor_set_out=...}, or the {@code .binpb} files the
 * protobus code generator writes beside its sources. Java has no runtime
 * {@code .proto} parser, which is why this factory reads descriptor sets where the
 * other ports read schema text.
 *
 * Thread-safe.
 */
public final class MessageFactory {
    /** Extensions of the descriptor-set files {@link #init} picks up in a directory. */
    public static final List<String> DESCRIPTOR_SET_EXTENSIONS = List.of(".binpb", ".desc", ".pb", ".protoset");

    private final Map<String, FileDescriptor> files = new LinkedHashMap<>();
    private final Map<String, Descriptor> messages = new HashMap<>();
    private final Map<String, ServiceDescriptor> services = new HashMap<>();

    public MessageFactory() {
        register(ProtobusTypes.getDescriptor());
    }

    // ---- loading ---------------------------------------------------------------------

    /**
     * Load every descriptor set under the given files or directories (recursively,
     * by {@link #DESCRIPTOR_SET_EXTENSIONS}).
     */
    public void init(List<String> locations) {
        if (locations == null) return;
        List<Path> found = new ArrayList<>();
        for (String location : locations) {
            Path root = Path.of(location);
            if (Files.isDirectory(root)) {
                try (Stream<Path> walk = Files.walk(root)) {
                    found.addAll(walk.filter(Files::isRegularFile)
                            .filter(p -> DESCRIPTOR_SET_EXTENSIONS.stream().anyMatch(p.toString()::endsWith))
                            .sorted()
                            .collect(Collectors.toList()));
                } catch (IOException e) {
                    throw new SchemaError("could not scan " + location + ": " + e.getMessage(), e);
                }
            } else if (Files.isRegularFile(root)) {
                found.add(root);
            } else {
                throw new SchemaError("schema location " + location + " does not exist");
            }
        }
        if (!found.isEmpty()) Logger.info("loading " + found.size() + " descriptor set(s)");
        for (Path p : found) loadDescriptorSet(p);
    }

    public void loadDescriptorSet(Path path) {
        try {
            loadDescriptorSet(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new SchemaError("could not read descriptor set " + path + ": " + e.getMessage(), e);
        }
    }

    /**
     * Load a serialized {@code FileDescriptorSet}. It must include the imports of
     * the files it carries ({@code protoc --include_imports}), except
     * {@code protobus/types.proto}, which is always known.
     */
    public void loadDescriptorSet(byte[] serialized) {
        FileDescriptorSet set;
        try {
            set = FileDescriptorSet.parseFrom(serialized);
        } catch (InvalidProtocolBufferException e) {
            throw new SchemaError("not a FileDescriptorSet: " + e.getMessage(), e);
        }
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptorProto f : set.getFileList()) byName.put(f.getName(), f);
        Map<String, FileDescriptor> built = new HashMap<>();
        for (String name : byName.keySet()) build(name, byName, built, new ArrayList<>());
    }

    private FileDescriptor build(String name, Map<String, FileDescriptorProto> byName,
                                 Map<String, FileDescriptor> built, List<String> stack) {
        FileDescriptor done = built.get(name);
        if (done != null) return done;
        synchronized (this) {
            FileDescriptor known = files.get(name);
            if (known != null) {
                built.put(name, known);
                return known;
            }
        }
        FileDescriptorProto proto = byName.get(name);
        if (proto == null) {
            throw new SchemaError("descriptor set is missing " + name
                    + ", imported by " + (stack.isEmpty() ? "?" : stack.get(stack.size() - 1))
                    + "; build it with protoc --include_imports");
        }
        if (stack.contains(name)) throw new SchemaError("import cycle through " + name);
        stack.add(name);
        List<FileDescriptor> deps = new ArrayList<>();
        for (String dep : proto.getDependencyList()) deps.add(build(dep, byName, built, stack));
        stack.remove(stack.size() - 1);
        try {
            FileDescriptor fd = FileDescriptor.buildFrom(proto, deps.toArray(new FileDescriptor[0]));
            register(fd);
            built.put(name, fd);
            return fd;
        } catch (DescriptorValidationException e) {
            throw new SchemaError("invalid schema " + name + ": " + e.getMessage(), e);
        }
    }

    /**
     * Register a compiled schema and, first, everything it imports. Idempotent per
     * file name: generated code registers its schema each time a service or proxy
     * is built.
     *
     * @throws SchemaError when the file declares a name another registered file
     *     already declares
     */
    public synchronized void register(FileDescriptor file) {
        if (files.containsKey(file.getName())) return;
        for (FileDescriptor dep : file.getDependencies()) register(dep);
        List<Descriptor> newMessages = new ArrayList<>();
        for (Descriptor d : file.getMessageTypes()) collect(d, newMessages);
        for (Descriptor d : newMessages) {
            Descriptor existing = messages.get(d.getFullName());
            if (existing != null) {
                throw new SchemaError("type " + d.getFullName() + " in " + file.getName()
                        + " is already declared by " + existing.getFile().getName());
            }
        }
        for (ServiceDescriptor s : file.getServices()) {
            ServiceDescriptor existing = services.get(s.getFullName());
            if (existing != null) {
                throw new SchemaError("service " + s.getFullName() + " in " + file.getName()
                        + " is already declared by " + existing.getFile().getName());
            }
        }
        files.put(file.getName(), file);
        for (Descriptor d : newMessages) messages.put(d.getFullName(), d);
        for (ServiceDescriptor s : file.getServices()) services.put(s.getFullName(), s);
        Logger.debug("registered schema " + file.getName());
    }

    private static void collect(Descriptor d, List<Descriptor> out) {
        out.add(d);
        for (Descriptor nested : d.getNestedTypes()) collect(nested, out);
    }

    // ---- lookup ----------------------------------------------------------------------

    public synchronized boolean hasService(String fullName) {
        return services.containsKey(fullName);
    }

    public synchronized boolean hasType(String fullName) {
        return messages.containsKey(fullName);
    }

    public synchronized ServiceDescriptor service(String fullName) {
        ServiceDescriptor s = services.get(fullName);
        if (s == null) throw new InvalidServiceNameError("no such service " + fullName);
        return s;
    }

    /** The message type of that full name. */
    public synchronized Descriptor type(String fullName) {
        Descriptor d = messages.get(fullName);
        if (d == null) throw new SchemaError("no such message type " + fullName);
        return d;
    }

    /** Method names a service declares, in declaration order. */
    public List<String> getServiceMethodNames(String serviceFullName) {
        return service(serviceFullName).getMethods().stream().map(MethodDescriptor::getName)
                .collect(Collectors.toList());
    }

    /** {@code <package>.<Service>.<method>} split from the right: service, then method. */
    public record MethodName(String serviceName, String methodName) {}

    /**
     * Split a fully-qualified method name. Parsed from the right, so a dotted
     * package ({@code com.example.Calc.add}) splits into service
     * {@code com.example.Calc} and method {@code add}.
     */
    public static MethodName splitMethodName(String fullName) {
        int i = fullName == null ? -1 : fullName.lastIndexOf('.');
        if (i <= 0 || i == fullName.length() - 1) {
            throw new InvalidMethodNameError("'" + fullName
                    + "' is not a fully-qualified method name (<package>.<Service>.<method>)");
        }
        return new MethodName(fullName.substring(0, i), fullName.substring(i + 1));
    }

    public synchronized MethodDescriptor method(String fullName) {
        MethodName n = splitMethodName(fullName);
        ServiceDescriptor s = services.get(n.serviceName());
        if (s == null) throw new UnknownMethodError("no such service '" + n.serviceName() + "'");
        MethodDescriptor m = s.findMethodByName(n.methodName());
        if (m == null) {
            throw new UnknownMethodError("service '" + n.serviceName() + "' declares no method '" + n.methodName() + "'");
        }
        return m;
    }

    /** Whether the method is declared server-streaming. Unknown methods are treated as unary. */
    public boolean isStreamingMethod(String fullName) {
        try {
            return method(fullName).isServerStreaming();
        } catch (ProtobusException e) {
            Logger.debug("isStreamingMethod(" + fullName + "): treating as unary (" + e.getMessage() + ")");
            return false;
        }
    }

    /** An empty dynamic message of that type, for the dynamic API. */
    public DynamicMessage.Builder newMessage(String typeFullName) {
        return DynamicMessage.newBuilder(type(typeFullName));
    }

    // ---- messages --------------------------------------------------------------------

    /** Serialize a message after checking its custom-type values. */
    public static byte[] encodeMessage(MessageOrBuilder message) {
        CustomTypes.validate(message);
        Message built = message instanceof Message.Builder ? ((Message.Builder) message).build() : (Message) message;
        return built.toByteArray();
    }

    /** Decode bytes as a dynamic message of that type, checking its custom-type values. */
    public DynamicMessage decodeMessage(String typeFullName, byte[] data) {
        if (typeFullName == null || typeFullName.isEmpty()) {
            throw new MessageTypeRequiredError("message type required");
        }
        Descriptor type = type(typeFullName);
        try {
            DynamicMessage m = DynamicMessage.parseFrom(type, data);
            CustomTypes.validate(m);
            return m;
        } catch (InvalidProtocolBufferException e) {
            Logger.error("error decoding message " + typeFullName + " (" + data.length + " bytes)");
            throw new SchemaError("could not decode " + typeFullName + ": " + e.getMessage(), e);
        }
    }

    private static void requireType(MessageOrBuilder message, Descriptor expected, String what) {
        String actual = message.getDescriptorForType().getFullName();
        if (!actual.equals(expected.getFullName())) {
            throw new InvalidRequestError(what + " must be " + expected.getFullName() + ", got " + actual);
        }
    }

    /** Encode a request for {@code methodFullName}: its payload wrapped in a RequestContainer. */
    public byte[] buildRequest(String methodFullName, MessageOrBuilder request, String actor) {
        MethodDescriptor m = method(methodFullName);
        requireType(request, m.getInputType(), "the request of " + methodFullName);
        return Envelopes.encodeRequest(new Envelopes.Request(methodFullName, actor, encodeMessage(request)));
    }

    /**
     * Decode the request envelope only, leaving the payload undecoded, so a
     * service can check which method the envelope names before it interprets the
     * bytes.
     */
    public Envelopes.Request decodeRequestEnvelope(byte[] data) {
        return Envelopes.decodeRequest(data);
    }

    /** Decode a request payload against the declared request type of {@code methodFullName}. */
    public DynamicMessage decodeRequestPayload(String methodFullName, byte[] payload) {
        return decodeMessage(method(methodFullName).getInputType().getFullName(), payload);
    }

    /** Encode a successful reply. */
    public byte[] buildResponse(String methodFullName, MessageOrBuilder result) {
        MethodDescriptor m = method(methodFullName);
        requireType(result, m.getOutputType(), "the result of " + methodFullName);
        return buildEncodedResponse(methodFullName, encodeMessage(result));
    }

    /** Encode a successful reply whose payload is already serialized. */
    public static byte[] buildEncodedResponse(String methodFullName, byte[] payload) {
        return Envelopes.encodeResponse(new Envelopes.Response(new Envelopes.Result(methodFullName, payload), null));
    }

    /**
     * Encode an error reply. No method lookup: an error carries the method only as
     * a label, and resolving it would make a failure that is about an unknown
     * method impossible to report.
     */
    public static byte[] buildErrorResponse(String methodFullName, Throwable error) {
        String code = Errors.codeOf(error);
        return buildErrorResponse(methodFullName, Errors.messageOf(error), code);
    }

    public static byte[] buildErrorResponse(String methodFullName, String message, String code) {
        return Envelopes.encodeResponse(new Envelopes.Response(null,
                new Envelopes.Error(methodFullName == null ? "" : methodFullName, message == null ? "" : message,
                        code == null ? "" : code)));
    }

    /** Decode a reply envelope. Its payload stays serialized; see {@link #decodeResultPayload}. */
    public static Envelopes.Response decodeResponse(byte[] data) {
        return Envelopes.decodeResponse(data);
    }

    /** Decode a result payload as the declared response type of its method. */
    public DynamicMessage decodeResultPayload(Envelopes.Result result) {
        return decodeMessage(method(result.method()).getOutputType().getFullName(), result.data());
    }

    /** Encode an event: {@code type} is the payload's full message name. */
    public static byte[] buildEvent(String type, MessageOrBuilder content, String topic) {
        return Envelopes.encodeEvent(new Envelopes.Event(type, topic, encodeMessage(content)));
    }

    public static Envelopes.Event decodeEventEnvelope(byte[] data) {
        return Envelopes.decodeEvent(data);
    }
}
