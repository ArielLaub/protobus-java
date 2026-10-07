package io.github.ariellaub.protobus.codegen;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import java.util.Set;

/** Java names for protobuf declarations, by protoc's own rules for --java_out. */
final class Names {
    private Names() {}

    static final Set<String> JAVA_KEYWORDS = Set.of(
            "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char", "class", "const", "continue",
            "default", "do", "double", "else", "enum", "extends", "final", "finally", "float", "for", "goto", "if",
            "implements", "import", "instanceof", "int", "interface", "long", "native", "new", "package", "private",
            "protected", "public", "return", "short", "static", "strictfp", "super", "switch", "synchronized",
            "this", "throw", "throws", "transient", "try", "void", "volatile", "while", "true", "false", "null",
            "var", "record", "yield", "sealed", "permits", "_");

    /**
     * Members of the generated classes' supertypes (MessageService, RunnableService,
     * ServiceProxy, Object) that an rpc must not shadow.
     */
    static final Set<String> RESERVED_MEMBERS = Set.of(
            "init", "close", "call", "callAsync", "callStream", "callTypedAsync", "callStreamTyped", "serviceName",
            "contractServiceName", "methods", "isStreaming", "isInitialized", "context", "options", "schema",
            "publishEvent", "subscribeEvent", "stopConsuming", "cleanup", "registerUnary", "registerStream",
            "registerMethod", "registerStreamingMethod", "start", "requestShutdown", "awaitShutdown", "await",
            "parserDecoder", "resultPayload", "proxy", "newProxy", "initProxy", "getDescriptor", "equals",
            "hashCode", "toString", "getClass", "notify", "notifyAll", "wait", "clone", "finalize");

    /** The Java identifier for an rpc: its name, with a trailing underscore when it would collide. */
    static String methodIdentifier(String rpc) {
        return JAVA_KEYWORDS.contains(rpc) || RESERVED_MEMBERS.contains(rpc) ? rpc + "_" : rpc;
    }

    static String javaPackage(FileDescriptor file) {
        String p = file.getOptions().hasJavaPackage() ? file.getOptions().getJavaPackage() : file.getPackage();
        return p == null ? "" : p;
    }

    /** protoc's UnderscoresToCamelCase(name, capitalizeFirst = true). */
    static String camel(String name) {
        StringBuilder out = new StringBuilder();
        boolean capNext = true;
        for (char c : name.toCharArray()) {
            if (c >= 'a' && c <= 'z') {
                out.append(capNext ? Character.toUpperCase(c) : c);
                capNext = false;
            } else if (c >= 'A' && c <= 'Z') {
                out.append(c);
                capNext = false;
            } else if (c >= '0' && c <= '9') {
                out.append(c);
                capNext = true;
            } else {
                capNext = true;
            }
        }
        return out.toString();
    }

    /** The outer class protoc generates for a file. */
    static String outerClass(FileDescriptor file) {
        if (file.getOptions().hasJavaOuterClassname()) return file.getOptions().getJavaOuterClassname();
        String base = file.getName();
        int slash = base.lastIndexOf('/');
        if (slash >= 0) base = base.substring(slash + 1);
        if (base.endsWith(".protodevel")) base = base.substring(0, base.length() - ".protodevel".length());
        else if (base.endsWith(".proto")) base = base.substring(0, base.length() - ".proto".length());
        String name = camel(base);
        return conflicts(file, name) ? name + "OuterClass" : name;
    }

    private static boolean conflicts(FileDescriptor file, String name) {
        for (EnumDescriptor e : file.getEnumTypes()) if (e.getName().equals(name)) return true;
        for (ServiceDescriptor s : file.getServices()) if (s.getName().equals(name)) return true;
        for (Descriptor d : file.getMessageTypes()) if (conflicts(d, name)) return true;
        return false;
    }

    private static boolean conflicts(Descriptor d, String name) {
        if (d.getName().equals(name)) return true;
        for (EnumDescriptor e : d.getEnumTypes()) if (e.getName().equals(name)) return true;
        for (Descriptor n : d.getNestedTypes()) if (conflicts(n, name)) return true;
        return false;
    }

    private static String prefix(FileDescriptor file) {
        String pkg = javaPackage(file);
        String head = pkg.isEmpty() ? "" : pkg + ".";
        return file.getOptions().getJavaMultipleFiles() ? head : head + outerClass(file) + ".";
    }

    /** The fully-qualified Java class of a message, as protoc names it. */
    static String messageClass(Descriptor d) {
        StringBuilder nested = new StringBuilder(d.getName());
        for (Descriptor p = d.getContainingType(); p != null; p = p.getContainingType()) {
            nested.insert(0, p.getName() + ".");
        }
        // A nested type always lives inside its top-level message, which is a file
        // of its own only with java_multiple_files.
        return prefix(d.getFile()) + nested;
    }

    /** The class holding a file's descriptor: its outer class. */
    static String descriptorHolder(FileDescriptor file) {
        String pkg = javaPackage(file);
        return (pkg.isEmpty() ? "" : pkg + ".") + outerClass(file);
    }
}
