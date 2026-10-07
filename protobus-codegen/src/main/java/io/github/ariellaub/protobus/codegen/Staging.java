package io.github.ariellaub.protobus.codegen;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Copies shared schemas into a staging directory and adds what Java needs.
 *
 * Shared protobus schemas carry no Java options and use the custom types without
 * importing them. Each staged copy gains, at its end (so line numbers in protoc's
 * errors still match the original), {@code import "protobus/types.proto";} when it
 * uses {@code bigint} or {@code timestamp} without declaring them, the import of
 * each declared custom type it uses, {@code option java_multiple_files = true;}
 * unless it sets that option itself, and, for a capitalised package with no
 * {@code java_package}, that package lowercased as its Java package. The original
 * files are never touched.
 */
final class Staging {
    private Staging() {}

    static final String TYPES = "protobus/types.proto";
    static final Set<String> BUILTIN = Set.of("bigint", "timestamp");

    /** A custom type of the user's: {@code message NAME { optional WIRE value = 1; }}. */
    record CustomType(String name, String wire) {
        static final Set<String> WIRES = Set.of("bytes", "int64", "uint64", "string", "int32", "uint32", "double");

        static CustomType parse(String spec) {
            int colon = spec.indexOf(':');
            if (colon <= 0) throw new IllegalArgumentException("--custom-type wants NAME:WIRE, got " + spec);
            String name = spec.substring(0, colon);
            String wire = spec.substring(colon + 1);
            if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) throw new IllegalArgumentException("bad custom type name " + name);
            if (!WIRES.contains(wire)) {
                throw new IllegalArgumentException("custom type " + name + ": wire type must be one of " + WIRES);
            }
            if (BUILTIN.contains(name)) throw new IllegalArgumentException(name + " is a built-in custom type");
            return new CustomType(name, wire);
        }

        String file() {
            return "protobus/custom/" + name + ".proto";
        }
    }

    record Staged(Path root, List<String> schemas, List<String> generatedTypes) {}

    /** Text with comments and string literals blanked, so names inside them do not count. */
    static String code(String proto) {
        return strip(proto, true);
    }

    /** Text with comments blanked, string literals kept: for reading import paths. */
    static String withoutComments(String proto) {
        return strip(proto, false);
    }

    private static String strip(String proto, boolean blankStrings) {
        StringBuilder out = new StringBuilder(proto.length());
        int i = 0;
        int n = proto.length();
        while (i < n) {
            char c = proto.charAt(i);
            if (c == '/' && i + 1 < n && proto.charAt(i + 1) == '/') {
                while (i < n && proto.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && i + 1 < n && proto.charAt(i + 1) == '*') {
                int end = proto.indexOf("*/", i + 2);
                int stop = end < 0 ? n : end + 2;
                while (i < stop) {
                    out.append(proto.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
            } else if (c == '"' || c == '\'') {
                // A literal is copied (or blanked) whole, so a "//" inside one is
                // never read as a comment.
                out.append(c);
                i++;
                while (i < n && proto.charAt(i) != c) {
                    if (proto.charAt(i) == '\\' && i + 1 < n) {
                        out.append(blankStrings ? "  " : proto.substring(i, i + 2));
                        i += 2;
                        continue;
                    }
                    out.append(blankStrings ? ' ' : proto.charAt(i));
                    i++;
                }
                if (i < n) {
                    out.append(c);
                    i++;
                }
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** Whether the schema uses {@code type} as a field, map value or rpc type. */
    static boolean uses(String code, String type) {
        Pattern p = Pattern.compile("(?<![\\w.])\\.?" + Pattern.quote(type)
                + "(?=\\s+[A-Za-z_]\\w*\\s*=|\\s*>|\\s*\\))");
        return p.matcher(code).find();
    }

    static boolean declares(String code, String type) {
        return Pattern.compile("\\bmessage\\s+" + Pattern.quote(type) + "\\b").matcher(code).find();
    }

    static boolean hasOption(String code, String option) {
        return Pattern.compile("\\boption\\s+" + Pattern.quote(option) + "\\s*=").matcher(code).find();
    }

    static String protoPackage(String code) {
        Matcher m = Pattern.compile("\\bpackage\\s+([A-Za-z_][\\w.]*)\\s*;").matcher(code);
        return m.find() ? m.group(1) : null;
    }

    static boolean imports(String code, String file) {
        Matcher m = Pattern.compile("\\bimport\\s+(?:public\\s+|weak\\s+)?\"([^\"]*)\"").matcher(code);
        while (m.find()) if (m.group(1).equals(file)) return true;
        return false;
    }

    /** The staged text of one schema. */
    static String stage(String proto, List<CustomType> custom) {
        String code = code(proto);
        String importable = withoutComments(proto);
        StringBuilder extra = new StringBuilder();
        boolean builtin = false;
        for (String t : BUILTIN) builtin |= uses(code, t) && !declares(code, t);
        if (builtin && !imports(importable, TYPES)) extra.append("import \"").append(TYPES).append("\";\n");
        for (CustomType t : custom) {
            if (uses(code, t.name()) && !declares(code, t.name()) && !imports(importable, t.file())) {
                extra.append("import \"").append(t.file()).append("\";\n");
            }
        }
        if (!hasOption(code, "java_multiple_files")) extra.append("option java_multiple_files = true;\n");
        // A capitalised proto package (package Calculator, in Calculator.proto) is
        // common in shared schemas, and as a Java package it breaks: protoc's outer
        // class Calculator.Calculator shadows the package in every qualified
        // reference, its own code included. Java packages are lowercase anyway.
        String pkg = protoPackage(code);
        if (pkg != null && !pkg.equals(pkg.toLowerCase(java.util.Locale.ROOT)) && !hasOption(code, "java_package")) {
            extra.append("option java_package = \"").append(pkg.toLowerCase(java.util.Locale.ROOT)).append("\";\n");
        }
        if (extra.length() == 0) return proto;
        String sep = proto.endsWith("\n") ? "" : "\n";
        return proto + sep + "// Added by protobus-codegen to its staged copy.\n" + extra;
    }

    static String customTypeSchema(CustomType t, String javaPackage) {
        return "// A protobus custom type, declared at the root like bigint and timestamp.\n"
                + "syntax = \"proto3\";\n\n"
                + "option java_package = \"" + javaPackage + "\";\n"
                + "option java_multiple_files = true;\n"
                // protoc would name the outer class Uuid beside the message class
                // uuid: one file on a case-insensitive file system.
                + "option java_outer_classname = \"ProtobusCustom" + Names.camel(t.name()) + "\";\n\n"
                + "message " + t.name() + " {\n  optional " + t.wire() + " value = 1;\n}\n";
    }

    /**
     * Stage every {@code .proto} under {@code dirs} into {@code root}, keyed by its
     * path relative to its directory, plus protobus/types.proto and the custom
     * types' schemas.
     */
    static Staged stage(List<Path> dirs, Path root, List<CustomType> custom, String customPackage) throws IOException {
        Map<String, Path> found = new LinkedHashMap<>();
        for (Path dir : dirs) {
            if (!Files.isDirectory(dir)) throw new IOException("proto directory " + dir + " does not exist");
            try (var walk = Files.walk(dir)) {
                for (Path p : (Iterable<Path>) walk.filter(Files::isRegularFile)
                        .filter(f -> f.getFileName().toString().endsWith(".proto")).sorted()::iterator) {
                    String rel = dir.relativize(p).toString().replace('\\', '/');
                    if (rel.equals(TYPES) || rel.startsWith("protobus/custom/")) continue;
                    Path previous = found.putIfAbsent(rel, p);
                    if (previous != null) {
                        throw new IOException(rel + " is in more than one proto directory: " + previous + " and " + p);
                    }
                }
            }
        }
        List<String> schemas = new ArrayList<>();
        for (Map.Entry<String, Path> e : found.entrySet()) {
            Path target = root.resolve(e.getKey());
            Files.createDirectories(target.getParent());
            Files.writeString(target, stage(Files.readString(e.getValue(), StandardCharsets.UTF_8), custom),
                    StandardCharsets.UTF_8);
            schemas.add(e.getKey());
        }
        Path types = root.resolve(TYPES);
        Files.createDirectories(types.getParent());
        try (InputStream in = Staging.class.getClassLoader().getResourceAsStream(TYPES)) {
            if (in == null) throw new IOException("the generator's copy of " + TYPES + " is missing");
            Files.write(types, in.readAllBytes());
        }
        List<String> generatedTypes = new ArrayList<>();
        for (CustomType t : custom) {
            Path p = root.resolve(t.file());
            Files.createDirectories(p.getParent());
            Files.writeString(p, customTypeSchema(t, customPackage), StandardCharsets.UTF_8);
            generatedTypes.add(t.file());
        }
        return new Staged(root, schemas, generatedTypes);
    }
}
