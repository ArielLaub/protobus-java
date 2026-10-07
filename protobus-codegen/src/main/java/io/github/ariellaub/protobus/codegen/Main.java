package io.github.ariellaub.protobus.codegen;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The protobus code generator.
 *
 * <pre>
 * protobus-java generate --proto-dir DIR... --out DIR [--descriptor-out FILE]
 *                        [--custom-type NAME:WIRE]... [--custom-type-package PKG] [--protoc PATH]
 * </pre>
 *
 * {@code generate} stages the schemas (see {@link Staging}), runs protoc with
 * {@code --java_out}, and writes a {@code <Service>Protobus} class per service.
 * protoc is {@code --protoc}, then {@code $PROTOC}, then the one on the PATH.
 *
 * Run with no arguments, it is a protoc plugin ({@code protoc-gen-protobus-java}):
 * it reads a CodeGeneratorRequest on stdin and writes the protobus classes for the
 * files protoc asks for. Schemas compiled that way import the custom types
 * themselves: {@code import "protobus/types.proto";}, which ships in the protobus
 * jar's resources and in this one.
 */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            plugin();
            return;
        }
        int status;
        try {
            status = run(args, System.out, System.err);
        } catch (IllegalArgumentException e) {
            System.err.println("protobus-java: " + e.getMessage());
            System.err.println(USAGE);
            status = 2;
        } catch (IOException e) {
            System.err.println("protobus-java: " + e.getMessage());
            status = 1;
        }
        System.exit(status);
    }

    static final String USAGE = String.join("\n",
            "usage: protobus-java generate --proto-dir DIR... --out DIR [--descriptor-out FILE]",
            "                              [--custom-type NAME:WIRE]... [--custom-type-package PKG] [--protoc PATH]",
            "       protobus-java            (no arguments: run as a protoc plugin)");

    static int run(String[] args, PrintStream out, PrintStream err) throws IOException {
        if (!args[0].equals("generate")) throw new IllegalArgumentException("unknown command " + args[0]);
        List<Path> dirs = new ArrayList<>();
        List<Staging.CustomType> custom = new ArrayList<>();
        Path outDir = null;
        Path descriptorOut = null;
        String protoc = null;
        String customPackage = "protobus.custom";
        for (int i = 1; i < args.length; i++) {
            String a = args[i];
            if (i + 1 >= args.length) throw new IllegalArgumentException(a + " needs a value");
            String v = args[++i];
            switch (a) {
                case "--proto-dir": dirs.add(Path.of(v)); break;
                case "--out": outDir = Path.of(v); break;
                case "--descriptor-out": descriptorOut = Path.of(v); break;
                case "--custom-type": custom.add(Staging.CustomType.parse(v)); break;
                case "--custom-type-package": customPackage = v; break;
                case "--protoc": protoc = v; break;
                default: throw new IllegalArgumentException("unknown option " + a);
            }
        }
        if (dirs.isEmpty()) throw new IllegalArgumentException("at least one --proto-dir is required");
        if (outDir == null) throw new IllegalArgumentException("--out is required");
        if (protoc == null) protoc = System.getenv("PROTOC");
        if (protoc == null || protoc.isEmpty()) protoc = "protoc";

        Path staging = Files.createTempDirectory("protobus-codegen-");
        try {
            Staging.Staged staged = Staging.stage(dirs, staging, custom, customPackage);
            if (staged.schemas().isEmpty()) {
                err.println("protobus-java: no .proto files under " + dirs);
                return 1;
            }
            Files.createDirectories(outDir);
            Path set = staging.resolve("descriptor-set.binpb");
            List<String> command = new ArrayList<>(List.of(protoc, "-I", staging.toString(),
                    "--java_out=" + outDir, "--descriptor_set_out=" + set, "--include_imports"));
            command.addAll(staged.schemas());
            command.addAll(staged.generatedTypes());
            Process p = new ProcessBuilder(command).directory(staging.toFile()).redirectErrorStream(true).start();
            String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int code;
            try {
                code = p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for protoc");
            }
            if (!output.isBlank()) err.print(output);
            if (code != 0) {
                err.println("protobus-java: protoc failed (exit " + code + ")");
                return 1;
            }
            FileDescriptorSet descriptors = FileDescriptorSet.parseFrom(Files.readAllBytes(set));
            Map<String, FileDescriptor> built = build(descriptors.getFileList());
            int written = 0;
            for (String name : staged.schemas()) {
                for (Generator.Output o : Generator.generate(built.get(name))) {
                    Path target = outDir.resolve(o.path());
                    Files.createDirectories(target.getParent());
                    Files.writeString(target, o.content(), StandardCharsets.UTF_8);
                    written++;
                }
            }
            if (descriptorOut != null) {
                if (descriptorOut.getParent() != null) Files.createDirectories(descriptorOut.getParent());
                Files.write(descriptorOut, descriptors.toByteArray());
            }
            out.println("protobus-java: " + staged.schemas().size() + " schema(s), " + written
                    + " service class(es) written to " + outDir);
            return 0;
        } finally {
            deleteTree(staging);
        }
    }

    static Map<String, FileDescriptor> build(List<FileDescriptorProto> files) throws IOException {
        Map<String, FileDescriptorProto> byName = new HashMap<>();
        for (FileDescriptorProto f : files) byName.put(f.getName(), f);
        Map<String, FileDescriptor> built = new HashMap<>();
        for (FileDescriptorProto f : files) build(f.getName(), byName, built);
        return built;
    }

    private static FileDescriptor build(String name, Map<String, FileDescriptorProto> byName,
                                        Map<String, FileDescriptor> built) throws IOException {
        FileDescriptor done = built.get(name);
        if (done != null) return done;
        FileDescriptorProto proto = byName.get(name);
        if (proto == null) throw new IOException("descriptor for " + name + " is missing");
        List<FileDescriptor> deps = new ArrayList<>();
        for (String dep : proto.getDependencyList()) deps.add(build(dep, byName, built));
        try {
            FileDescriptor fd = FileDescriptor.buildFrom(proto, deps.toArray(new FileDescriptor[0]));
            built.put(name, fd);
            return fd;
        } catch (DescriptorValidationException e) {
            throw new IOException("invalid schema " + name + ": " + e.getMessage(), e);
        }
    }

    /** protoc plugin mode. */
    static void plugin() throws IOException {
        CodeGeneratorRequest request = CodeGeneratorRequest.parseFrom(System.in);
        CodeGeneratorResponse.Builder response = CodeGeneratorResponse.newBuilder()
                .setSupportedFeatures(CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL_VALUE);
        try {
            Map<String, FileDescriptor> built = build(request.getProtoFileList());
            for (String name : request.getFileToGenerateList()) {
                for (Generator.Output o : Generator.generate(built.get(name))) {
                    response.addFile(CodeGeneratorResponse.File.newBuilder().setName(o.path()).setContent(o.content()));
                }
            }
        } catch (IOException | RuntimeException e) {
            response.setError(e.getMessage());
        }
        response.build().writeTo(System.out);
        System.out.flush();
    }

    private static void deleteTree(Path root) {
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException ignored) {
                    // Best effort: a temp directory.
                }
            });
        } catch (IOException ignored) {
            // Already gone.
        }
    }
}
