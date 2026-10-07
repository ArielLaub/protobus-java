package io.github.ariellaub.protobus.gradle;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

/**
 * Generates protobus Java sources from shared .proto files and adds them to the
 * main source set: protoc's messages, and a {@code <Service>Protobus} class per
 * service. The schemas are used as they are, with no Java options and no import
 * for the custom types: the generator adds what Java needs to a staged copy.
 * A {@code protobus/schemas.binpb} descriptor set is added to the resources, for
 * the dynamic API.
 *
 * <pre>
 * plugins {
 *     java
 *     id("io.github.ariellaub.protobus") version "2.0.0"
 * }
 * dependencies {
 *     implementation("io.github.ariellaub:protobus:2.0.0")
 * }
 * </pre>
 *
 * Do not also apply {@code com.google.protobuf} to the same directory: it would
 * compile the schemas a second time, without the custom types.
 */
public class ProtobusPlugin implements Plugin<Project> {
    public static final String TASK = "generateProtobus";

    /** protoc's Maven classifier for this machine. */
    static String protocClassifier() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
        String o = os.contains("mac") ? "osx" : os.contains("win") ? "windows" : "linux";
        String a = switch (arch) {
            case "aarch64", "arm64" -> "aarch_64";
            case "x86_64", "amd64" -> "x86_64";
            default -> arch;
        };
        return o + "-" + a;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(JavaPlugin.class);
        ProtobusExtension ext = project.getExtensions().create("protobus", ProtobusExtension.class);
        ext.getProtoDir().convention(project.getLayout().getProjectDirectory().dir("src/main/proto"));
        ext.getCustomTypePackage().convention("protobus.custom");
        ext.getCodegenVersion().convention(Versions.PROTOBUS);
        ext.getProtocVersion().convention(Versions.PROTOC);

        Configuration codegen = project.getConfigurations().create("protobusCodegen", c -> {
            c.setCanBeConsumed(false);
            c.defaultDependencies(d -> d.add(project.getDependencies().create(
                    "io.github.ariellaub:protobus-codegen:" + ext.getCodegenVersion().get())));
        });
        Configuration protoc = project.getConfigurations().create("protobusProtoc", c -> {
            c.setCanBeConsumed(false);
            c.setTransitive(false);
            c.defaultDependencies(d -> {
                if (!ext.getProtocPath().isPresent()) {
                    d.add(project.getDependencies().create("com.google.protobuf:protoc:" + ext.getProtocVersion().get()
                            + ":" + protocClassifier() + "@exe"));
                }
            });
        });

        Provider<Directory> out = project.getLayout().getBuildDirectory().dir("generated/sources/protobus/java/main");
        Provider<Directory> resources = project.getLayout().getBuildDirectory().dir("generated/resources/protobus/main");

        TaskProvider<JavaExec> task = project.getTasks().register(TASK, JavaExec.class, t -> {
            t.setDescription("Generates protobus Java sources from the shared .proto files.");
            t.setGroup("build");
            t.setClasspath(codegen);
            t.getMainClass().set("io.github.ariellaub.protobus.codegen.Main");
            t.getInputs().dir(ext.getProtoDir()).withPropertyName("protoDir");
            t.getInputs().files(protoc).withPropertyName("protoc");
            t.getInputs().property("customTypes", ext.getCustomTypes());
            t.getInputs().property("customTypePackage", ext.getCustomTypePackage());
            t.getOutputs().dir(out).withPropertyName("sources");
            t.getOutputs().dir(resources).withPropertyName("resources");
            t.doFirst(x -> {
                project.delete(out, resources);
                String exe;
                if (ext.getProtocPath().isPresent()) {
                    exe = ext.getProtocPath().get();
                } else {
                    File f = protoc.getSingleFile();
                    if (!f.canExecute() && !f.setExecutable(true)) {
                        throw new IllegalStateException("cannot make " + f + " executable");
                    }
                    exe = f.getAbsolutePath();
                }
                List<String> args = new ArrayList<>(List.of("generate",
                        "--proto-dir", ext.getProtoDir().get().getAsFile().getAbsolutePath(),
                        "--out", out.get().getAsFile().getAbsolutePath(),
                        "--descriptor-out", resources.get().file("protobus/schemas.binpb").getAsFile().getAbsolutePath(),
                        "--custom-type-package", ext.getCustomTypePackage().get(),
                        "--protoc", exe));
                for (String ct : ext.getCustomTypes().get()) {
                    args.add("--custom-type");
                    args.add(ct);
                }
                t.setArgs(args);
            });
        });

        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet main = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);
        main.getJava().srcDir(task.map(t -> out.get()));
        main.getResources().srcDir(task.map(t -> resources.get()));
    }
}
