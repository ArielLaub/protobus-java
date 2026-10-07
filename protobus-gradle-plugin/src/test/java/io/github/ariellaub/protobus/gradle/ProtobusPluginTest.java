package io.github.ariellaub.protobus.gradle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A real build applying the plugin to a shared schema, resolving protobus from a repository. */
class ProtobusPluginTest {
    @Test
    void aProjectBuildsAgainstASharedSchema(@TempDir Path dir) throws Exception {
        String repo = System.getProperty("testRepo").replace('\\', '/');
        Files.writeString(dir.resolve("settings.gradle.kts"), "rootProject.name = \"shop\"\n");
        Files.writeString(dir.resolve("build.gradle.kts"), String.join("\n",
                "plugins {",
                "    java",
                "    id(\"io.github.ariellaub.protobus\")",
                "}",
                "repositories {",
                "    maven { url = uri(\"" + repo + "\") }",
                "    mavenCentral()",
                "}",
                "dependencies {",
                "    implementation(\"io.github.ariellaub:protobus:" + Versions.PROTOBUS + "\")",
                "}",
                "protobus {",
                "    customTypes.add(\"uuid:bytes\")",
                "}",
                ""));
        Path proto = Files.createDirectories(dir.resolve("src/main/proto"));
        // Shared verbatim: no Java options, the custom types unimported.
        Files.writeString(proto.resolve("Shop.proto"), String.join("\n",
                "syntax = \"proto3\";",
                "package Shop;",
                "service Orders { rpc place(Order) returns (Receipt); rpc watch(Order) returns (stream Receipt); }",
                "message Order { bigint total = 1; uuid id = 2; }",
                "message Receipt { timestamp at = 1; }",
                ""));
        Path src = Files.createDirectories(dir.resolve("src/main/java/app"));
        Files.writeString(src.resolve("OrderService.java"), String.join("\n",
                "package app;",
                "import io.github.ariellaub.protobus.*;",
                "import shop.*;",
                "public class OrderService extends OrdersProtobus.Base {",
                "    public OrderService(Context c) { super(c); }",
                "    @Override public Receipt place(Order o, CallContext ctx) {",
                "        CustomTypes.toBigInteger(o.getTotal());",
                "        return Receipt.newBuilder().setAt(CustomTypes.timestamp(java.time.Instant.now())).build();",
                "    }",
                "    static Receipt call(Context c) {",
                "        OrdersProtobus.Proxy p = new OrdersProtobus.Proxy(c);",
                "        p.init();",
                "        return p.place(Order.newBuilder().setTotal(CustomTypes.bigint(5)).build());",
                "    }",
                "}",
                ""));
        BuildResult result = GradleRunner.create().withProjectDir(dir.toFile()).withPluginClasspath()
                .withArguments("compileJava", "--stacktrace").forwardOutput().build();
        assertEquals(TaskOutcome.SUCCESS, result.task(":generateProtobus").getOutcome());
        assertEquals(TaskOutcome.SUCCESS, result.task(":compileJava").getOutcome());
        assertTrue(Files.exists(dir.resolve("build/generated/resources/protobus/main/protobus/schemas.binpb")));

        BuildResult again = GradleRunner.create().withProjectDir(dir.toFile()).withPluginClasspath()
                .withArguments("compileJava").build();
        assertEquals(TaskOutcome.UP_TO_DATE, again.task(":generateProtobus").getOutcome());
    }
}
