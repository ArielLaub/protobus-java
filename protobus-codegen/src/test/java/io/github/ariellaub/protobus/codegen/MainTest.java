package io.github.ariellaub.protobus.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MainTest {
    static String protoc() {
        String p = System.getProperty("protoc");
        Assumptions.assumeTrue(p != null && Files.isExecutable(Path.of(p)), "no protoc for the generator test");
        return p;
    }

    @Test
    void generatesTheServiceClassesAndADescriptorSet(@TempDir Path dir) throws Exception {
        Path proto = Files.createDirectories(dir.resolve("proto/shop"));
        Files.writeString(proto.resolve("orders.proto"), String.join("\n",
                "syntax = \"proto3\";",
                "package shop.orders;",
                "service Orders {",
                "  rpc place(Order) returns (Receipt);",
                "  rpc watch(Order) returns (stream Receipt);",
                "  rpc init(Order) returns (Receipt);",
                "}",
                "message Order { bigint total = 1; }",
                "message Receipt { timestamp at = 1; uuid id = 2; }",
                ""));
        Path out = dir.resolve("out");
        Path set = dir.resolve("res/schemas.binpb");
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        int code = Main.run(new String[] {"generate", "--proto-dir", dir.resolve("proto").toString(), "--out",
                out.toString(), "--descriptor-out", set.toString(), "--custom-type", "uuid:bytes", "--protoc", protoc()},
                new PrintStream(log), new PrintStream(log));
        assertEquals(0, code, log.toString());
        String svc = Files.readString(out.resolve("shop/orders/OrdersProtobus.java"));
        assertTrue(svc.contains("public static final String SERVICE_NAME = \"shop.orders.Orders\";"));
        assertTrue(svc.contains("public shop.orders.Receipt place(shop.orders.Order request"));
        assertTrue(svc.contains("public void watch(shop.orders.Order request, io.github.ariellaub.protobus.StreamWriter"
                + "<shop.orders.Receipt> out"));
        assertTrue(svc.contains("registerUnary(\"init\", shop.orders.Order.getDefaultInstance(), this::init_);"));
        assertTrue(Files.exists(out.resolve("shop/orders/Order.java")), "java_multiple_files was added");
        assertTrue(Files.exists(out.resolve("protobus/custom/uuid.java")), "the custom type was generated");
        // The outer class must not be Uuid beside the message class uuid: a
        // case-insensitive file system (macOS, Windows) holds them as one file.
        assertTrue(Files.exists(out.resolve("protobus/custom/ProtobusCustomUuid.java")));
        assertTrue(Files.readString(out.resolve("protobus/custom/uuid.java")).contains("public final class uuid"));
        assertTrue(!Files.exists(out.resolve("io/github/ariellaub/protobus/types/bigint.java")),
                "the built-in types ship with the runtime");
        assertTrue(Files.size(set) > 0);
        // The originals are untouched.
        assertTrue(!Files.readString(proto.resolve("orders.proto")).contains("import"));
    }

    @Test
    void aSchemaErrorFailsWithProtocsMessage(@TempDir Path dir) throws Exception {
        Path proto = Files.createDirectories(dir.resolve("proto"));
        Files.writeString(proto.resolve("bad.proto"), "syntax = \"proto3\";\nmessage M { nope x = 1; }\n");
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        int code = Main.run(new String[] {"generate", "--proto-dir", proto.toString(), "--out",
                dir.resolve("out").toString(), "--protoc", protoc()}, new PrintStream(log), new PrintStream(log));
        assertEquals(1, code);
        assertTrue(log.toString().contains("bad.proto:2"), log.toString());
    }

    @Test
    void usageErrorsAreReported() {
        PrintStream sink = new PrintStream(new ByteArrayOutputStream());
        assertThrows(IllegalArgumentException.class, () -> Main.run(new String[] {"frobnicate"}, sink, sink));
        assertThrows(IllegalArgumentException.class, () -> Main.run(new String[] {"generate", "--out", "x"}, sink, sink));
        assertThrows(IllegalArgumentException.class,
                () -> Main.run(new String[] {"generate", "--proto-dir"}, sink, sink));
    }
}
