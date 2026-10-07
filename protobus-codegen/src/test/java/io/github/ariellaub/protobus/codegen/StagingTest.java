package io.github.ariellaub.protobus.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class StagingTest {
    static final String SCHEMA = String.join("\n",
            "syntax = \"proto3\";",
            "package billing;",
            "message Invoice {",
            "  bigint amount = 1;",
            "  map<string, timestamp> due = 2;",
            "  int64 timestamp = 3; // a field named like the type",
            "}",
            "");

    @Test
    void theCustomTypeImportAndJavaOptionAreAppended() {
        String staged = Staging.stage(SCHEMA, List.of());
        assertTrue(staged.startsWith(SCHEMA), "the original text is kept as is, so line numbers match");
        assertTrue(staged.contains("import \"protobus/types.proto\";"));
        assertTrue(staged.contains("option java_multiple_files = true;"));
    }

    @Test
    void namesInCommentsAndStringsDoNotCount() {
        String schema = "syntax = \"proto3\";\n// bigint x = 1;\n/* timestamp y = 2; */\n"
                + "message M { string s = 1 [json_name = \"bigint z = 3\"]; int64 timestamp = 2; }\n";
        String staged = Staging.stage(schema, List.of());
        assertFalse(staged.contains("protobus/types.proto"), staged);
    }

    @Test
    void aSchemaThatDeclaresOrImportsTheTypesIsLeftAlone() {
        String declares = "syntax = \"proto3\";\nmessage bigint { optional bytes value = 1; }\n"
                + "message M { bigint a = 1; }\noption java_multiple_files = false;\n";
        assertEquals(declares, Staging.stage(declares, List.of()));
        String imports = "syntax = \"proto3\";\nimport \"protobus/types.proto\";\nmessage M { bigint a = 1; }\n"
                + "option java_multiple_files = true;\n";
        assertEquals(imports, Staging.stage(imports, List.of()));
    }

    @Test
    void typePositionsAreRecognised() {
        String code = Staging.code("message M { repeated bigint a = 1; map<string,bigint> b = 2; }\n"
                + "service S { rpc f(timestamp) returns (Empty); }");
        assertTrue(Staging.uses(code, "bigint"));
        assertTrue(Staging.uses(code, "timestamp"));
        assertFalse(Staging.uses(Staging.code("message M { my.bigint a = 1; }"), "bigint"));
        assertTrue(Staging.uses(Staging.code("message M { .bigint a = 1; }"), "bigint"));
    }

    @Test
    void customTypesAreDeclaredAndImported() {
        Staging.CustomType uuid = Staging.CustomType.parse("uuid:bytes");
        String staged = Staging.stage("syntax = \"proto3\";\nmessage M { uuid id = 1; }\n", List.of(uuid));
        assertTrue(staged.contains("import \"protobus/custom/uuid.proto\";"));
        assertFalse(staged.contains("protobus/types.proto"));
        String schema = Staging.customTypeSchema(uuid, "acme.types");
        assertTrue(schema.contains("message uuid {\n  optional bytes value = 1;\n}"));
        assertTrue(schema.contains("option java_package = \"acme.types\";"));
        assertThrows(IllegalArgumentException.class, () -> Staging.CustomType.parse("uuid:float"));
        assertThrows(IllegalArgumentException.class, () -> Staging.CustomType.parse("bigint:bytes"));
        assertThrows(IllegalArgumentException.class, () -> Staging.CustomType.parse("bad-name:bytes"));
    }

    @Test
    void aCommentMarkerInsideAStringIsNotAComment() {
        String schema = "syntax = \"proto3\";\noption go_package = \"example.com/x//y\"; import \"protobus/types.proto\";\n"
                + "message M { bigint a = 1; }\noption java_multiple_files = true;\n";
        assertEquals(schema, Staging.stage(schema, List.of()));
    }

    @Test
    void aCapitalisedPackageBecomesALowercaseJavaPackage() {
        // package Chat in chat.proto: protoc's outer class Chat.Chat would shadow
        // the package in every qualified reference, and its own code would not
        // compile.
        String staged = Staging.stage("syntax = \"proto3\";\npackage Chat;\nmessage Token {}\n", List.of());
        assertTrue(staged.contains("option java_package = \"chat\";"), staged);
        assertTrue(Staging.stage("syntax = \"proto3\";\npackage Deep.Pkg;\n", List.of())
                .contains("option java_package = \"deep.pkg\";"));
        // Already lowercase, or chosen by the schema: left alone.
        assertFalse(Staging.stage("syntax = \"proto3\";\npackage interop;\n", List.of()).contains("java_package"));
        String own = "syntax = \"proto3\";\npackage Chat;\noption java_package = \"com.acme.chat\";\n";
        assertFalse(Staging.stage(own, List.of()).contains("option java_package = \"chat\""));
    }
}
