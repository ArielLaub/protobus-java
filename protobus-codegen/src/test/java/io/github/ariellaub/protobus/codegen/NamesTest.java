package io.github.ariellaub.protobus.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

class NamesTest {
    static FileDescriptor file(String name, String pkg, FileOptions options, String... messages) throws Exception {
        FileDescriptorProto.Builder f = FileDescriptorProto.newBuilder().setName(name).setPackage(pkg)
                .setSyntax("proto3").setOptions(options);
        for (String m : messages) {
            f.addMessageType(DescriptorProto.newBuilder().setName(m)
                    .addNestedType(DescriptorProto.newBuilder().setName("Inner")));
        }
        f.addService(ServiceDescriptorProto.newBuilder().setName("Svc"));
        return FileDescriptor.buildFrom(f.build(), new FileDescriptor[0]);
    }

    @Test
    void outerClassesFollowProtoc() throws Exception {
        assertEquals("Interop", Names.outerClass(file("interop.proto", "interop", FileOptions.getDefaultInstance())));
        assertEquals("MyFile2X", Names.outerClass(file("dir/my_file2x.proto", "p", FileOptions.getDefaultInstance())));
        // A type named like the file: protoc appends OuterClass.
        assertEquals("CalculatorOuterClass", Names.outerClass(file("Calculator.proto", "Calculator",
                FileOptions.getDefaultInstance(), "Calculator")));
        assertEquals("Custom", Names.outerClass(file("x.proto", "p",
                FileOptions.newBuilder().setJavaOuterClassname("Custom").build())));
    }

    @Test
    void messageClassesFollowProtoc() throws Exception {
        FileDescriptor multiple = file("a.proto", "deep.pkg", FileOptions.newBuilder().setJavaMultipleFiles(true)
                .build(), "Item");
        assertEquals("deep.pkg.Item", Names.messageClass(multiple.findMessageTypeByName("Item")));
        assertEquals("deep.pkg.Item.Inner",
                Names.messageClass(multiple.findMessageTypeByName("Item").getNestedTypes().get(0)));
        FileDescriptor single = file("a.proto", "deep.pkg", FileOptions.newBuilder().setJavaPackage("com.acme")
                .build(), "Item");
        assertEquals("com.acme.A.Item", Names.messageClass(single.findMessageTypeByName("Item")));
        assertEquals("com.acme.A", Names.descriptorHolder(single));
    }

    @Test
    void rpcsThatWouldCollideGetAnUnderscore() {
        assertEquals("delete", Names.methodIdentifier("delete"));
        assertEquals("init_", Names.methodIdentifier("init"));
        assertEquals("class_", Names.methodIdentifier("class"));
        assertEquals("call_", Names.methodIdentifier("call"));
        assertEquals("add", Names.methodIdentifier("add"));
    }
}
