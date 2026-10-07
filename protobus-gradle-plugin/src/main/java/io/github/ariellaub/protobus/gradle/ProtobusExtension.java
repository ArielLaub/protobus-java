package io.github.ariellaub.protobus.gradle;

import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;

/**
 * Configures the protobus code generator.
 *
 * <pre>
 * protobus {
 *     protoDir = layout.projectDirectory.dir("proto")   // default src/main/proto
 *     customTypes.add("uuid:bytes")
 * }
 * </pre>
 */
public abstract class ProtobusExtension {
    /** Where the shared .proto files are. Default {@code src/main/proto}. */
    public abstract DirectoryProperty getProtoDir();

    /** Custom types of your own, as {@code NAME:WIRE}; every port must declare the same. */
    public abstract ListProperty<String> getCustomTypes();

    /** The Java package of the custom types' classes. Default {@code protobus.custom}. */
    public abstract Property<String> getCustomTypePackage();

    /** The protobus-codegen version. Default: this plugin's. */
    public abstract Property<String> getCodegenVersion();

    /**
     * The protoc version, from Maven Central. Default: the one protobus is built
     * with; use your protobuf-java runtime's version or older.
     */
    public abstract Property<String> getProtocVersion();

    /** A protoc on disk, instead of one from Maven Central. */
    public abstract Property<String> getProtocPath();
}
