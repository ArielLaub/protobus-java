# Code generation

Schemas are shared verbatim between the five ports. They need no Java options
and no import for the custom types.

## The Gradle plugin

> Not on the Gradle Plugin Portal yet; until it is, use [the CLI](#the-cli)
> (`io.github.ariellaub:protobus-codegen` on Maven Central).

```kotlin
plugins {
    java
    id("io.github.ariellaub.protobus") version "2.0.0"
}

dependencies {
    implementation("io.github.ariellaub:protobus:2.0.0")
}

protobus {
    protoDir = layout.projectDirectory.dir("proto")  // default src/main/proto
    customTypes.add("uuid:bytes")                    // your own custom types, NAME:WIRE
    protocVersion = "3.25.9"                         // default: the version protobus is built with
}
```

`generateProtobus` runs before `compileJava`. It resolves protoc and the
generator from Maven Central (or `protocPath = "/usr/local/bin/protoc"`), stages
the schemas, and writes the Java sources and a `protobus/schemas.binpb`
descriptor set, for the dynamic API, into the main source set. Use a protoc no
newer than your protobuf-java runtime.

Do not also apply `com.google.protobuf` to the same directory: it would compile
the schemas a second time, without the custom types.

## The CLI

```
protobus-java generate --proto-dir DIR... --out DIR [--descriptor-out FILE]
                       [--custom-type NAME:WIRE]... [--custom-type-package PKG] [--protoc PATH]
```

The `protobus-codegen` artifact is the CLI (`io.github.ariellaub.protobus.codegen.Main`;
the `all` classifier is a single executable jar). protoc is `--protoc`, then
`$PROTOC`, then the one on the `PATH`.

`generate` copies the schemas to a staging directory and adds, at the end of
each copy so protoc's line numbers still match:

- `import "protobus/types.proto";` when it uses `bigint` or `timestamp`
  without declaring them, and the import of each `--custom-type` it uses;
- `option java_multiple_files = true;`, unless it sets that option;
- `option java_package = "<package, lowercased>";` when its package is
  capitalised and it sets no `java_package`.

The last is needed, not cosmetic: with `package Calculator` in
`Calculator.proto`, protoc's outer class `Calculator.Calculator` would shadow
the package in every qualified reference, and its own code would not compile.

Then it runs protoc with `--java_out` and writes the protobus classes. The
originals are never modified.

### Maven

Run the CLI from the build with `exec-maven-plugin`, and add its output as a
source root:

```xml
<plugin>
  <groupId>org.codehaus.mojo</groupId>
  <artifactId>exec-maven-plugin</artifactId>
  <executions>
    <execution>
      <id>protobus</id>
      <phase>generate-sources</phase>
      <goals><goal>java</goal></goals>
      <configuration>
        <includePluginDependencies>true</includePluginDependencies>
        <mainClass>io.github.ariellaub.protobus.codegen.Main</mainClass>
        <arguments>
          <argument>generate</argument>
          <argument>--proto-dir</argument><argument>${project.basedir}/src/main/proto</argument>
          <argument>--out</argument><argument>${project.build.directory}/generated-sources/protobus</argument>
        </arguments>
      </configuration>
    </execution>
  </executions>
  <dependencies>
    <dependency>
      <groupId>io.github.ariellaub</groupId>
      <artifactId>protobus-codegen</artifactId>
      <version>2.0.0</version>
    </dependency>
  </dependencies>
</plugin>
<plugin>
  <groupId>org.codehaus.mojo</groupId>
  <artifactId>build-helper-maven-plugin</artifactId>
  <executions>
    <execution>
      <phase>generate-sources</phase>
      <goals><goal>add-source</goal></goals>
      <configuration>
        <sources><source>${project.build.directory}/generated-sources/protobus</source></sources>
      </configuration>
    </execution>
  </executions>
</plugin>
```

This form needs protoc on the `PATH` (or `--protoc`).

## With protoc or buf directly

Run with no arguments, the CLI is a protoc plugin:

```bash
cat > protoc-gen-protobus-java <<'SH'
#!/bin/sh
exec java -jar /path/to/protobus-codegen-2.0.0-all.jar
SH
chmod +x protoc-gen-protobus-java
protoc -I proto --java_out=gen --protobus-java_out=gen --plugin=protoc-gen-protobus-java proto/Calculator.proto
```

Nothing is staged in this mode, so the schemas carry what Java needs
themselves: `import "protobus/types.proto";` (the file ships in the protobus
jar, and the `com.google.protobuf` Gradle plugin finds it there), and the Java
options you want. Do not generate Java for `protobus/types.proto`: its classes
ship with the runtime.

## What is generated

For `service Service` in `package Calculator`, in Java package `calculator`:

```java
public final class ServiceProtobus {
    public static final String SERVICE_NAME = "Calculator.Service";
    public static FileDescriptor getDescriptor();

    public abstract static class Base extends RunnableService {
        protected Base(Context context);
        protected Base(Context context, MessageServiceOptions options);
        // rpc add(Calculator.AddRequest) returns (Calculator.AddResponse)
        public AddResponse add(AddRequest request, CallContext context) throws Exception;
        // rpc tick(...) returns (stream Tick)
        public void tick(TickRequest request, StreamWriter<Tick> out, CallContext context) throws Exception;
    }

    public static class Proxy extends ServiceProxy {
        public Proxy(Context context);
        public Proxy(Context context, String serviceName);
        public AddResponse add(AddRequest request);
        public AddResponse add(AddRequest request, CallOptions options);
        public CompletableFuture<AddResponse> addAsync(AddRequest request);
        public CompletableFuture<AddResponse> addAsync(AddRequest request, CallOptions options);
        public ProtobusStream<Tick> tick(TickRequest request);
        public ProtobusStream<Tick> tick(TickRequest request, StreamOptions options);
    }
}
```

An rpc named like a Java keyword or a member of the base classes gets a
trailing underscore in Java (`delete` stays, `init` becomes `init_`, `class`
becomes `class_`); its name on the wire is unchanged. Client-streaming rpcs
are not part of the protobus protocol and are skipped.

## Custom types

`bigint` and `timestamp` are declared at the root of the type namespace, as in
every port:

```protobuf
message bigint    { optional bytes value = 1; }  // unsigned, up to 2^256-1: 32 bytes, big-endian
message timestamp { optional int64 value = 1; }  // signed milliseconds since the epoch
```

In Java they are `io.github.ariellaub.protobus.types.bigint` and `.timestamp`,
converted with `CustomTypes`:

<!-- doc-check: compile -->
```java
package app;

import calculator.Calculated;
import io.github.ariellaub.protobus.CustomTypes;
import io.github.ariellaub.protobus.types.bigint;
import java.math.BigInteger;
import java.time.Instant;

class Amounts {
    static void amounts() {
        bigint amount = CustomTypes.bigint(new BigInteger("1000000000000000000000000000000"));
        BigInteger value = CustomTypes.toBigInteger(amount);
        bigint hex = CustomTypes.bigint("0xff");

        Calculated event = Calculated.newBuilder().setAt(CustomTypes.timestamp(Instant.now())).build();
        Instant when = CustomTypes.toInstant(event.getAt());
        long ms = CustomTypes.toMillis(event.getAt());
    }
}
```

Every port refuses a negative or oversized `bigint` and refuses to decode one
wider than 32 bytes; a `timestamp` is refused beyond ±8.64e15 ms, the range
every port can represent. protobus-java checks every request, reply and event
it encodes or decodes, so a malformed value built by hand is refused before it
leaves, and one arriving in a request is answered `PROTOCOL_ERROR`.

### Custom types of your own

Declare one as `NAME:WIRE` (`customTypes` in the plugin, `--custom-type` in the
CLI), where WIRE is `bytes`, `int64`, `uint64`, `string`, `int32`, `uint32` or
`double`. It becomes a root-level message `NAME { optional WIRE value = 1; }`,
generated into the `protobus.custom` Java package (`customTypePackage` to
change it) and imported by every schema that uses it. The other ports must
declare the same type with the same wire type.

## Loading schemas at runtime

Java has no runtime `.proto` parser, so where the other ports load schema text,
this one loads compiled descriptor sets: `Context.init(url,
List.of("schemas/"))`, or `context.factory().loadDescriptorSet(path)`. The
plugin's `protobus/schemas.binpb`, or `protoc --include_imports
--descriptor_set_out=...`, provides one. Generated code needs none of this: it
registers its own schema.
