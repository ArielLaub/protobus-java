plugins {
    `java-library`
    alias(libs.plugins.protobuf)
}

description = "Schema-first RPC over RabbitMQ, with Protobuf on the wire"

dependencies {
    api(libs.protobuf.java)
    api(libs.amqp.client)
    implementation(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    withSourcesJar()
    withJavadocJar()
}

protobuf {
    // Only protobus/types.proto (bigint, timestamp) is compiled here. The
    // envelopes are encoded by hand, byte for byte as TypeScript writes them.
    protoc { artifact = libs.protoc.get().toString() }
}

tasks.named<Javadoc>("javadoc") {
    // Generated sources carry no javadoc of their own.
    exclude("io/github/ariellaub/protobus/types/**")
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
}

val integrationTest by tasks.registering(Test::class) {
    description = "Runs the suites that need a real RabbitMQ (PROTOBUS_TEST_AMQP_URL)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    shouldRunAfter(tasks.test)
}

tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}
