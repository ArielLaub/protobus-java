plugins {
    application
}

description = "The protobus code generator: service bases and typed proxies from .proto files"

dependencies {
    implementation(libs.protobuf.java)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

sourceSets {
    main {
        // protobus/types.proto, which the generator adds to every staged schema
        // that uses bigint or timestamp.
        resources.srcDir("../protobus/src/main/proto")
    }
}

application {
    mainClass.set("io.github.ariellaub.protobus.codegen.Main")
    applicationName = "protobus-java"
}

tasks.jar {
    manifest { attributes("Main-Class" to "io.github.ariellaub.protobus.codegen.Main") }
}

/** A single executable jar, for protoc's --plugin and for build tools that run the CLI. */
val fatJar by tasks.registering(Jar::class) {
    archiveClassifier.set("all")
    manifest { attributes("Main-Class" to "io.github.ariellaub.protobus.codegen.Main") }
    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({ configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) } })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
val protocForTests by configurations.creating
dependencies {
    protocForTests("com.google.protobuf:protoc:${libs.versions.protobuf.get()}:${protocClassifier()}@exe")
}
tasks.test {
    doFirst {
        val exe = protocForTests.singleFile
        exe.setExecutable(true)
        systemProperty("protoc", exe.absolutePath)
    }
}
