plugins {
    `java-gradle-plugin`
}

description = "Gradle plugin: generate protobus service bases and proxies from shared .proto files"

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

gradlePlugin {
    plugins {
        create("protobus") {
            id = "io.github.ariellaub.protobus"
            implementationClass = "io.github.ariellaub.protobus.gradle.ProtobusPlugin"
            displayName = "protobus"
            description = project.description
        }
    }
}

val pluginVersion = project.version.toString()
val protocVersion = libs.versions.protobuf.get()
val generatedVersions = layout.buildDirectory.dir("generated/versions")
val writeVersions by tasks.registering {
    val out = generatedVersions
    inputs.property("version", pluginVersion)
    inputs.property("protoc", protocVersion)
    outputs.dir(out)
    doLast {
        val f = out.get().file("io/github/ariellaub/protobus/gradle/Versions.java").asFile
        f.parentFile.mkdirs()
        f.writeText("""
            package io.github.ariellaub.protobus.gradle;

            /** Written by the build. */
            final class Versions {
                private Versions() {}

                static final String PROTOBUS = "$pluginVersion";
                static final String PROTOC = "$protocVersion";
            }
        """.trimIndent() + "\n")
    }
}
sourceSets.main { java.srcDir(writeVersions) }

tasks.test {
    // The functional test resolves the runtime and the generator from a repository
    // the build publishes them to.
    dependsOn(":protobus:publishAllPublicationsToTestRepoRepository",
        ":protobus-codegen:publishAllPublicationsToTestRepoRepository")
    systemProperty("testRepo", rootProject.layout.buildDirectory.dir("test-repo").get().asFile.absolutePath)
}
