plugins {
    java
    alias(libs.plugins.protobuf) apply false
}

subprojects {
    apply(plugin = "java")

    repositories { mavenCentral() }

    extensions.configure<JavaPluginExtension> {
        toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
    }

    tasks.withType<JavaCompile>().configureEach {
        // Built on 21, but the bytecode and the API surface are Java 17's:
        // `release` refuses any JDK API newer than 17, not just newer bytecode.
        options.release.set(17)
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing,-try,-this-escape", "-Werror"))
    }

    // CI runs the suites on other JDKs and protobuf runtimes than the build's:
    //   -PtestJava=17                    the JVM the tests run on
    //   -PprotobufRuntime=4.36.2         the protobuf-java the tests run against
    val testJava = providers.gradleProperty("testJava").orNull
    val protobufRuntime = providers.gradleProperty("protobufRuntime").orNull
    if (protobufRuntime != null) {
        configurations.matching { it.name.endsWith("RuntimeClasspath") && it.name != "runtimeClasspath" }.configureEach {
            resolutionStrategy.force("com.google.protobuf:protobuf-java:$protobufRuntime")
        }
    }

    val toolchains = extensions.getByType<JavaToolchainService>()
    tasks.withType<Test>().configureEach {
        if (testJava != null) {
            javaLauncher.set(toolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(testJava.toInt()))
            })
        }
        useJUnitPlatform()
        testLogging {
            events("failed", "skipped")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showStandardStreams = providers.environmentVariable("PROTOBUS_TEST_LOG").isPresent
        }
    }
}

// The artifacts users depend on: the runtime and the code generator. Signing and
// the Maven Central upload are a release step, configured where the keys live.
configure(listOf(project(":protobus"), project(":protobus-codegen"))) {
    apply(plugin = "maven-publish")
    extensions.configure<JavaPluginExtension> {
        withSourcesJar()
        withJavadocJar()
    }
    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
                pom {
                    name.set(project.name)
                    description.set(project.description)
                    url.set("https://github.com/ArielLaub/protobus-java")
                    licenses {
                        license {
                            name.set("MIT License")
                            url.set("https://opensource.org/licenses/MIT")
                        }
                    }
                    developers {
                        developer {
                            id.set("ArielLaub")
                            name.set("Ariel Laub")
                        }
                    }
                    scm {
                        url.set("https://github.com/ArielLaub/protobus-java")
                        connection.set("scm:git:https://github.com/ArielLaub/protobus-java.git")
                    }
                }
            }
        }
        repositories {
            // Used by the Gradle plugin's functional test.
            maven {
                name = "testRepo"
                url = uri(rootProject.layout.buildDirectory.dir("test-repo"))
            }
        }
    }
}
