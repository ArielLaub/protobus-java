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

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("failed", "skipped")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showStandardStreams = providers.environmentVariable("PROTOBUS_TEST_LOG").isPresent
        }
    }
}
