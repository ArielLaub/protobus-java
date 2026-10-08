pluginManagement {
    repositories {
        gradlePluginPortal()
        // com.vanniktech.maven.publish releases to Maven Central; the Plugin
        // Portal's copy of it is years out of date.
        mavenCentral()
    }
}

rootProject.name = "protobus-java"

include("protobus")
include("protobus-codegen")
include("protobus-gradle-plugin")
include("examples")
include("crosslang")
