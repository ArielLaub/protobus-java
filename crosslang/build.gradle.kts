plugins {
    java
}

description = "The cross-language suite: protobus-java against the TypeScript, Python, Go and C++ ports"

dependencies {
    implementation(project(":protobus"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobusGenerate("main", "proto", libs.versions.protobuf.get())

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}

tasks.test {
    // Needs a broker and the peers; run explicitly: ./gradlew :crosslang:test
    onlyIf { System.getenv("PROTOBUS_TEST_AMQP_URL") != null }
    systemProperty("crosslang.dir", projectDir.absolutePath)
    systemProperty("crosslang.classpath", sourceSets.main.get().runtimeClasspath.asPath)
    dependsOn(sourceSets.main.get().runtimeClasspath)
    maxParallelForks = 1
}

tasks.register("printClasspath") {
    doLast { println(sourceSets.main.get().runtimeClasspath.asPath) }
}
