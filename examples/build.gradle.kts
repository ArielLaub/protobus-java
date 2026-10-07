plugins {
    java
}

description = "The protobus-java examples: calculator, tokenstream and combat"

dependencies {
    implementation(project(":protobus"))
    runtimeOnly(libs.slf4j.simple)
}

protobusGenerate("main", "proto", libs.versions.protobuf.get())

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}

for ((task, main) in listOf("runCalculator" to "examples.Calculator", "runTokenstream" to "examples.Tokenstream",
        "runCombat" to "examples.Combat")) {
    tasks.register<JavaExec>(task) {
        group = "examples"
        description = "Runs $main against AMQP_URL (default amqp://guest:guest@127.0.0.1:25672/)."
        classpath = sourceSets.main.get().runtimeClasspath
        mainClass.set(main)
        args = (project.findProperty("args") as String?)?.split(" ") ?: emptyList()
    }
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    // The documentation's Java snippets are compiled against these examples' schemas.
    systemProperty("docs.root", rootProject.projectDir.absolutePath)
    inputs.files(fileTree(rootProject.projectDir) { include("README.md", "docs/**/*.md") })
}
