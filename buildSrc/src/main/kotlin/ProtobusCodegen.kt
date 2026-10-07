import org.gradle.api.Project
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.register

/** protoc's Maven classifier for this machine. */
fun protocClassifier(): String {
    val os = System.getProperty("os.name").lowercase()
    val arch = System.getProperty("os.arch").lowercase()
    val o = when {
        os.contains("mac") -> "osx"
        os.contains("win") -> "windows"
        else -> "linux"
    }
    val a = when (arch) {
        "aarch64", "arm64" -> "aarch_64"
        "x86_64", "amd64" -> "x86_64"
        else -> arch
    }
    return "$o-$a"
}

/**
 * Run the protobus code generator (this build's own :protobus-codegen) over
 * `protoDir`, adding the generated sources to `sourceSetName`. This is what a
 * project using protobus does with the Gradle plugin; here it builds the tests,
 * examples and cross-language peers against the generator in the same tree.
 */
fun Project.protobusGenerate(sourceSetName: String, protoDir: String, protocVersion: String,
                             vararg extraArgs: String) {
    val codegen = configurations.maybeCreate("protobusCodegen")
    val protoc = configurations.maybeCreate("protobusProtoc")
    dependencies.add(codegen.name, dependencies.project(mapOf("path" to ":protobus-codegen")))
    dependencies.add(protoc.name, "com.google.protobuf:protoc:$protocVersion:${protocClassifier()}@exe")

    val out = layout.buildDirectory.dir("generated/protobus/$sourceSetName")
    val descriptors = layout.buildDirectory.file("generated/protobus-resources/$sourceSetName/protobus/schemas.binpb")
    val taskName = "generate${sourceSetName.replaceFirstChar { it.uppercase() }}Protobus"
    val task = tasks.register<JavaExec>(taskName) {
        description = "Generates protobus Java sources from $protoDir."
        group = "build"
        classpath = codegen
        mainClass.set("io.github.ariellaub.protobus.codegen.Main")
        inputs.dir(protoDir)
        inputs.files(protoc)
        outputs.dir(out)
        outputs.file(descriptors)
        doFirst {
            project.delete(out)
            val exe = protoc.singleFile
            exe.setExecutable(true)
            args = listOf(
                "generate", "--proto-dir", file(protoDir).absolutePath, "--out", out.get().asFile.absolutePath,
                "--descriptor-out", descriptors.get().asFile.absolutePath, "--protoc", exe.absolutePath,
            ) + extraArgs
        }
    }
    val sourceSets = extensions.getByType(SourceSetContainer::class.java)
    val ss: SourceSet = sourceSets[sourceSetName]
    ss.java.srcDir(task.map { out.get() })
    ss.resources.srcDir(task.map { descriptors.get().asFile.parentFile.parentFile })
}
