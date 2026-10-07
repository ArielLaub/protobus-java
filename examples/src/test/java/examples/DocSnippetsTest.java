package examples;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Compiles every Java snippet in the README and docs/ that is marked
 * {@code <!-- doc-check: compile -->}, against the examples' generated code
 * (the Calculator, Chat and Combat schemas) and the runtime.
 */
class DocSnippetsTest {
    static final Pattern SNIPPET = Pattern.compile("<!-- doc-check: compile -->\\s*```java\\n(.*?)```", Pattern.DOTALL);

    record Snippet(Path file, int line, String code) {}

    static List<Snippet> snippets() throws IOException {
        Path root = Path.of(System.getProperty("docs.root"));
        List<Path> files = new ArrayList<>();
        files.add(root.resolve("README.md"));
        Path docs = root.resolve("docs");
        if (Files.isDirectory(docs)) {
            try (Stream<Path> walk = Files.walk(docs)) {
                walk.filter(p -> p.toString().endsWith(".md")).sorted().forEach(files::add);
            }
        }
        List<Snippet> out = new ArrayList<>();
        for (Path f : files) {
            String text = Files.readString(f, StandardCharsets.UTF_8);
            Matcher m = SNIPPET.matcher(text);
            while (m.find()) {
                int line = text.substring(0, m.start()).split("\n", -1).length;
                out.add(new Snippet(root.relativize(f), line, m.group(1)));
            }
        }
        return out;
    }

    static String className(String code) {
        Matcher m = Pattern.compile("(?m)^(?:public\\s+|abstract\\s+|final\\s+)*(?:class|interface|record|enum)\\s+(\\w+)")
                .matcher(code);
        return m.find() ? m.group(1) : "Snippet";
    }

    /**
     * The snippets of one page compile together, as a reader assembles them: a
     * test snippet may use the service the page defined above it.
     */
    @TestFactory
    Stream<DynamicTest> everyMarkedSnippetCompiles() throws IOException {
        List<Snippet> all = snippets();
        assertTrue(!all.isEmpty(), "no doc-check snippets found");
        java.util.Map<Path, List<Snippet>> byFile = new java.util.LinkedHashMap<>();
        for (Snippet s : all) byFile.computeIfAbsent(s.file(), k -> new ArrayList<>()).add(s);
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        return byFile.entrySet().stream().map(e -> DynamicTest.dynamicTest(e.getKey().toString(), () -> {
            Path dir = Files.createTempDirectory("doc-snippet-");
            List<Path> sources = new ArrayList<>();
            for (Snippet s : e.getValue()) {
                Path src = Files.createDirectories(dir.resolve("src" + s.line())).resolve(className(s.code()) + ".java");
                Files.writeString(src, s.code());
                sources.add(src);
            }
            StringWriter log = new StringWriter();
            boolean ok = javac.getTask(log, null, null,
                    List.of("--release", "17", "-proc:none", "-nowarn", "-d", dir.resolve("out").toString(), "-cp",
                            System.getProperty("java.class.path")),
                    null, javac.getStandardFileManager(null, null, StandardCharsets.UTF_8).getJavaFileObjects(
                            sources.toArray(new Path[0])))
                    .call();
            assertTrue(ok, e.getKey() + " does not compile:\n" + log);
        }));
    }
}
