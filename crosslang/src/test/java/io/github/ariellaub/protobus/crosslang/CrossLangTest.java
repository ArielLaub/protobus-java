package io.github.ariellaub.protobus.crosslang;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.EventListener;
import io.github.ariellaub.protobus.LogLevel;
import io.github.ariellaub.protobus.Logger;
import io.github.ariellaub.protobus.RemoteError;
import io.github.ariellaub.protobus.amqp.AmqpChannel;
import io.github.ariellaub.protobus.amqp.Delivery;
import io.github.ariellaub.protobus.internal.Headers;
import interop.Attempted;
import interop.FailRequest;
import interop.FlakyProtobus;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The cross-language suite: protobus-java against the TypeScript, Python, Go and
 * C++ ports' real libraries over a real broker, in both directions.
 *
 * <ul>
 *   <li>the Java client against a Java, TypeScript, Python, Go and C++ server;
 *   <li>the TypeScript, Python, Go and C++ clients against a Java server;
 *   <li>replicas of one service in all five languages sharing a queue and its
 *       retry ladder.
 * </ul>
 *
 * It needs PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL, and the sibling
 * checkouts: PROTOBUS_TS (default ../protobus, built with {@code npm run
 * build-ts}), PROTOBUS_PY (default ../protobus-py, with a venv/), PROTOBUS_GO
 * (default ../protobus-go, with Go on the PATH) and PROTOBUS_CPP (default
 * ../protobus-cpp, built into build/). A missing peer skips its tests, unless
 * its variable is set explicitly, as CI does: then its absence is a failure.
 */
class CrossLangTest {
    static final Path HERE = Path.of(System.getProperty("crosslang.dir", "."));
    static final HttpClient http = HttpClient.newHttpClient();

    final List<Process> servers = new ArrayList<>();
    final Map<Process, Path> stderr = new java.util.HashMap<>();

    /** The last lines a peer wrote to stderr, for a failure message. */
    String stderrOf(Process p) {
        try {
            List<String> lines = Files.readAllLines(stderr.get(p));
            return String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
        } catch (IOException | RuntimeException e) {
            return "(no stderr: " + e + ")";
        }
    }
    String vhost;
    String vhostUrl;

    // ---- broker --------------------------------------------------------------------

    static String amqpUrl() {
        String url = System.getenv("PROTOBUS_TEST_AMQP_URL");
        Assumptions.assumeTrue(url != null && !url.isEmpty(), "PROTOBUS_TEST_AMQP_URL is not set");
        return url;
    }

    static URI mgmt() {
        String url = System.getenv("PROTOBUS_TEST_MGMT_URL");
        Assumptions.assumeTrue(url != null && !url.isEmpty(), "PROTOBUS_TEST_MGMT_URL is not set");
        return URI.create(url.replaceAll("/+$", ""));
    }

    static HttpResponse<String> api(String method, String path, String body) {
        URI base = mgmt();
        String info = base.getUserInfo() == null ? "guest:guest" : base.getUserInfo();
        URI uri = URI.create(base.getScheme() + "://" + base.getHost() + ":" + base.getPort() + path);
        HttpRequest.Builder b = HttpRequest.newBuilder(uri)
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(info.getBytes(StandardCharsets.UTF_8)))
                .header("content-type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        try {
            return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("management API: " + e, e);
        }
    }

    @BeforeEach
    void freshVhost() {
        String url = amqpUrl();
        mgmt();
        if (System.getenv("PROTOBUS_TEST_LOG") == null) Logger.setLevel(LogLevel.SILENT);
        vhost = "pbjava-" + UUID.randomUUID().toString().substring(0, 8);
        api("PUT", "/api/vhosts/" + vhost, null);
        String user = URI.create(url).getUserInfo();
        String name = user == null ? "guest" : user.split(":")[0];
        api("PUT", "/api/permissions/" + vhost + "/" + name, "{\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}");
        vhostUrl = url.replaceAll("/[^/]*$", "") + "/" + vhost;
    }

    @AfterEach
    void cleanUp() {
        for (Process p : servers) stop(p);
        if (vhost != null) api("DELETE", "/api/vhosts/" + vhost, null);
        Logger.setLevel(LogLevel.INFO);
    }

    // ---- peers ---------------------------------------------------------------------

    static Path sibling(String var, String name) {
        String v = System.getenv(var);
        if (v != null && !v.isEmpty()) return Path.of(v);
        return HERE.toAbsolutePath().getParent().getParent().resolve(name);
    }

    static boolean configured(String var) {
        String v = System.getenv(var);
        return v != null && !v.isEmpty();
    }

    record Command(List<String> argv, Map<String, String> env) {}

    private static String goPeer;
    private static String goFailure;

    static synchronized String goPeerBinary() {
        if (goPeer != null || goFailure != null) return goPeer;
        Path go = sibling("PROTOBUS_GO", "protobus-go");
        if (!Files.exists(go.resolve("crosslang/gopeer/gopeer.go"))) {
            goFailure = "protobus-go not found at " + go + " (set PROTOBUS_GO)";
            return null;
        }
        try {
            // Built in a scratch copy whose go.mod points at the checkout under test.
            Path dir = Files.createTempDirectory("pbjava-gopeer-");
            Files.copy(HERE.resolve("peers/go/main.go"), dir.resolve("main.go"));
            Files.copy(go.resolve("go.sum"), dir.resolve("go.sum"));
            Files.writeString(dir.resolve("go.mod"), "module protobus-java/crosslang/gopeer\n\ngo 1.25\n\n"
                    + "require github.com/ArielLaub/protobus-go/v2 v2.0.0\n\n"
                    + "replace github.com/ArielLaub/protobus-go/v2 => " + go.toAbsolutePath() + "\n");
            Path out = dir.resolve("gopeer");
            ProcessBuilder pb = new ProcessBuilder("go", "build", "-o", out.toString(), ".").directory(dir.toFile())
                    .redirectErrorStream(true);
            pb.environment().put("GOFLAGS", "-mod=mod");
            Process p = pb.start();
            String log = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (p.waitFor() != 0 || !Files.exists(out)) {
                goFailure = "could not build the Go peer: " + log;
                return null;
            }
            goPeer = out.toString();
        } catch (IOException | InterruptedException e) {
            goFailure = "could not build the Go peer: " + e;
        }
        return goPeer;
    }

    /** The command for a peer, or a skip (or failure, when configured) saying why it is unavailable. */
    static Command command(String lang, String mode) {
        String why;
        String var;
        switch (lang) {
            case "java": {
                String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
                return new Command(List.of(java, "-cp", System.getProperty("crosslang.classpath"),
                        JavaPeer.class.getName(), mode), Map.of());
            }
            case "ts": {
                Path ts = sibling("PROTOBUS_TS", "protobus");
                var = "PROTOBUS_TS";
                if (Files.exists(ts.resolve("dist/lib/context.js"))) {
                    return new Command(List.of("node", HERE.resolve("peers/ts/peer.js").toString(), mode),
                            Map.of("PROTOBUS_TS", ts.toString()));
                }
                why = "TypeScript protobus not built at " + ts + " (set PROTOBUS_TS)";
                break;
            }
            case "py": {
                Path py = sibling("PROTOBUS_PY", "protobus-py");
                var = "PROTOBUS_PY";
                Path interp = py.resolve("venv/bin/python");
                if (Files.exists(interp)) {
                    return new Command(List.of(interp.toString(), HERE.resolve("peers/py/peer.py").toString(), mode),
                            Map.of("PYTHONPATH", py.toString()));
                }
                why = "protobus-py venv not found at " + py + " (set PROTOBUS_PY)";
                break;
            }
            case "go": {
                var = "PROTOBUS_GO";
                String bin = goPeerBinary();
                if (bin != null) return new Command(List.of(bin, mode), Map.of());
                why = goFailure;
                break;
            }
            case "cpp": {
                var = "PROTOBUS_CPP";
                Path bin = sibling("PROTOBUS_CPP", "protobus-cpp").resolve("build/crosslang/cpppeer");
                if (Files.isExecutable(bin)) return new Command(List.of(bin.toString(), mode), Map.of());
                why = "protobus-cpp's cpppeer not built at " + bin + " (set PROTOBUS_CPP)";
                break;
            }
            default:
                throw new IllegalArgumentException(lang);
        }
        if (configured(var)) fail(why);
        Assumptions.abort(why);
        return null;
    }

    Process launch(String lang, String mode, String target) throws IOException {
        Command c = command(lang, mode);
        ProcessBuilder pb = new ProcessBuilder(c.argv());
        pb.environment().putAll(c.env());
        pb.environment().put("PROTOBUS_TEST_AMQP", vhostUrl);
        pb.environment().put("PROTOBUS_TEST_PROTO_DIR", HERE.resolve("proto").toString());
        pb.environment().put("PEER_TARGET", target);
        Path err = Files.createTempFile("pbjava-peer-" + lang + "-", ".err");
        pb.redirectError(err.toFile());
        Process p = pb.start();
        stderr.put(p, err);
        return p;
    }

    static void stop(Process p) {
        p.destroy();
        try {
            if (!p.waitFor(10, TimeUnit.SECONDS)) p.destroyForcibly().waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Start a peer's server, returning once it has printed READY. */
    void startServer(String lang) throws Exception {
        Process p = launch(lang, "server", lang);
        servers.add(p);
        BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        CompletableFuture<Boolean> ready = CompletableFuture.supplyAsync(() -> {
            try {
                for (String l = out.readLine(); l != null; l = out.readLine()) {
                    if (l.equals("READY")) return true;
                }
            } catch (IOException e) {
                return false;
            }
            return false;
        });
        Boolean ok;
        try {
            ok = ready.get(90, TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            ok = false;
        }
        if (!ok) fail(lang + " server did not become ready (alive: " + p.isAlive() + ")\n" + stderrOf(p));
        // Keep draining its stdout so it can never block on a full pipe.
        CompletableFuture.runAsync(() -> {
            try {
                while (out.readLine() != null) {
                    // discard
                }
            } catch (IOException ignored) {
                // The process ended.
            }
        });
    }

    /** Run a peer's client scenario against {@code target}; every check must pass. */
    void runClient(String lang, String target) throws Exception {
        Process p = launch(lang, "client", target);
        BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        int passed = 0;
        boolean done = false;
        List<String> failed = new ArrayList<>();
        for (String l = out.readLine(); l != null; l = out.readLine()) {
            if (l.startsWith("PASS ")) passed++;
            else if (l.startsWith("FAIL ")) failed.add(l.substring(5));
            else if (l.equals("DONE")) done = true;
        }
        int code = p.waitFor();
        assertTrue(failed.isEmpty(), lang + " client against " + target + ": " + failed);
        assertTrue(done, lang + " client did not finish (exit " + code + ")\n" + stderrOf(p));
        assertTrue(passed >= 15, lang + " client against " + target + " passed only " + passed + " checks");
    }

    // ---- the suites ----------------------------------------------------------------

    @ParameterizedTest(name = "java client against a {0} server")
    @ValueSource(strings = {"java", "ts", "py", "go", "cpp"})
    void javaClientAgainstEveryServer(String server) throws Exception {
        startServer(server);
        runClient("java", server);
    }

    @ParameterizedTest(name = "{0} client against a java server")
    @ValueSource(strings = {"ts", "py", "go", "cpp"})
    void everyClientAgainstAJavaServer(String client) throws Exception {
        command(client, "client");
        startServer("java");
        runClient(client, "java");
    }

    /**
     * interop.Flaky in all five languages at once, competing on one queue. Every
     * attempt fails, so each message climbs the retry ladder across replicas of
     * different languages: the x-retry-count one writes is read by the others, the
     * queue arguments each declares must be equivalent to the rest's, and the
     * message must end in the dead-letter queue exactly once, after exactly three
     * retries.
     */
    @Test
    void mixedReplicasShareOneRetryLadder() throws Exception {
        for (String lang : List.of("java", "ts", "py", "go", "cpp")) command(lang, "server");
        for (String lang : List.of("java", "ts", "py", "go", "cpp")) startServer(lang);

        try (Context ctx = new Context()) {
            ctx.init(vhostUrl);
            EventListener listener = new EventListener(ctx.connection(), ctx.factory(), null);
            listener.init(null, "");
            Map<String, AtomicInteger> perMessage = new ConcurrentHashMap<>();
            Map<String, AtomicInteger> langs = new ConcurrentHashMap<>();
            AtomicInteger total = new AtomicInteger();
            listener.subscribe(Attempted.getDefaultInstance(), (a, t, x) -> {
                perMessage.computeIfAbsent(a.getMessageId(), k -> new AtomicInteger()).incrementAndGet();
                langs.computeIfAbsent(a.getLang(), k -> new AtomicInteger()).incrementAndGet();
                total.incrementAndGet();
            }, "EVENT.attempted");
            listener.start();

            int messages = 10;
            int retries = 3;
            FlakyProtobus.Proxy flaky = new FlakyProtobus.Proxy(ctx);
            flaky.init();
            List<CompletableFuture<?>> calls = new ArrayList<>();
            for (int i = 0; i < messages; i++) {
                calls.add(flaky.failAsync(FailRequest.newBuilder().setId(String.valueOf(i)).build(),
                        CallOptions.DEFAULT.withMessageId("flaky-" + i).withTimeoutMs(30000)));
            }
            for (CompletableFuture<?> c : calls) {
                try {
                    c.get(60, TimeUnit.SECONDS);
                    fail("a flaky call succeeded");
                } catch (java.util.concurrent.ExecutionException e) {
                    // The caller is answered with the final failure.
                    assertTrue(e.getCause() instanceof RemoteError, String.valueOf(e.getCause()));
                    assertTrue(e.getCause().getMessage().startsWith("flaky "), e.getCause().getMessage());
                }
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (total.get() < messages * (retries + 1) && System.nanoTime() < deadline) Thread.sleep(20);
            assertEquals(messages * (retries + 1), total.get(), "attempts");
            for (Map.Entry<String, AtomicInteger> e : perMessage.entrySet()) {
                assertEquals(retries + 1, e.getValue().get(), e.getKey());
            }
            System.out.println("attempts by language: " + langs);
            assertTrue(langs.size() >= 2, "the ladder never crossed languages; the test proves nothing");

            AmqpChannel ch = ctx.connection().openChannel();
            List<Delivery> dead = new CopyOnWriteArrayList<>();
            ch.consume("interop.Flaky.DLQ", "dlq", true, false, dead::add, null);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (dead.size() < messages && System.nanoTime() < deadline) Thread.sleep(20);
            assertEquals(messages, dead.size(), "dead-lettered");
            Set<String> ids = new HashSet<>();
            for (Delivery d : dead) {
                BasicProperties props = d.properties();
                Map<String, Object> h = props.getHeaders();
                assertEquals(Long.valueOf(retries), Headers.integer(h.get("x-retry-count")));
                assertEquals("interop.Flaky", Headers.text(h.get("x-original-queue")));
                assertEquals("REQUEST.interop.Flaky.fail", Headers.text(h.get("x-original-routing-key")));
                String last = Headers.text(h.get("x-last-error"));
                assertFalse(last == null || last.isEmpty());
                assertFalse(last.contains("flaky"), "x-last-error must name the class, never the message: " + last);
                ids.add(props.getMessageId());
            }
            assertEquals(messages, ids.size(), "each message must be dead-lettered exactly once");
        }
    }
}
