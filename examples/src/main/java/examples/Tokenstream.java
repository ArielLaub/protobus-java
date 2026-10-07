package examples;

import chat.AssistantProtobus;
import chat.GenerateRequest;
import chat.StatsRequest;
import chat.StatsResponse;
import chat.Token;
import io.github.ariellaub.protobus.AbortController;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.ProtobusStream;
import io.github.ariellaub.protobus.StreamOptions;
import io.github.ariellaub.protobus.StreamWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Server-streaming with cancellation, in the shape a chat UI needs: a service
 * streams tokens like a language model, and the caller stops it three ways.
 *
 * <pre>
 *   docker compose up -d --wait
 *   AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ ./gradlew :examples:runTokenstream
 * </pre>
 *
 * After each run the demo asks the SERVER how much it generated. Cancellation that
 * only stopped the reader would show the full count; here the producer stops,
 * because the caller's cancel reaches it as its signal firing.
 */
public final class Tokenstream {
    private Tokenstream() {}

    static List<String> completion(String prompt) {
        StringBuilder body = new StringBuilder("Answering \"" + prompt + "\". ");
        for (int i = 0; i < 3; i++) {
            body.append("Streaming responses arrive one token at a time, which is what lets a chat interface render "
                    + "text as it is produced rather than waiting for a whole reply. That same property is what makes "
                    + "stopping useful: when the reader has seen enough, every token after that point is wasted work "
                    + "on the server. ");
        }
        List<String> words = new ArrayList<>();
        for (String w : body.toString().trim().split("\\s+")) words.add(w + " ");
        return words;
    }

    /** Streams a long canned completion, one word at a time. */
    static final class Assistant extends AssistantProtobus.Base {
        private StatsResponse stats = StatsResponse.getDefaultInstance();

        Assistant(Context context, MessageServiceOptions options) {
            super(context, options);
        }

        @Override
        public void generate(GenerateRequest request, StreamWriter<Token> out, CallContext context)
                throws InterruptedException {
            List<String> words = completion(request.getPrompt());
            Duration delay = Duration.ofMillis(request.getTokenDelayMs() > 0 ? request.getTokenDelayMs() : 60);
            synchronized (this) {
                stats = StatsResponse.getDefaultInstance();
            }
            for (int i = 0; i < words.size(); i++) {
                // The signal fires when the caller cancels: it closed the stream,
                // aborted its own signal, or went idle. This is where a real service
                // would abort its upstream model call, and the point of the demo:
                // stopping saves the work, not just the reading.
                if (context.signal().await(delay)) {
                    synchronized (this) {
                        stats = stats.toBuilder().setStoppedEarly(true).build();
                    }
                    return;
                }
                out.write(Token.newBuilder().setIndex(i).setText(words.get(i)).build());
                synchronized (this) {
                    stats = stats.toBuilder().setTokensGenerated(i + 1).build();
                }
            }
        }

        @Override
        public synchronized StatsResponse stats(StatsRequest request, CallContext context) {
            return stats;
        }
    }

    static void header(String title) {
        String rule = "=".repeat(64);
        System.out.println("\n" + rule + "\n" + title + "\n" + rule);
    }

    static void report(AssistantProtobus.Proxy client) throws InterruptedException {
        Thread.sleep(300); // let the cancellation travel
        StatsResponse stats = client.stats(StatsRequest.getDefaultInstance());
        System.out.println("  server generated " + stats.getTokensGenerated() + " tokens; stopped early: "
                + stats.getStoppedEarly());
    }

    /** Stop lives outside the loop (another thread, a UI handler) and takes effect at once. */
    static void stopButton(AssistantProtobus.Proxy client) throws InterruptedException {
        header("1. Stop button (an AbortSignal)");
        AbortController stop = new AbortController();
        Thread button = new Thread(() -> {
            try {
                Thread.sleep(900);
            } catch (InterruptedException ignored) {
                return;
            }
            System.out.println("\n  [user pressed Stop]");
            stop.abort();
        });
        button.start();
        System.out.print("  ");
        // A cancelled stream ends rather than raising.
        try (ProtobusStream<Token> tokens = client.generate(
                GenerateRequest.newBuilder().setPrompt("why does streaming matter?").setTokenDelayMs(60).build(),
                StreamOptions.DEFAULT.withSignal(stop.signal()))) {
            for (Token token : tokens) System.out.print(token.getText());
        }
        button.join();
        report(client);
    }

    /** The decision is made inside the loop: leaving it closes, and so cancels, the stream. */
    static void breakOut(AssistantProtobus.Proxy client) throws InterruptedException {
        header("2. break out of the loop");
        System.out.print("  ");
        int printed = 0;
        try (ProtobusStream<Token> tokens = client.generate(
                GenerateRequest.newBuilder().setPrompt("only the first few words").setTokenDelayMs(60).build())) {
            for (Token token : tokens) {
                System.out.print(token.getText());
                if (++printed == 8) break;
            }
        }
        System.out.println("\n  [consumer stopped reading]");
        report(client);
    }

    /** The control, where nothing cancels. */
    static void runToCompletion(AssistantProtobus.Proxy client) throws InterruptedException {
        header("3. no cancellation");
        int received = 0;
        try (ProtobusStream<Token> tokens = client.generate(
                GenerateRequest.newBuilder().setPrompt("short answer").setTokenDelayMs(1).build())) {
            for (Token ignored : tokens) received++;
        }
        System.out.println("  received " + received + " tokens");
        report(client);
    }

    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault("AMQP_URL", "amqp://guest:guest@127.0.0.1:25672/");
        try (Context context = new Context()) {
            context.init(url);
            // A streaming handler holds its prefetch slot for the life of its
            // stream, so concurrency is how many callers are served at once.
            Assistant assistant = new Assistant(context, MessageServiceOptions.DEFAULT.withMaxConcurrent(8));
            assistant.init();

            AssistantProtobus.Proxy client = new AssistantProtobus.Proxy(context);
            client.init();
            stopButton(client);
            breakOut(client);
            runToCompletion(client);
        }
    }
}
