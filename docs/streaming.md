# Streaming

A method declared `returns (stream T)` streams its reply: the service writes
chunks, the caller iterates them.

```protobuf
service Assistant {
  rpc generate(GenerateRequest) returns (stream Token);
}
```

## The service

The generated base gives the rpc a `StreamWriter<T>`. Each `write` is a chunk;
returning ends the stream; throwing ends it with the error, which the caller's
iteration raises.

<!-- doc-check: compile -->
```java
package app;

import chat.AssistantProtobus;
import chat.GenerateRequest;
import chat.Token;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.HandledError;
import io.github.ariellaub.protobus.StreamWriter;
import java.time.Duration;

class Assistant extends AssistantProtobus.Base {
    Assistant(Context context) {
        super(context);
    }

    @Override
    public void generate(GenerateRequest request, StreamWriter<Token> out, CallContext context)
            throws InterruptedException {
        if (request.getPrompt().isEmpty()) throw new HandledError("empty prompt", "EMPTY_PROMPT");
        String[] words = request.getPrompt().split(" ");
        for (int i = 0; i < words.length; i++) {
            // Fires when the caller cancels or stops listening: stop the work, not
            // just the writing.
            if (context.signal().await(Duration.ofMillis(50))) return;
            out.write(Token.newBuilder().setIndex(i).setText(words[i]).build());
        }
    }
}
```

A chunk is held until the next one is written (or the handler returns), so the
last chunk can be marked final: `x-protobus-final=true` on the last message,
`x-protobus-seq` numbering every one from 0. A stream that writes nothing still
sends one empty final message, so the caller's loop ends.

An error is never retried: the stream ends with a terminal error chunk, which
a `HandledError` fills with its own message and code, and anything else with
the same message a unary call would get (see [Errors](errors.md)). The
processing timeout does not apply to streams.

A streaming handler holds one of the service's `maxConcurrent` slots for as
long as it runs.

## The caller

The proxy method returns a `ProtobusStream<T>`: an `Iterator` and `Iterable`
whose `hasNext()` blocks until a chunk arrives, the stream ends, or it fails.

<!-- doc-check: compile -->
```java
package app;

import chat.AssistantProtobus;
import chat.GenerateRequest;
import chat.Token;
import io.github.ariellaub.protobus.AbortController;
import io.github.ariellaub.protobus.ProtobusStream;
import io.github.ariellaub.protobus.StreamOptions;

class Reader {
    static void read(AssistantProtobus.Proxy assistant) {
        GenerateRequest request = GenerateRequest.newBuilder().setPrompt("hello streaming world").build();

        // Closing the stream before its end cancels the call: use try-with-resources.
        try (ProtobusStream<Token> tokens = assistant.generate(request)) {
            for (Token token : tokens) {
                System.out.print(token.getText() + " ");
                if (token.getIndex() == 1) break;
            }
        }

        // Or cancel from anywhere: a Stop button, a request's own deadline.
        AbortController stop = new AbortController();
        try (ProtobusStream<Token> tokens = assistant.generate(request,
                StreamOptions.DEFAULT.withSignal(stop.signal()).withIdleTimeoutMs(5000))) {
            for (Token token : tokens) {
                if (token.getIndex() == 0) stop.abort(); // the loop ends; it does not throw
            }
        }
    }
}
```

| `StreamOptions` | Default | |
|---|---|---|
| `withIdleTimeoutMs(long)` | `STREAM_IDLE_TIMEOUT_MS` (60 s) | the longest gap allowed between chunks |
| `withSignal(AbortSignal)` | none | cancels the stream when it fires |
| `withActor(String)` | none | the caller's identity, as for unary calls |

The request is published when the method is called; a failure to publish
surfaces from the first `hasNext()`.

What the iteration can raise:

| | |
|---|---|
| `RemoteError` | the service's error, after the chunks before it |
| `StreamTimeoutError` | no chunk within the idle timeout; the producer is told to stop |
| `StreamBackpressureError` | the caller fell behind: more than `STREAM_MAX_BUFFERED_CHUNKS` or `STREAM_MAX_BUFFERED_BYTES` buffered for this call, or `STREAM_MAX_TOTAL_BUFFERED_BYTES` across the context |
| `StreamSequenceError` | a chunk was lost (a gap in `x-protobus-seq`) |
| `DisconnectedError` | the connection dropped |

A duplicate chunk (a broker redelivery) is dropped. A peer that sends no
sequence numbers is accepted as it is.

## Cancellation

Cancelling (closing the stream early, firing its signal, or the idle timeout)
releases the call's buffer at once and publishes a notice on the
`proto.bus.cancel` fanout exchange. The replica running that stream fires the
handler's `signal()` and stops publishing what it writes; the next `write`
throws, which unwinds the handler. Cancellation is cooperative and best effort:
a handler that never looks at its signal or its writer runs to the end, and a
lost notice is the same as none.
