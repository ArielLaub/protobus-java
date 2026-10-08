# ProtoBus for Java

**RabbitMQ-native microservices for the JVM, with Protocol Buffers on the wire.**

[![CI](https://github.com/ArielLaub/protobus-java/actions/workflows/ci.yml/badge.svg)](https://github.com/ArielLaub/protobus-java/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-17%2B-007396?logo=openjdk&logoColor=white)](https://openjdk.org)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-%E2%89%A53.8-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com)

Define a service in a `.proto` file, implement the class protobus generates for
it, and call it from anywhere on the bus as if it were local. ProtoBus turns
each service into **one durable RabbitMQ queue with N processes competing for
it**, so load balancing, failover, backpressure, retries and dead-lettering are
handled by the broker.

This is the Java port of [protobus](https://github.com/ArielLaub/protobus)
(TypeScript), [protobus-py](https://github.com/ArielLaub/protobus-py) (Python),
[protobus-go](https://github.com/ArielLaub/protobus-go) (Go) and
[protobus-cpp](https://github.com/ArielLaub/protobus-cpp) (C++), designed after
the TypeScript reference class for class. The five are **wire-compatible**: a
Java service serves TypeScript, Python, Go and C++ callers and the other way
round, with streaming, events, custom types and error codes included. See
[Compatibility](docs/compatibility.md).

**Status: new.** 2.0.0 is the first release of the Java port. Its runtime is at
feature parity with the TypeScript port's, and its CI runs it against the TypeScript,
Python, Go and C++ ports' real libraries, in both directions, on RabbitMQ.

---

## Install

Java 17 or newer. With Gradle (Kotlin DSL):

```kotlin
plugins {
    java
    id("io.github.ariellaub.protobus") version "2.0.0"
}

dependencies {
    implementation("io.github.ariellaub:protobus:2.0.0")
}
```

The plugin generates the code for every `.proto` under `src/main/proto` at
build time; protoc comes from Maven Central, so nothing needs installing. For
Maven, or for protoc and buf directly, see [Code generation](docs/codegen.md).

> **The Gradle plugin is not on the Gradle Plugin Portal yet.** Until it is,
> generate the code with the `protobus-codegen` CLI, which is on Maven Central
> (see [Code generation](docs/codegen.md#the-cli)); the runtime dependency above
> is unaffected.

The runtime depends on [protobuf-java](https://github.com/protocolbuffers/protobuf)
(3.25 or 4.x) and the [RabbitMQ Java client](https://github.com/rabbitmq/rabbitmq-java-client),
and logs through SLF4J when a provider is on the class path.

You also need a RabbitMQ 3.8+ broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

---

## Quick start

Four steps to a working RPC. The code is the
[`examples`](examples/src/main/java/examples/Calculator.java) calculator, trimmed.

### 1. Describe the service

```protobuf
// src/main/proto/Calculator.proto
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
}

message AddResponse {
  int32 result = 1;
}

message DivideRequest {
  double dividend = 1;
  double divisor = 2;
}

message DivideResponse {
  double quotient = 1;
}
```

The package plus the service name is the service's name on the bus:
`Calculator.Service`. The file is an ordinary protobus schema, shared as-is
with TypeScript, Python, Go and C++. It needs no Java options.

### 2. Generate the Java code

The Gradle plugin runs on every build. The proto package becomes the Java
package, lowercased (`package Calculator` becomes `calculator`), holding
protoc's message classes plus `ServiceProtobus`, with:

- `ServiceProtobus.Base`, the class your implementation extends, with a method
  per rpc;
- `ServiceProtobus.Proxy`, a typed client.

### 3. Implement and run the service

<!-- doc-check: compile -->
```java
package app;

import calculator.AddRequest;
import calculator.AddResponse;
import calculator.DivideRequest;
import calculator.DivideResponse;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.HandledError;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.RunnableService;

public class CalculatorService extends ServiceProtobus.Base {
    public CalculatorService(Context context, MessageServiceOptions options) {
        super(context, options);
    }

    @Override
    public AddResponse add(AddRequest request, CallContext context) {
        return AddResponse.newBuilder().setResult(request.getA() + request.getB()).build();
    }

    @Override
    public DivideResponse divide(DivideRequest request, CallContext context) {
        if (request.getDivisor() == 0) {
            // A HandledError is an answer, not a failure: never retried.
            throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
        }
        return DivideResponse.newBuilder().setQuotient(request.getDividend() / request.getDivisor()).build();
    }

    public static void main(String[] args) throws InterruptedException {
        Context context = new Context();
        context.init(System.getenv("AMQP_URL"));
        RunnableService.start(context, CalculatorService::new, MessageServiceOptions.DEFAULT.withMaxConcurrent(8), null);
        // Serves until SIGINT/SIGTERM, then drains in-flight work and closes.
        System.exit(RunnableService.awaitShutdown());
    }
}
```

Every handler receives a `CallContext` with the caller's actor, the message id
to deduplicate on, and an `AbortSignal` that fires when the processing timeout
expires. An rpc you do not override answers `PROTOCOL_ERROR`.

### 4. Call it

<!-- doc-check: compile -->
```java
package app;

import calculator.AddRequest;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Context;

public class Client {
    public static void main(String[] args) {
        try (Context context = new Context()) {
            context.init(System.getenv("AMQP_URL"));
            ServiceProtobus.Proxy calculator = new ServiceProtobus.Proxy(context);
            calculator.init();
            AddRequest request = AddRequest.newBuilder().setA(5).setB(3).build();
            System.out.println("5 + 3 = " + calculator.add(request, CallOptions.DEFAULT.withTimeoutMs(10000)).getResult());
        }
    }
}
```

```
$ java app.CalculatorService &
$ java app.Client
5 + 3 = 8
```

Without `withTimeoutMs` the call is bounded by `RPC_CALL_TIMEOUT_MS` (10 minutes
by default, as in the other ports). Every method also has an `Async` form
returning a `CompletableFuture`: `calculator.addAsync(request)`.

The full walkthrough adds events, error handling and a unit test:
**[Getting Started](docs/getting-started.md)**.

---

## Why ProtoBus

### RabbitMQ only, on purpose

ProtoBus is built for one broker, so the things a broker is good at stay in the
broker instead of being reimplemented above it:

| Concern | Where it lives |
|---|---|
| Load balancing | competing consumers on one queue |
| Routing | topic exchange bindings (`REQUEST.<Service>.*`) |
| Redelivery on consumer loss | late ack: an unacked delivery returns to the queue |
| Retry delay | the retry queue's `x-message-ttl`, drained by a dead-letter exchange |
| Persistence | durable queues, persistent messages |
| Dead letters | a real `<Service>.DLQ` |
| Priority | native queue priorities |

A request goes publisher → exchange → queue → consumer. Nothing tracks live
instances, so nothing holds a stale one, and a consumer that dies mid-request
leaves its delivery unacked for the next consumer to take.

If you may need to swap RabbitMQ for another broker, use a transport-agnostic
framework instead. That is a real feature and protobus does not have it.

### Protocol Buffers, not JSON

- **Contract-first.** The `.proto` file is the interface between teams and
  languages, and the generated Java types fail the build when the two drift
  apart.
- **Versioning by field number.** Adding a field does not break an old peer.

### A small protocol, a small port

The protocol protobus adds on top of AMQP is small and documented: five
envelope messages, a routing-key convention, an error encoding and two
streaming headers. Because queueing, consumer distribution and retry delays
belong to the broker, a port adapts that protocol to another AMQP client
rather than reimplementing messaging. This port depends on the RabbitMQ Java
client for AMQP, protobuf-java for the wire, and SLF4J's API for logging, and
nothing else at runtime. It brings no container, no dependency injection, no
annotation processing and no runtime bytecode generation.

---

## Features

- **Retries and dead-lettering.** An unhandled exception or a processing
  timeout parks the request on `<Service>.Retry` and redelivers it; after
  `maxRetries` (3 by default, 5 seconds apart) it lands on `<Service>.DLQ`,
  and the caller is answered with the final error. A `HandledError` is an
  answer and is never retried. See [Errors](docs/errors.md).
- **Server streaming.** A method declared `returns (stream T)` is implemented
  by writing chunks to a `StreamWriter<T>`, and consumed as an iterator.
  Closing the stream early, or firing an `AbortSignal`, stops the producer on
  the server. See [Streaming](docs/streaming.md).
- **Events.** `publishEvent` publishes on a topic exchange; `subscribeEvent`
  receives events by message type, topic pattern (`*`, `#`) or both, typed or
  dynamic, with optional per-listener retries and a dead-letter queue. See
  [Events](docs/events.md).
- **Custom types.** `bigint` (unsigned 256-bit) and `timestamp` (milliseconds
  since the epoch) are built in and need no import in the schema; in Java they
  convert to `BigInteger` and `Instant` with `CustomTypes`. Declare your own as
  `NAME:WIRE`. See [Code generation](docs/codegen.md).
- **Priority.** `withMaxPriority` makes a service queue a priority queue, and
  `CallOptions.withPriority` lets a control message overtake a bulk backlog.
- **Instance-named services.** Many instances can serve one contract under
  their own names (`Combat.Player.player6`), each addressed by its own proxy.
- **Processing timeout.** `withProcessingTimeoutMs` caps one attempt at a
  unary request; the handler's signal fires and the attempt counts as failed.
- **Reconnection.** A lost connection is re-established with capped
  exponential backoff, and every service, listener and dispatcher is restored
  before the connection reports itself ready. Calls made meanwhile wait for it;
  calls in flight when it dropped fail with `DisconnectedError`.
- **Publisher confirms.** Every publish waits for the broker's confirm. A
  failure is a `PublishError` that says whether the outcome is ambiguous, and
  `CallOptions.withMessageId` makes a republish safe to deduplicate.
- **Blocking or asynchronous.** Every generated client method has a blocking
  form and an `Async` form returning a `CompletableFuture`. Handlers run on an
  executor you can replace, for example with virtual threads on Java 21.
- **Dynamic API.** `ServiceProxy.call`, `ServiceProxy.callStream` and
  `MessageService.registerMethod` serve and call services over
  `DynamicMessage`s, from descriptor sets loaded at runtime.
- **An in-memory broker.** `MemoryBroker` runs services and clients
  in-process, with retries, dead-lettering, priorities and connection loss
  modelled. See [Testing](docs/testing.md).
- **Graceful shutdown.** `RunnableService` stops intake on SIGINT/SIGTERM, lets
  in-flight work finish within `SHUTDOWN_DRAIN_TIMEOUT_MS`, runs your
  `cleanup()`, then closes.

### A short tour

<!-- doc-check: compile -->
```java
package app;

import calculator.AddRequest;
import calculator.Calculated;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Config;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.CustomTypes;
import io.github.ariellaub.protobus.EventListener;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.RetryOptions;
import java.time.Instant;

class Tour {
    static void tour(Context context) {
        // Options on a service: concurrency, retries, a processing timeout and a
        // priority queue.
        MessageServiceOptions options = MessageServiceOptions.DEFAULT
                .withMaxConcurrent(16)
                .withRetry(RetryOptions.defaults().withMaxRetries(5).withRetryDelayMs(10000))
                .withProcessingTimeoutMs(30000)
                .withMaxPriority(Config.RECOMMENDED_MAX_PRIORITY);

        // Options on a call.
        ServiceProtobus.Proxy calculator = new ServiceProtobus.Proxy(context);
        calculator.init();
        calculator.add(AddRequest.getDefaultInstance(), CallOptions.DEFAULT
                .withPriority(Config.PRIORITY_HIGH)
                .withTimeoutMs(5000)
                .withActor("billing-job"));

        // Events: subscribe by type, publish from anywhere on the bus.
        EventListener listener = new EventListener(context.connection(), context.factory(), null);
        listener.init(null, "");
        listener.subscribe(Calculated.getDefaultInstance(),
                (event, type, topic) -> System.out.println(event.getOperation()), null);
        listener.start();

        context.publishEvent(Calculated.newBuilder().setOperation("add")
                .setAt(CustomTypes.timestamp(Instant.now())).build());
    }
}
```

<!-- doc-check: compile -->
```java
package app;

import chat.AssistantProtobus;
import chat.GenerateRequest;
import chat.Token;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.ProtobusStream;
import io.github.ariellaub.protobus.StreamWriter;

// Streaming, server side: each write is one chunk.
class Assistant extends AssistantProtobus.Base {
    Assistant(Context context) {
        super(context);
    }

    @Override
    public void generate(GenerateRequest request, StreamWriter<Token> out, CallContext context) {
        int i = 0;
        for (String word : new String[] {"hello", "from", "java"}) {
            if (context.signal().aborted()) return; // the caller has gone: stop producing
            out.write(Token.newBuilder().setIndex(i++).setText(word).build());
        }
    }

    // Client side: an iterator over the generated method's stream.
    static void read(AssistantProtobus.Proxy assistant) {
        try (ProtobusStream<Token> tokens = assistant.generate(GenerateRequest.newBuilder().setPrompt("hi").build())) {
            for (Token token : tokens) {
                System.out.print(token.getText() + " ");
                if (token.getIndex() == 1) break; // closing the stream tells the server to stop
            }
        }
    }
}
```

---

## Concurrency

Each unacknowledged delivery runs on the context's handler executor, so
handlers run in parallel and must be safe for concurrent use. The number in
flight is bounded by the consumer's prefetch: `maxConcurrent` for a service
(default 1, one request at a time), `DEFAULT_PREFETCH` for event handling (also
1). A streaming handler holds its slot for the life of its stream. Replies are
routed on the RabbitMQ client's own thread, never on the handler executor, so a
handler that calls another service and waits for its reply cannot starve the
reply's delivery. On Java 21, pass
`ContextOptions.DEFAULT.withHandlerExecutor(Executors.newVirtualThreadPerTaskExecutor())`
to run every handler on a virtual thread.

A `Context`, its proxies and its dispatchers are safe for concurrent use:
create one context per process and share it. See
[Architecture](docs/architecture.md).

---

## Wire compatibility

protobus-java speaks the protobus wire protocol exactly as TypeScript protobus
2.5, protobus-py 2.0, protobus-go 2.0 and protobus-cpp 2.0 do: the same
exchanges, queues, envelopes (byte for byte), headers and error codes, and the
same environment variables for configuration. Replicas of one service in
different languages can share its queue and climb one retry ladder together.
The [cross-language suite](crosslang/README.md) runs Java against the other
ports' real libraries over a real broker, in both directions.

The type mapping, the topology and the few deliberate behavioural differences
are in **[Compatibility](docs/compatibility.md)**.

---

## Documentation

Full index: **[docs/](docs/README.md)**

| Start | |
|---|---|
| [Getting Started](docs/getting-started.md) | From an empty directory to a service, a client, events and a test |
| [Services](docs/services.md) | Implementing services: options, retries, concurrency, lifecycle |
| [Clients](docs/clients.md) | Calling services: options, timeouts, errors, async, the dynamic proxy |
| [Streaming](docs/streaming.md) | Server streaming and cancellation |
| [Events](docs/events.md) | Publishing and subscribing, topic patterns, event retry |
| [Errors](docs/errors.md) | The error model, retries and dead letters |

| Reference | |
|---|---|
| [Configuration](docs/configuration.md) | Environment variables, reconnection, executors, logging |
| [Code generation](docs/codegen.md) | The Gradle plugin, Maven, the CLI, the protoc plugin, custom types |
| [Testing](docs/testing.md) | The in-memory broker and the suites |
| [Compatibility](docs/compatibility.md) | The wire contract and how the ports differ |
| [Architecture](docs/architecture.md) | Threads, ownership and reconnection |
| [Security](docs/security.md) | Dispatch checks, error exposure, logging, TLS |

---

## Examples

| Example | Shows |
|---|---|
| [`Calculator`](examples/src/main/java/examples/Calculator.java) | A service, a client, a handled error and an event |
| [`Tokenstream`](examples/src/main/java/examples/Tokenstream.java) | Streaming tokens, cancelled three ways, with the server's own count |
| [`Combat`](examples/src/main/java/examples/Combat.java) | Six instances of one service playing a game over RPC and events |

```bash
docker compose up -d --wait   # RabbitMQ on 127.0.0.1:25672
AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ ./gradlew :examples:runCalculator
```

---

## License

MIT. See [LICENSE](LICENSE).
