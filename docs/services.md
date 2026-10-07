# Services

A service extends the generated `<Service>Protobus.Base` and overrides its
rpcs. Each handler receives the decoded request and a `CallContext`, and
returns the response (or, for a streaming rpc, writes chunks; see
[Streaming](streaming.md)).

```java
public AddResponse add(AddRequest request, CallContext context) { ... }
```

What a handler throws decides the outcome:

| Thrown | Caller receives | Request |
|---|---|---|
| nothing | the response | acknowledged |
| `HandledError` (or a subclass) | its message and code, at once | acknowledged, never retried |
| anything else | the final error, once retries are spent | retried, then dead-lettered |

See [Errors](errors.md).

## Starting and stopping

`RunnableService.start(context, constructor)` constructs the service, calls
`init()` and registers it for graceful shutdown, which runs on SIGINT or
SIGTERM (a JVM shutdown hook) or on `RunnableService.requestShutdown()`:

1. every started service stops consuming, keeping its channels open so the
   work in hand can still reply and acknowledge;
2. in-flight deliveries drain, up to `SHUTDOWN_DRAIN_TIMEOUT_MS` (30 s);
3. each service's `cleanup()` runs;
4. the contexts close.

`RunnableService.awaitShutdown()` blocks until that has happened and returns
the exit code. If `init()` fails, `start` closes the service and the context
and rethrows.

A service can also be driven by hand, for example from a framework's own
lifecycle: construct it, call `init()`, and at shutdown call
`stopConsuming()`, `context.connection().drainInFlight(ms)`, then
`context.close()`.

## Options

<!-- doc-check: compile -->
```java
package app;

import io.github.ariellaub.protobus.Config;
import io.github.ariellaub.protobus.EventRetryOptions;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.RetryOptions;

class Options {
    static final MessageServiceOptions OPTIONS = MessageServiceOptions.DEFAULT
            // Requests handled in parallel by this process: the queue's prefetch. Default 1.
            .withMaxConcurrent(16)
            // Retry hops before the DLQ (default 3), and the delay between them (default 5000 ms).
            .withRetry(RetryOptions.defaults().withMaxRetries(5).withRetryDelayMs(10000))
            // One attempt's time limit (default MESSAGE_PROCESSING_TIMEOUT, 10 minutes).
            .withProcessingTimeoutMs(30000)
            // Declare the queue with x-max-priority (default: a plain queue).
            .withMaxPriority(Config.RECOMMENDED_MAX_PRIORITY)
            // Retry for this service's event subscriptions (default: off).
            .withEventRetry(EventRetryOptions.of(3));
}
```

`withLateAck(false)` acknowledges on delivery instead of after the handler. It
turns off retries, dead-lettering and, for priority queues, prefetch: use it
only for genuine at-most-once work. `withMaxPriority` together with
`withLateAck(false)` is refused.

RabbitMQ fixes a queue's arguments when it is first declared. Changing
`retryDelayMs` for a service that has run fails `init()` with
`RetryQueueMismatchError`; changing `maxPriority` or `messageTtlMs` fails with
the broker's 406. Drain and delete the queue to change them.

## Concurrency

Handlers run on the context's handler executor, up to `maxConcurrent` at once
per service, so a handler must be safe for concurrent use. The default executor
is a pool of daemon threads that grows as needed. Pass your own in
`ContextOptions.withHandlerExecutor`; on Java 21,
`Executors.newVirtualThreadPerTaskExecutor()` runs each handler on a virtual
thread. Event handlers run the same way, bounded by `DEFAULT_PREFETCH`.

## The call context

| | |
|---|---|
| `actor()` | the caller's free-text identity, from the request envelope. Not authenticated. |
| `correlationId()` | the call's id |
| `method()` | the contract method, `Calculator.Service.add` |
| `messageId()` | stable across redeliveries and retries: deduplicate on it |
| `redelivered()` | the broker has delivered this message before |
| `routingKey()` | the key the broker delivered on |
| `headers()` | the delivery's AMQP headers |
| `signal()` | fires on the processing timeout and, for a stream, on cancellation |

protobus never interrupts a handler's thread. When the processing timeout
passes, the signal fires, the attempt is failed and retried, and the result the
handler eventually returns is discarded. A handler doing long work should check
`signal().aborted()`, or wait with `signal().await(Duration)` instead of
sleeping.

## Processing timeouts

`withProcessingTimeoutMs` caps one attempt at a unary request. A timed-out
attempt is retried like an unhandled error, and once the retries are spent the
caller is answered with `PROCESSING_TIMEOUT`. A streaming rpc is not bounded by
it: its caller's idle timeout applies instead (see [Streaming](streaming.md)).

## Instance names

Several services can serve one contract under their own names: override
`serviceName()`.

```java
class Player extends PlayerProtobus.Base {
    @Override
    public String serviceName() {
        return "Combat.Player." + id; // its own queue, REQUEST.Combat.Player.<id>.*
    }
}
```

The contract is found by trimming segments off the name until one names a
service in the schema. A proxy built with the instance name reaches that
instance: `new PlayerProtobus.Proxy(context, "Combat.Player.player6")`.

## Without generated code

Extend `MessageService` directly, return the schema's `FileDescriptor` from
`schema()` (or load a descriptor set into `context.factory()`), and register
handlers over `DynamicMessage`s:

```java
registerMethod("add", (request, context) -> {
    Descriptor type = this.context.factory().type("Calculator.AddResponse");
    return DynamicMessage.newBuilder(type).setField(type.findFieldByName("result"), 5).build();
});
```

## Events

A service publishes with `publishEvent(message)` and subscribes with
`subscribeEvent(Type.class, handler)`, after `init()`. See [Events](events.md).
