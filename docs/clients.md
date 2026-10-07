# Clients

A client calls a service through its generated proxy. Build one per service
(they are cheap and thread-safe), call `init()`, and call the rpcs.

<!-- doc-check: compile -->
```java
package app;

import calculator.AddRequest;
import calculator.AddResponse;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Context;
import java.util.concurrent.CompletableFuture;

class Calls {
    static void calls(Context context) {
        ServiceProtobus.Proxy calculator = new ServiceProtobus.Proxy(context);
        calculator.init();
        AddRequest request = AddRequest.newBuilder().setA(2).setB(3).build();

        // Blocking: returns the response, or throws.
        AddResponse sum = calculator.add(request);

        // Asynchronous: the same call as a CompletableFuture.
        CompletableFuture<AddResponse> later = calculator.addAsync(request, CallOptions.DEFAULT.withTimeoutMs(2000));
        later.thenAccept(r -> System.out.println(r.getResult()));
    }
}
```

The future completes on one of the context's threads, not the RabbitMQ
client's, so a continuation may block; prefer the `...Async` methods of
`CompletableFuture` with your own executor for heavy work.

## Call options

`CallOptions` is immutable; each `with` method returns a copy.

| | Default | |
|---|---|---|
| `withActor(String)` | none | a free-text caller identity, carried in the envelope for tracing |
| `withTimeoutMs(long)` | `RPC_CALL_TIMEOUT_MS` (10 min) | how long the call may take, confirm included |
| `withPriority(int)` | none | AMQP priority 0-255; only a priority queue honours it |
| `withMessageId(String)` | a fresh UUID | the message's identity, for deduplication |
| `withRpc(false)` | `true` | publish without waiting for a reply; returns once the broker confirms |

The deadline starts once the connection is ready to publish: a call made during
a reconnection first waits for it, up to `CONNECTION_READY_TIMEOUT_MS` (30 s).

## What a call can throw

| | Meaning | Retry? |
|---|---|---|
| `RemoteError` | the service answered with an error; `code()` and `getMessage()` are what it sent | depends on the code |
| `RpcTimeoutError` | no reply within the timeout | ambiguous: the service may have run it |
| `UnroutableError` | no service is bound to the routing key | safe |
| `PublishNackedError` | the broker refused the request | safe |
| `PublishConfirmTimeoutError` | no broker confirm in time (or no confirm slot free on the channel, in which case it was not sent) | ambiguous |
| `ChannelClosedError` | the channel closed before the confirm | ambiguous |
| `DisconnectedError` | the connection dropped while the call was pending | ambiguous |
| `NotReadyError` | the connection did not come back within `CONNECTION_READY_TIMEOUT_MS` | safe |
| `NotConnectedError` | the context is not connected and is not reconnecting | safe |
| `InvalidRequestError` | the request could not be encoded (a custom-type value out of range, say) | no |

"Ambiguous" means the request may have been stored and served. Retrying can
run it twice, so make the retry recognisable: pass the same
`withMessageId(...)`, derived from the work (an order id), and have the service
deduplicate on `context.messageId()`. Every port carries the id unchanged
across redeliveries, retries and dead-lettering.

All of these extend `ProtobusException`, which is unchecked; the publish
failures extend `PublishError`, which carries the `messageId()`.

## Instance names

A proxy built with an instance name calls that instance, while the envelope
names the contract method:

```java
PlayerProtobus.Proxy player6 = new PlayerProtobus.Proxy(context, "Combat.Player.player6");
```

## The dynamic proxy

`ServiceProxy` calls any service the context's factory knows, by method name,
with `DynamicMessage`s. Load the schema as a descriptor set first (the Gradle
plugin writes one to `protobus/schemas.binpb` in the resources; `protoc
--include_imports --descriptor_set_out` makes one too).

<!-- doc-check: compile -->
```java
package app;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.ServiceProxy;

class Dynamic {
    static void call(Context context) {
        context.factory().loadDescriptorSet(java.nio.file.Path.of("build/resources/main/protobus/schemas.binpb"));
        ServiceProxy calculator = new ServiceProxy(context, "Calculator.Service");
        calculator.init();
        Descriptor type = context.factory().type("Calculator.AddRequest");
        Message request = context.factory().newMessage("Calculator.AddRequest")
                .setField(type.findFieldByName("a"), 2)
                .setField(type.findFieldByName("b"), 3)
                .build();
        Message reply = calculator.call("add", request);
        System.out.println(reply);
    }
}
```

`Context.init(url, List.of("schemas/"))` loads every descriptor set under a
directory at startup.

## From Kotlin

The `Async` methods return `CompletableFuture`, which kotlinx.coroutines'
`await()` suspends on:

```kotlin
val sum = calculator.addAsync(request).await()
```
