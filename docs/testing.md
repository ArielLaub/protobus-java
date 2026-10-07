# Testing

## Your services, without RabbitMQ

`io.github.ariellaub.protobus.testing.MemoryBroker` is an AMQP broker in
memory. Give it to a context as its transport and everything runs in-process:

<!-- doc-check: compile -->
```java
package app;

import calculator.AddRequest;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.ContextOptions;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import java.time.Duration;

class InMemory {
    static void run() {
        try (MemoryBroker broker = new MemoryBroker();
             Context context = new Context(ContextOptions.DEFAULT.withTransport(broker))) {
            context.init("amqp://memory/");
            // ...init services as usual...
            ServiceProtobus.Proxy calculator = new ServiceProtobus.Proxy(context);
            calculator.init();

            // Inspect the broker.
            int dead = broker.queueDepth("Calculator.Service.DLQ");
            // Inject faults.
            broker.killConnections();      // a lost socket: reconnection runs
            broker.setConfirmMode(MemoryBroker.ConfirmMode.NACK);
            MemoryBroker.waitFor(() -> context.connection().isReady(), Duration.ofSeconds(5));
        }
    }
}
```

It implements what protobus relies on: direct, topic and fanout exchanges and
the default exchange; durable, exclusive, auto-delete and server-named queues;
per-queue TTL with dead-lettering; priority queues; per-consumer prefetch with
acknowledgement, rejection and redelivery; publisher confirms with mandatory
returns; and the channel errors RabbitMQ raises for a missing exchange or queue
(404), a redeclaration with different arguments (406) and an exclusive queue
owned elsewhere (405). Callbacks run on a thread of the broker's own, and a
blocking channel call made from one fails, as it would deadlock against
RabbitMQ.

| Fault injection | |
|---|---|
| `killConnections()` | drop every connection: unacked deliveries requeue, exclusive queues go |
| `refuseConnections(true)` | make connecting fail |
| `setConfirmMode(NACK / DROP)` | refuse publishes, or never confirm them |
| `releaseHeldConfirms(outcome)` | deliver the confirms `DROP` held back |
| `closeChannelsConsuming(queue, reason)` | close a channel, leaving the connection up |
| `cancelConsumers(queue)` | cancel consumers from the broker side |

| Inspection | |
|---|---|
| `queueExists`, `exchangeExists`, `queueNames` | topology |
| `queueDepth`, `unackedCount`, `consumerCount` | queue state |
| `peek(queue)` | the waiting messages, with their properties and headers |
| `bindings(queue, exchange)`, `queueArguments(queue)` | declarations |
| `flush()` | wait until every queued callback has run |

Timeouts are environment-driven; `Config.set("RPC_CALL_TIMEOUT_MS", "500")`
shortens one for a test, and `Config.reset()` undoes it.

## This repository's suites

| | Needs | Run |
|---|---|---|
| unit (runtime, generator, plugin, doc snippets) | nothing | `./gradlew build` |
| integration | RabbitMQ | `./gradlew :protobus:integrationTest` |
| cross-language | RabbitMQ and the other ports | `./gradlew :crosslang:test` |
| examples | RabbitMQ | `./gradlew :examples:runCalculator` and friends |

The broker suites never default to a broker. Point them at one:

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
```

The unit suites run every behaviour against the in-memory broker:
golden envelope bytes shared with the TypeScript, Go and C++ ports, retries and
dead-lettering with their headers, streaming with cancellation, backpressure,
sequence gaps and duplicates, events and event retry, reconnection, channel
and consumer loss, publish confirms, and graceful shutdown. The integration
suite repeats the parts only a real broker can prove: TTL and dead-lettering,
returns, priorities, header encodings and a broker-side connection close. The
[cross-language suite](../crosslang/README.md) runs Java against the other
four ports' real libraries.

`PROTOBUS_TEST_LOG=1` turns the library's logging on in every suite.
