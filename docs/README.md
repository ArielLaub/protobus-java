# protobus-java documentation

| Start | |
|---|---|
| [Getting Started](getting-started.md) | From an empty directory to a service, a client, events and a test |
| [Services](services.md) | Implementing services: options, retries, concurrency, lifecycle |
| [Clients](clients.md) | Calling services: options, timeouts, errors, async, the dynamic proxy |
| [Streaming](streaming.md) | Server streaming and cancellation |
| [Events](events.md) | Publishing and subscribing, topic patterns, event retry |
| [Errors](errors.md) | The error model, retries and dead letters |

| Reference | |
|---|---|
| [Configuration](configuration.md) | Environment variables, reconnection, executors, logging |
| [Code generation](codegen.md) | The Gradle plugin, Maven, the CLI, the protoc plugin, custom types |
| [Testing](testing.md) | The in-memory broker and the suites |
| [Compatibility](compatibility.md) | The wire contract and how the ports differ |
| [Architecture](architecture.md) | Threads, ownership and reconnection |
| [Security](security.md) | Dispatch checks, error exposure, logging, TLS |

The Java snippets marked for it in these pages are compiled by the test suite
(`examples/src/test/java/examples/DocSnippetsTest.java`), against the examples'
schemas, so they track the API.

## Other languages

The `.proto` files are the contract and RabbitMQ does the routing, so a port
needs only protobuf and an AMQP client.

| Language | Repo | Status |
|---|---|---|
| TypeScript | [protobus](https://github.com/ArielLaub/protobus) | stable (reference) |
| Python | [protobus-py](https://github.com/ArielLaub/protobus-py) | stable |
| Go | [protobus-go](https://github.com/ArielLaub/protobus-go) | stable |
| C++ | [protobus-cpp](https://github.com/ArielLaub/protobus-cpp) | new |
| Java | [protobus-java](https://github.com/ArielLaub/protobus-java) (this repository) | new |

This port's CI runs it against the TypeScript, Python, Go and C++ ports' real
libraries, in both directions; the differences that remain are listed in
[Compatibility](compatibility.md).
