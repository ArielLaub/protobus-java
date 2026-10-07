# Changelog

## 2.0.0 (unreleased)

The first release of the Java port, numbered with the other ports' 2.x line,
whose wire protocol it speaks.

- The runtime, designed after TypeScript protobus 2.5 class for class:
  services, typed and dynamic proxies, server streaming with cancellation,
  events with topic patterns and optional retry, retries and dead-lettering,
  priority queues, instance-named services, processing timeouts, publisher
  confirms, reconnection with coordinated restoration, and graceful shutdown.
- Blocking calls and `CompletableFuture` variants; handlers on a replaceable
  executor (virtual threads on Java 21).
- `protobus-codegen`: a CLI and protoc plugin generating a `<Service>Protobus`
  class (a `Base` and a typed `Proxy`) per service, from shared schemas used
  verbatim.
- The `io.github.ariellaub.protobus` Gradle plugin.
- `MemoryBroker`, an in-memory AMQP broker for tests.
- Java 17+, protobuf-java 3.25 or 4.x, RabbitMQ 3.8+.
