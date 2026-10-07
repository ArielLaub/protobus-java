# Compatibility with the TypeScript, Python, Go and C++ ports

protobus-java speaks the protobus wire protocol exactly as
[protobus](https://github.com/ArielLaub/protobus) (TypeScript, 2.5),
[protobus-py](https://github.com/ArielLaub/protobus-py) (2.0),
[protobus-go](https://github.com/ArielLaub/protobus-go) (2.0) and
[protobus-cpp](https://github.com/ArielLaub/protobus-cpp) (2.0) do. Services
and clients in all five languages can share one broker, one schema and even
one queue: replicas of a service in different languages compete for its
requests and climb one retry ladder together.

This is tested: the [cross-language suite](../crosslang/README.md) runs Java
against the other ports' real libraries over a real broker, in both
directions, and runs `interop.Flaky` in all five languages at once on one
queue.

## The contract is the `.proto`

A schema is shared verbatim. It needs no Java options and no import for the
built-in custom types: the code generator adds them to its staged copy. A
proto package becomes the Java package, lowercased when it is capitalised
(`package Calculator` → `calculator`), unless the schema sets `java_package`.

| Proto | TypeScript | Python | Go | C++ | Java |
|---|---|---|---|---|---|
| `int64`, `uint64`, … | decimal `string` | `int` | `int64`, `uint64` | `int64_t`, `uint64_t` | `long` (`uint64` as unsigned bits: `Long.toUnsignedString`) |
| `bigint` (custom) | `bigint` | `int` | `*pbtypes.Bigint` | `::bigint` | `types.bigint` (`CustomTypes.toBigInteger`) |
| `timestamp` (custom) | `Date` | aware `datetime` (UTC) | `*pbtypes.Timestamp` | `::timestamp` | `types.timestamp` (`CustomTypes.toInstant`) |
| `bytes` | `Buffer` | `bytes` | `[]byte` | `std::string` | `ByteString` |
| enum | value name (`string`) | value name | generated enum type | generated enum | generated enum |
| unset scalar | its default | its default | its default | its default | its default |

The wire bytes are identical in every case; only the in-language
representation differs.

`bigint` is an unsigned integer up to 2^256-1 carried as exactly 32 big-endian
bytes; `timestamp` is signed milliseconds since the epoch. Both are one-field
messages declared at the root of the type namespace. Every port refuses a
negative or oversized `bigint` and refuses to decode one wider than 32 bytes.

## Topology

Identical names, flags and arguments; the exchange names come from the same
environment variables.

| | Name | Type / flags |
|---|---|---|
| RPC exchange | `proto.bus` (`BUS_EXCHANGE_NAME`) | topic, durable |
| Reply exchange | `proto.bus.callback` (`CALLBACKS_EXCHANGE_NAME`) | direct, durable |
| Event exchange | `proto.bus.events` (`EVENTS_EXCHANGE_NAME`) | topic, durable |
| Stream-cancel exchange | `proto.bus.cancel` (`CANCEL_EXCHANGE_NAME`) | fanout, durable |
| Service queue | `<Service>` bound `REQUEST.<Service>.*` | durable; arguments only when configured (`x-message-ttl`, `x-max-priority`) |
| Retry | `<Service>.Retry` (TTL, DLX → `proto.bus`), `<Service>.Retry.Exchange` (topic, `#`), `<Service>.DLQ` | durable |
| Event queue | `<Service>.Events` | durable |
| Event retry (opt-in) | `<Service>.Events.Retry`, `.Events.Retry.Exchange`, `.Events.Redelivery`, `.Events.DLQ` | durable |
| Reply queue | server-named, bound to `proto.bus.callback` under its own name | exclusive, auto-delete |
| Cancel queue | server-named, bound to `proto.bus.cancel` | exclusive, auto-delete |

RabbitMQ compares integer queue arguments by value, so the different integer
widths the five AMQP clients pick for the same TTL are equivalent; the
mixed-replica test declares one service's queues from all five languages.

## Messages

- Requests, replies and events travel in the protobus envelopes
  (`RequestContainer`, `ResponseContainer`, `EventContainer`). protobus-java
  encodes them byte for byte as TypeScript does (the golden vectors in
  `protobus/src/test/java/.../internal/EnvelopesTest.java`, shared with
  protobus-go and protobus-cpp), including the empty fields TypeScript writes
  explicitly.
- Every publish carries a `messageId` (a UUID unless the caller sets one),
  `contentType: application/octet-stream` and a `correlationId`; requests and
  events are persistent. Unary requests are published `mandatory`.
- Streaming replies carry `x-protobus-seq` (from 0) and `x-protobus-final`;
  the last frame is final, an empty stream is one empty final frame, and a
  failure is the final frame. Cancellation is an empty message on
  `proto.bus.cancel` carrying the stream's correlation id.
- Retry and dead-letter copies carry `x-retry-count`,
  `x-original-routing-key` (the delivered routing key; nothing routes by the
  header), `x-first-failure-time`, `x-last-error` (the error's class and code,
  never an unhandled error's message), and on the DLQ `x-original-queue` and
  `x-dlq-time`; they keep `contentType`, `contentEncoding`, `priority`,
  `timestamp`, `type` and `appId`, and drop `expiration` and `userId`.
- Readers accept every encoding peers produce: integer headers of any width or
  as text, `x-protobus-final` as a boolean, number or text.
- A mandatory publish whose `messageId` is shared with another publish still
  awaiting its confirm on the same channel also carries
  `x-protobus-publish-tag`, a per-publish token that tells the broker's returns
  apart, as protobus-cpp does. Every port ignores or copies unknown headers.

## Errors

A service error crosses as `ResponseError{method, message, code}`. In Java a
remote error is a `RemoteError`; the codes are shared: `HANDLED_ERROR`
(default for a `HandledError`), `PROTOCOL_ERROR` (a request the service will
not run), `INTERNAL_ERROR` (an unhandled error when
`PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`), `PROCESSING_TIMEOUT`, and any code a
service chooses.

## Where the ports differ

These are deliberate, and none changes what is on the wire. Java follows the
TypeScript reference except where all the other ports' experience, or Java
itself, argued otherwise; where the threaded ports had to decide something
TypeScript's event loop never faced, it follows protobus-cpp.

| | Java | TypeScript 2.5 | Python 2.0 | Go 2.0 | C++ 2.0 |
|---|---|---|---|---|---|
| Who declares the core exchanges | every process, for what it publishes to | services only | every process | every process | every process |
| Processing timeout, after the last retry | caller answered: `PROCESSING_TIMEOUT` | caller waits for its own timeout | caller answered | caller answered | caller answered |
| Settlement publish fails | message requeued after 1 s | left unacknowledged | requeued after 1 s | requeued after 1 s | requeued after 1 s |
| Retry/DLQ copies | `mandatory` | not mandatory | `mandatory` | `mandatory` | `mandatory` |
| Channel lost on a live connection | listener rebuilt; dispatcher reopens on next publish | not recovered | listener rebuilt | component rebuilt | listener rebuilt; dispatcher reopens on next publish |
| Reply queue replaced on a live connection | calls pending on the old one fail with `DisconnectedError` | not recovered | | | |
| Streaming handler | writes to a `StreamWriter`; a throw at any point ends the stream with an error chunk | async generator; an eager throw before the iterable exists is answered like a unary error | | | coroutine generator |
| Streaming call | request published at call time; failures surface at the first `hasNext()` | starts at call time | starts at iteration | starts when ranged over | published at call time |
| Processing timeout on streams | not applied | covers obtaining the iterator | applied | not applied | covers obtaining the generator |
| Handlers run | on an executor, up to the prefetch, in parallel | on the event loop | on the event loop | on goroutines, in parallel | on worker threads, in parallel |
| Early-ack (at-most-once) consumers | at most `maxConcurrent` handlers at once | unbounded on the event loop | | | |
| Closing the context | fails pending calls and streams at once, then waits for running handlers, up to `SHUTDOWN_DRAIN_TIMEOUT_MS` | closes at once | closes at once | `Close` at once; `Shutdown` drains first | fails pending, then drains |
| Schemas loaded at runtime | compiled descriptor sets (Java has no `.proto` parser) | `.proto` text | | | `.proto` text |
| Logging | `LogSink` / structured `Log`, through SLF4J when present | `ILogger` / `Log` | `logging` | `log/slog` | `ILogger` / `Log` |

Blank cells are behaviours the other port's documentation does not state.
