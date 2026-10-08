# Architecture

The classes follow the TypeScript reference one to one.

| Class | Role |
|---|---|
| `Context` | one process's place on the bus: owns the connection, the factory and the two dispatchers |
| `Connection` | the AMQP connection: reconnection, restoration, confirmed publishing, the consume and settle loop |
| `MessageFactory` | schemas, and the request, reply and event envelopes |
| `MessageDispatcher` | the caller's side of RPC: publishes requests, routes replies and stream chunks |
| `EventDispatcher` | publishes events |
| `BaseListener` | a queue and its consumer, restored after a reconnection |
| `MessageListener` | a service's request queue, with its retry queue, retry exchange and DLQ |
| `EventListener` | an event queue and its topic router |
| `CallbackListener` | the context's reply queue |
| `CancelListener` | hears stream cancellations |
| `MessageService`, `RunnableService`, `ProxiedService` | the service base classes |
| `ServiceProxy` | calls a service by name |

`amqp.Transport` is the seam to the AMQP client: `RabbitTransport` over the
RabbitMQ Java client in production, `testing.MemoryBroker` in tests.

## Threads

| Thread | Runs |
|---|---|
| the RabbitMQ client's consumer threads | deliveries, one at a time per channel; replies and cancellations are handled right there |
| the RabbitMQ client's I/O thread | confirms, returns and closes |
| the handler executor | service and event handlers, and their settlement |
| `protobus-internal` | publishes (one writer per channel, in order), reconnection, restoration, settlement after a processing timeout, completion of callers' futures |
| `protobus-timer` | deadlines: RPC timeouts, idle timeouts, processing timeouts, confirm timeouts, reconnection backoff |

A delivery to a service is handed to the handler executor at once; the handler
runs there and its delivery is settled there (reply, ack, retry or
dead-letter), each publish waiting for its broker confirm. A delivery to the
reply queue is routed on the consumer thread, which never blocks, so a handler
waiting for a reply cannot starve the reply's own delivery. Callers' futures
are completed on `protobus-internal` threads, never on the RabbitMQ client's.

## Settling a delivery

A late-ack consumer (every service, by default) settles after its handler:

1. On success, publish the reply (or the stream's chunks), then ack. The reply
   goes first, so the worst case is a redelivered request rather than a
   settled one whose reply was lost.
2. On a `HandledError` (or a protocol error, which is handled), reply with it,
   then ack.
3. On any other failure, publish the message to the retry exchange with
   `x-retry-count` incremented, then ack; or, after the last retry, reply with
   the error, publish it to the DLQ, then ack. The reply is best effort: the
   dead-letter copy is the durable record, and a failed reply never stops it.
4. If a settlement publish itself fails, the delivery is requeued after a
   second.

Retry and dead-letter copies are published `mandatory`, and the ack waits for
their confirm, so the only copy of a message is never acknowledged on the
strength of a publish that went nowhere.

## Reconnection

Every listener and dispatcher registers a restorer with the connection. When
the socket drops: the generation number advances, pending calls fail, and
reconnection is scheduled with backoff. After connecting, the restorers run in
registration order (each re-opens its channel and re-declares its topology),
and only when all have succeeded does the connection report itself ready and
release the publishes waiting on it. A restorer that fails, or a socket that
drops during restoration, fails the whole attempt; a stale attempt, whose
generation was superseded meanwhile, discards itself.

## Publishing

Every channel is a confirm channel, and its publishes are written by one
writer of its own, in order, on a library thread: a socket write can block on a
full buffer or broker flow control, and the caller (and its deadline) never
wait on it. A publish records its sequence number and
waits for the broker's ack or nack; a mandatory publish that comes back as a
basic.return is reported `UnroutableError` when its ack arrives. At most
`MAX_OUTSTANDING_CONFIRMS` publishes await the broker's answer per channel. A
publish whose confirm timed out keeps its slot until the broker answers or the
channel closes, so a stalled broker cannot be handed an unbounded backlog; a
publish still waiting for a slot when its deadline passes fails without being
sent. Two pending
mandatory publishes that share a messageId carry an `x-protobus-publish-tag`
header, so their returns cannot be confused.
