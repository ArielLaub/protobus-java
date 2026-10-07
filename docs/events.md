# Events

Events are fire-and-forget messages on the `proto.bus.events` topic exchange.
Every subscriber with a matching binding gets its own copy; a publisher does
not know or wait for subscribers.

## Publishing

```java
context.publishEvent(calculated);                              // type and topic from the message: EVENT.Calculator.Calculated
context.publishEvent("Calculator.Calculated", calculated, "billing.done");   // a topic of your own
service.publishEvent(calculated);                              // the same, from inside a service
```

`publishEvent` returns once the broker has confirmed the event; there is a
`publishEventAsync` too. An event nobody subscribes to is dropped by the broker
and is not an error.

## Subscribing

A service subscribes after `init()`, on its own `<Service>.Events` queue:

<!-- doc-check: compile -->
```java
package app;

import calculator.Calculated;
import io.github.ariellaub.protobus.MessageService;

class Subscriptions {
    static void subscribe(MessageService service) {
        // Typed, on the default topic EVENT.Calculator.Calculated.
        service.subscribeEvent(Calculated.class, (event, type, topic) -> System.out.println(event.getOperation()));
        // On a pattern: * is one word, # is zero or more.
        service.subscribeEvent(Calculated.class, (event, type, topic) -> System.out.println(topic), "billing.*");
        // Dynamic: a DynamicMessage of any type the context's factory knows.
        service.subscribeEvent("Calculator.Calculated", (event, type, topic) -> System.out.println(event), "#");
    }
}
```

Several handlers may match one event; each runs once. A
typed handler only receives events of its own type; one of another type on
the same topic is skipped (with a warning), never decoded as the wrong type.
Handlers are matched by the routing key the broker delivered on, not by the
topic written in the event's body, so a publisher cannot reach handlers its
routing key does not.

Outside a service, an `EventListener` subscribes on a queue of its own; with
an empty queue name it gets an exclusive, server-named queue that disappears
with the connection:

```java
EventListener listener = new EventListener(context.connection(), context.factory(), null);
listener.init(null, "");
listener.subscribe(Calculated.getDefaultInstance(), (event, type, topic) -> { ... }, null);
listener.start();
```

## When a handler fails

By default a handler that throws loses its event: the delivery is rejected
without requeue, so one permanently failing event cannot stall the subscriber
behind its own prefetch.

`MessageServiceOptions.withEventRetry(EventRetryOptions.of(3))` gives events
the ladder requests climb: the failed event waits on `<Service>.Events.Retry`
for `retryDelayMs` (5 s by default), comes back to this subscriber only (through
`<Service>.Events.Redelivery`, never to the others), and after the last retry
lands on `<Service>.Events.DLQ`. A retried event re-runs every handler that
matched it, including the ones that succeeded. A `HandledError` is not retried.
