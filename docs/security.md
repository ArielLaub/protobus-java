# Security

## Dispatch checks

The method a request runs comes from its body, which any client that can
publish to the bus controls. Before a payload is decoded, a service checks
that:

1. the routing key the broker delivered on belongs to this service
   (`REQUEST.<serviceName>.`);
2. the body's method is the one the routing key names, so a client cannot
   route to one method and have another executed, which keeps RabbitMQ topic
   permissions meaningful;
3. the body names, in full, a method the service's own contract declares, so
   one service's request type is never parsed as another's.

Any failure is answered `PROTOCOL_ERROR` and never retried. Events are routed
to handlers by the key the broker delivered on, not by the topic written in
the body.

## Error exposure

An unhandled error's message reaches the caller by default, because the caller
is another of your services. Set `PROTOBUS_EXPOSE_INTERNAL_ERRORS=false` where
callers relay errors further (see [Errors](errors.md#what-the-caller-sees)).
`x-last-error` on retry and dead-letter copies carries an unhandled error's
class and code only, never its message, whatever the setting.

## Logging

protobus never logs a request, reply or event body: they routinely carry
credentials and personal data. Log lines name types, sizes, correlation ids
and routing keys. A broker URL is logged with its password replaced by `***`.
Unhandled errors are logged with their message and stack trace in the
service's own log.

Structured logging through `Log` can carry diagnostics, but only what a
`DiagnosticsSerializer` you install chooses to keep.

## Custom-type values

A `bigint` wider than 32 bytes, or a `timestamp` outside ±8.64e15 ms, in a
request is answered `PROTOCOL_ERROR` before the handler runs, and one in a
reply or an event is refused before it is sent.

## TLS

An `amqps://` URL connects with TLS and verifies the broker's certificate and
host name against the JVM's default trust store; configure it with the
standard `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`
properties. `?verify=verify_none` turns verification off, with a warning, for
development only. (The RabbitMQ Java client on its own trusts every
certificate for an `amqps://` URL; protobus does not use that default.)

## The actor

`CallOptions.withActor` travels in the request envelope as free text, for
tracing. Nothing authenticates it: do not make authorisation decisions on it.
