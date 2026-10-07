package io.github.ariellaub.protobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import io.github.ariellaub.protobus.amqp.Delivery;
import io.github.ariellaub.protobus.internal.Envelopes;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import pbtest.AddRequest;
import pbtest.AddResponse;
import pbtest.CalcProtobus;
import pbtest.DivideRequest;
import pbtest.FailRequest;
import pbtest.Kind;
import pbtest.Nothing;
import pbtest.SlowRequest;
import pbtest.Wallet;
import pbtest.Who;

class RpcTest extends MemoryBus {
    static AddRequest add(int a, int b) {
        return AddRequest.newBuilder().setA(a).setB(b).build();
    }

    @Test
    void unaryCallsRoundTrip() {
        serve();
        assertEquals(42, proxy().add(add(20, 22)).getResult());
        assertEquals(0.25, proxy().divide(DivideRequest.newBuilder().setDividend(1).setDivisor(4).build())
                .getQuotient());
    }

    @Test
    void asyncCallsComplete() throws Exception {
        serve();
        CalcProtobus.Proxy p = proxy();
        List<CompletableFuture<AddResponse>> calls = List.of(p.addAsync(add(1, 1)), p.addAsync(add(2, 2)),
                p.addAsync(add(3, 3)));
        CompletableFuture.allOf(calls.toArray(new CompletableFuture[0])).get();
        assertEquals(List.of(2, 4, 6), calls.stream().map(c -> c.join().getResult()).toList());
    }

    @Test
    void aHandledErrorIsAnsweredAtOnceAndNeverRetried() {
        CalcService s = serve();
        RemoteError e = assertThrows(RemoteError.class,
                () -> proxy().fail(FailRequest.newBuilder().setId("7").setHandled(true).build()));
        assertEquals("refused 7", e.getMessage());
        assertEquals("REFUSED", e.code());
        assertEquals("pbtest.Calc.fail", e.method());
        assertEquals(1, s.failAttempts.get());
        assertEquals(0, broker.queueDepth("pbtest.Calc.DLQ"));
    }

    @Test
    void anUnhandledErrorClimbsTheRetryLadderThenReachesTheDlq() {
        CalcService s = serve(MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withRetryDelayMs(20)));
        RemoteError e = assertThrows(RemoteError.class,
                () -> proxy().fail(FailRequest.newBuilder().setId("9").build()));
        // Exposed by default: the caller is another of your own services.
        assertEquals("boom 9", e.getMessage());
        assertEquals(4, s.failAttempts.get());
        assertTrue(eventually(() -> broker.queueDepth("pbtest.Calc.DLQ") == 1));
        Delivery dead = broker.peek("pbtest.Calc.DLQ").get(0);
        assertEquals("3", header(dead, "x-retry-count"));
        assertEquals("REQUEST.pbtest.Calc.fail", header(dead, "x-original-routing-key"));
        assertEquals("pbtest.Calc", header(dead, "x-original-queue"));
        // Name only: an unhandled error's message never reaches a header.
        assertEquals("IllegalStateException", header(dead, "x-last-error"));
        assertFalse(header(dead, "x-first-failure-time").isEmpty());
        assertFalse(header(dead, "x-dlq-time").isEmpty());
        assertEquals("application/octet-stream", dead.properties().getContentType());
        // One identity across every attempt and the dead-letter copy.
        assertEquals(1, s.failMessageIds.stream().distinct().count());
        assertEquals(s.failMessageIds.get(0), dead.properties().getMessageId());
    }

    @Test
    void anUnhandledErrorIsHiddenWhenExposureIsOff() {
        Config.set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "false");
        serve(MessageServiceOptions.DEFAULT.withRetry(RetryOptions.defaults().withMaxRetries(0)));
        RemoteError e = assertThrows(RemoteError.class,
                () -> proxy().fail(FailRequest.newBuilder().setId("x").build()));
        assertEquals("INTERNAL_ERROR", e.code());
        assertTrue(e.getMessage().startsWith("internal service error (correlationId "));
    }

    @Test
    void anUnimplementedRpcIsAProtocolError() {
        serve();
        RemoteError e = assertThrows(RemoteError.class, () -> proxy().unimplemented(Nothing.getDefaultInstance()));
        assertEquals("PROTOCOL_ERROR", e.code());
        assertEquals(0, broker.queueDepth("pbtest.Calc.DLQ"));
    }

    @Test
    void callMetadataReachesTheHandler() {
        serve();
        Who who = proxy().whoami(Nothing.getDefaultInstance(),
                CallOptions.DEFAULT.withActor("tester").withMessageId("order-1"));
        assertEquals("tester", who.getActor());
        assertEquals("order-1", who.getMessageId());
        assertEquals("REQUEST.pbtest.Calc.whoami", who.getRoutingKey());
        assertFalse(who.getRedelivered());
        // A fresh UUID otherwise.
        assertEquals(36, proxy().whoami(Nothing.getDefaultInstance()).getMessageId().length());
    }

    @Test
    void customTypesMapsAndDefaultsRoundTrip() {
        serve();
        Wallet w = Wallet.newBuilder()
                .setAmount(CustomTypes.bigint(BigInteger.TEN.pow(30)))
                .setAt(CustomTypes.timestamp(Instant.parse("1969-07-20T20:17:40Z")))
                .putBalances("k", CustomTypes.bigint(BigInteger.TWO.pow(200)))
                .addParts(CustomTypes.bigint(1)).addParts(CustomTypes.bigint(2))
                .setKind(Kind.KIND_SPOT)
                .setBig(9007199254740993L)
                .build();
        Wallet back = proxy().echo(w);
        assertEquals(w, back);
        assertEquals(BigInteger.TEN.pow(30), CustomTypes.toBigInteger(back.getAmount()));
    }

    @Test
    void aMalformedCustomTypeInARequestIsAProtocolError() {
        serve();
        // Built by hand, bypassing the client-side check, as a foreign peer could.
        byte[] payload = Wallet.newBuilder().setAmount(io.github.ariellaub.protobus.types.bigint.newBuilder()
                .setValue(com.google.protobuf.ByteString.copyFrom(new byte[33]))).build().toByteArray();
        byte[] request = Envelopes.encodeRequest(new Envelopes.Request("pbtest.Calc.echo", null, payload));
        byte[] reply = ctx.publishMessage(request, "REQUEST.pbtest.Calc.echo", CallOptions.DEFAULT);
        Envelopes.Response r = Envelopes.decodeResponse(reply);
        assertEquals("PROTOCOL_ERROR", r.error().code());
    }

    @Test
    void oneWayCallsReturnOnceConfirmed() {
        CalcService s = serve();
        AddResponse empty = proxy().add(add(1, 2), CallOptions.DEFAULT.withRpc(false));
        assertEquals(AddResponse.getDefaultInstance(), empty);
        proxy().fail(FailRequest.newBuilder().setId("1").setHandled(true).build(), CallOptions.DEFAULT.withRpc(false));
        assertTrue(eventually(() -> s.failAttempts.get() == 1));
    }

    @Test
    void aCallToNoServiceIsUnroutable() {
        UnroutableError e = assertThrows(UnroutableError.class, () -> proxy().add(add(1, 1)));
        assertNotEquals(null, e.messageId());
    }

    @Test
    void anUnansweredCallTimesOut() {
        serve(MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(60000));
        assertThrows(RpcTimeoutError.class, () -> proxy().slow(SlowRequest.newBuilder().setMs(2000).build(),
                CallOptions.DEFAULT.withTimeoutMs(100)));
    }

    @Test
    void aProcessingTimeoutIsRetriedThenAnsweredWithItsCode() {
        CalcService s = serve(MessageServiceOptions.DEFAULT.withProcessingTimeoutMs(50)
                .withRetry(RetryOptions.defaults().withMaxRetries(1).withRetryDelayMs(10)));
        RemoteError e = assertThrows(RemoteError.class,
                () -> proxy().slow(SlowRequest.newBuilder().setMs(5000).build()));
        assertEquals("PROCESSING_TIMEOUT", e.code());
        assertEquals(2, s.slowStarted.get());
        // The handler's signal fired: it stopped on its own.
        assertTrue(eventually(() -> s.slowAborted.get() == 2));
    }

    @Test
    void anInstanceNamedServiceIsReachedThroughItsName() {
        serve((c, o) -> new CalcService.Instance(c, o, "pbtest.Calc.player6"), MessageServiceOptions.DEFAULT, ctx);
        CalcProtobus.Proxy p = proxy("pbtest.Calc.player6");
        assertEquals("pbtest.Calc", p.contractServiceName());
        assertEquals("REQUEST.pbtest.Calc.player6.whoami", p.whoami(Nothing.getDefaultInstance()).getRoutingKey());
        assertTrue(broker.queueExists("pbtest.Calc.player6"));
    }

    @Test
    void theDynamicProxyCallsByName() {
        serve();
        ServiceProxy p = new ServiceProxy(ctx, "pbtest.Calc");
        p.init();
        Message request = ctx.factory().newMessage("pbtest.AddRequest")
                .setField(ctx.factory().type("pbtest.AddRequest").findFieldByName("a"), 2)
                .setField(ctx.factory().type("pbtest.AddRequest").findFieldByName("b"), 3).build();
        Message reply = p.call("add", request);
        assertEquals(5, reply.getField(reply.getDescriptorForType().findFieldByName("result")));
        assertThrows(UnknownMethodError.class, () -> p.call("nope", request));
        assertThrows(InvalidRequestError.class, () -> p.call("divide", request));
    }

    @Test
    void invalidCallOptionsAreRefusedBeforeAnythingIsSent() {
        serve();
        assertThrows(InvalidPriorityError.class, () -> proxy().add(add(1, 1), CallOptions.DEFAULT.withPriority(256)));
        assertThrows(InvalidMessageIdError.class, () -> proxy().add(add(1, 1), CallOptions.DEFAULT.withMessageId(" ")));
        assertThrows(InvalidMessageIdError.class,
                () -> proxy().add(add(1, 1), CallOptions.DEFAULT.withMessageId("é".repeat(128))));
    }

    @Test
    void aRequestWhoseBodyContradictsItsRoutingKeyIsRefused() {
        CalcService s = serve();
        byte[] request = Envelopes.encodeRequest(new Envelopes.Request("pbtest.Calc.fail", null,
                FailRequest.newBuilder().setId("x").build().toByteArray()));
        byte[] reply = ctx.publishMessage(request, "REQUEST.pbtest.Calc.add", CallOptions.DEFAULT);
        Envelopes.Response r = Envelopes.decodeResponse(reply);
        assertEquals("PROTOCOL_ERROR", r.error().code());
        assertEquals("pbtest.Calc.add", r.error().method());
        assertEquals(0, s.failAttempts.get());
    }

    @Test
    void anUndecodableEnvelopeIsAProtocolError() {
        serve();
        byte[] reply = ctx.publishMessage(new byte[] {0x0a, (byte) 0xff}, "REQUEST.pbtest.Calc.add",
                CallOptions.DEFAULT);
        assertEquals("PROTOCOL_ERROR", Envelopes.decodeResponse(reply).error().code());
    }

    @Test
    void aMaxPriorityQueueDeliversHigherPrioritiesFirst() throws Exception {
        CalcService s = serve(MessageServiceOptions.DEFAULT.withMaxPriority(2));
        assertEquals(2, broker.queueArguments("pbtest.Calc").get("x-max-priority"));
        // A plain queue's arguments stay empty.
        assertThrows(InvalidPriorityError.class,
                () -> new CalcService(ctx, MessageServiceOptions.DEFAULT.withMaxPriority(2).withLateAck(false)));
        assertEquals(3, proxy().add(add(1, 2), CallOptions.DEFAULT.withPriority(Config.PRIORITY_HIGH)).getResult());
        assertEquals(0, s.failAttempts.get());
    }
}
