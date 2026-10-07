package io.github.ariellaub.protobus;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import pbtest.AddRequest;
import pbtest.AddResponse;
import pbtest.CalcProtobus;
import pbtest.DivideRequest;
import pbtest.DivideResponse;
import pbtest.FailRequest;
import pbtest.Nothing;
import pbtest.SlowRequest;
import pbtest.Tick;
import pbtest.TickRequest;
import pbtest.Wallet;
import pbtest.Who;

/** Implements every rpc of pbtest.Calc except {@code unimplemented}, recording what it saw. */
public class CalcService extends CalcProtobus.Base {
    final AtomicInteger failAttempts = new AtomicInteger();
    final AtomicInteger slowStarted = new AtomicInteger();
    final AtomicInteger slowAborted = new AtomicInteger();
    final AtomicInteger yielded = new AtomicInteger();
    final AtomicBoolean stoppedEarly = new AtomicBoolean();
    final AtomicBoolean finished = new AtomicBoolean();
    final AtomicBoolean cleanedUp = new AtomicBoolean();
    final List<String> failMessageIds = new CopyOnWriteArrayList<>();

    public CalcService(Context context, MessageServiceOptions options) {
        super(context, options);
    }

    @Override
    public AddResponse add(AddRequest r, CallContext ctx) {
        return AddResponse.newBuilder().setResult(r.getA() + r.getB()).build();
    }

    @Override
    public DivideResponse divide(DivideRequest r, CallContext ctx) {
        if (r.getDivisor() == 0) throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
        return DivideResponse.newBuilder().setQuotient(r.getDividend() / r.getDivisor()).build();
    }

    @Override
    public Nothing fail(FailRequest r, CallContext ctx) {
        failAttempts.incrementAndGet();
        failMessageIds.add(ctx.messageId());
        if (r.getHandled()) throw new HandledError("refused " + r.getId(), "REFUSED");
        throw new IllegalStateException("boom " + r.getId());
    }

    @Override
    public Nothing slow(SlowRequest r, CallContext ctx) throws InterruptedException {
        slowStarted.incrementAndGet();
        if (ctx.signal().await(Duration.ofMillis(r.getMs()))) slowAborted.incrementAndGet();
        return Nothing.getDefaultInstance();
    }

    @Override
    public void ticks(TickRequest r, StreamWriter<Tick> out, CallContext ctx) throws InterruptedException {
        yielded.set(0);
        stoppedEarly.set(false);
        finished.set(false);
        for (int i = 0; i < r.getCount(); i++) {
            if (r.getFailAt() > 0 && i >= r.getFailAt()) {
                if (r.getUnhandled()) throw new IllegalStateException("stream broke");
                throw new HandledError("deliberate failure at chunk " + i, "TEST_FAIL");
            }
            if (ctx.signal().aborted()) {
                stoppedEarly.set(true);
                return;
            }
            out.write(Tick.newBuilder().setSeq(i).setPayload("chunk-" + i).build());
            yielded.incrementAndGet();
            if (r.getDelayMs() > 0 && ctx.signal().await(Duration.ofMillis(r.getDelayMs()))) {
                stoppedEarly.set(true);
                return;
            }
        }
        finished.set(true);
    }

    @Override
    public Who whoami(Nothing r, CallContext ctx) {
        return Who.newBuilder().setActor(ctx.actor()).setMessageId(ctx.messageId() == null ? "" : ctx.messageId())
                .setRoutingKey(ctx.routingKey()).setRedelivered(ctx.redelivered()).build();
    }

    @Override
    public Wallet echo(Wallet w, CallContext ctx) {
        return w;
    }

    @Override
    protected void cleanup() {
        cleanedUp.set(true);
    }

    /** The same service under an instance name. */
    public static class Instance extends CalcService {
        private final String name;

        public Instance(Context context, MessageServiceOptions options, String name) {
            super(context, options);
            this.name = name;
        }

        @Override
        public String serviceName() {
            return name;
        }
    }
}
