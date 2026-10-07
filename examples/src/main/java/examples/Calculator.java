package examples;

import calculator.AddRequest;
import calculator.AddResponse;
import calculator.Calculated;
import calculator.DivideRequest;
import calculator.DivideResponse;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.CallOptions;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.CustomTypes;
import io.github.ariellaub.protobus.HandledError;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.RemoteError;
import io.github.ariellaub.protobus.RunnableService;
import java.time.Instant;

/**
 * The protobus getting-started example: a service, a client, and an event, in one
 * program.
 *
 * <pre>
 *   docker compose up -d --wait                          # RabbitMQ on 127.0.0.1:25672
 *   export AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
 *   ./gradlew :examples:runCalculator -Pargs=server      # in one terminal
 *   ./gradlew :examples:runCalculator -Pargs=client      # in another
 *   ./gradlew :examples:runCalculator                    # or both at once
 * </pre>
 *
 * The schema (proto/Calculator.proto) is an ordinary protobus schema, shared
 * as-is with TypeScript, Python, Go and C++ services.
 */
public final class Calculator {
    private Calculator() {}

    /** Implements Calculator.Service. An rpc left unimplemented answers PROTOCOL_ERROR. */
    static final class CalculatorService extends ServiceProtobus.Base {
        CalculatorService(Context context, MessageServiceOptions options) {
            super(context, options);
        }

        @Override
        public AddResponse add(AddRequest request, CallContext context) {
            announce("add");
            return AddResponse.newBuilder().setResult(request.getA() + request.getB()).build();
        }

        @Override
        public DivideResponse divide(DivideRequest request, CallContext context) {
            if (request.getDivisor() == 0) {
                // A HandledError is an answer, not a failure: the caller gets it at
                // once and it is never retried. Anything else thrown is retried.
                throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
            }
            announce("divide");
            return DivideResponse.newBuilder().setQuotient(request.getDividend() / request.getDivisor()).build();
        }

        private void announce(String operation) {
            // Events fan out: every subscribing service gets one.
            publishEvent(Calculated.newBuilder().setOperation(operation)
                    .setAt(CustomTypes.timestamp(Instant.now())).build());
        }
    }

    static void call(Context context) {
        ServiceProtobus.Proxy calculator = new ServiceProtobus.Proxy(context);
        calculator.init();

        AddResponse sum = calculator.add(AddRequest.newBuilder().setA(20).setB(22).build());
        System.out.println("20 + 22 = " + sum.getResult());

        DivideResponse quotient = calculator.divide(DivideRequest.newBuilder().setDividend(1).setDivisor(4).build(),
                CallOptions.DEFAULT.withActor("example-client"));
        System.out.println("1 / 4 = " + quotient.getQuotient());

        try {
            calculator.divide(DivideRequest.newBuilder().setDividend(1).setDivisor(0).build());
        } catch (RemoteError e) {
            System.out.println("1 / 0 -> " + e.getMessage() + " (" + e.code() + ")");
        }
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "both";
        String url = System.getenv().getOrDefault("AMQP_URL", "amqp://guest:guest@127.0.0.1:25672/");
        Context context = new Context();
        context.init(url);

        if (mode.equals("client")) {
            call(context);
            context.close();
            return;
        }

        CalculatorService service = RunnableService.start(context, CalculatorService::new,
                MessageServiceOptions.DEFAULT.withMaxConcurrent(8), null);
        // Subscribers receive typed events, on the default topic EVENT.Calculator.Calculated.
        service.subscribeEvent(Calculated.class, (event, type, topic) ->
                System.out.println("event: " + event.getOperation() + " at " + CustomTypes.toInstant(event.getAt())));

        if (mode.equals("both")) {
            call(context);
            // Let the events arrive before shutting down.
            Thread.sleep(300);
            RunnableService.requestShutdown();
        } else {
            System.out.println("calculator service running; Ctrl-C to stop");
        }
        // Returns once SIGINT/SIGTERM (or requestShutdown) has shut the service down gracefully.
        System.exit(RunnableService.awaitShutdown());
    }
}
