# Getting started

From an empty directory to a service, a client, an event and a unit test. It
assumes Java 17+ and a broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

## 1. Lay out the project

```
app/
├── settings.gradle.kts
├── build.gradle.kts
└── src/
    ├── main/
    │   ├── proto/Calculator.proto
    │   └── java/app/
    │       ├── CalculatorService.java
    │       └── Client.java
    └── test/java/app/CalculatorServiceTest.java
```

## 2. Write the schema

```protobuf
// src/main/proto/Calculator.proto
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
}

message AddResponse {
  int32 result = 1;
}

message DivideRequest {
  double dividend = 1;
  double divisor = 2;
}

message DivideResponse {
  double quotient = 1;
}

// Published after every successful calculation.
message Calculated {
  string operation = 1;
  timestamp at = 2;
}
```

`timestamp` is a protobus custom type: milliseconds since the epoch, shared by
every port. It needs no import.

## 3. Build it

```kotlin
// build.gradle.kts
plugins {
    java
    application
    id("io.github.ariellaub.protobus") version "2.0.0"
}

repositories { mavenCentral() }

dependencies {
    implementation("io.github.ariellaub:protobus:2.0.0")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")

    testImplementation("org.junit.jupiter:junit-jupiter:5.14.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test { useJUnitPlatform() }
application { mainClass.set("app.CalculatorService") }
```

The plugin generates the Java for `src/main/proto` on every build. The package
`Calculator` becomes the Java package `calculator`, holding protoc's message
classes and `ServiceProtobus` with its `Base` and `Proxy`.

## 4. The service

<!-- doc-check: compile -->
```java
// src/main/java/app/CalculatorService.java
package app;

import calculator.AddRequest;
import calculator.AddResponse;
import calculator.Calculated;
import calculator.DivideRequest;
import calculator.DivideResponse;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.CallContext;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.CustomTypes;
import io.github.ariellaub.protobus.HandledError;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.RunnableService;
import java.time.Instant;

public class CalculatorService extends ServiceProtobus.Base {
    public CalculatorService(Context context, MessageServiceOptions options) {
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
            // Answered at once with this code, and never retried.
            throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
        }
        announce("divide");
        return DivideResponse.newBuilder().setQuotient(request.getDividend() / request.getDivisor()).build();
    }

    private void announce(String operation) {
        publishEvent(Calculated.newBuilder().setOperation(operation)
                .setAt(CustomTypes.timestamp(Instant.now())).build());
    }

    public static void main(String[] args) throws InterruptedException {
        Context context = new Context();
        context.init(System.getenv("AMQP_URL"));
        CalculatorService service = RunnableService.start(context, CalculatorService::new,
                MessageServiceOptions.DEFAULT.withMaxConcurrent(8), null);
        // Typed events, on the default topic EVENT.Calculator.Calculated.
        service.subscribeEvent(Calculated.class, (event, type, topic) ->
                System.out.println(event.getOperation() + " at " + CustomTypes.toInstant(event.getAt())));
        System.exit(RunnableService.awaitShutdown());
    }
}
```

`RunnableService.start` constructs the service, declares its queue
(`Calculator.Service`, bound to `REQUEST.Calculator.Service.*`), its retry
queue and its dead-letter queue, and starts consuming. On SIGINT or SIGTERM it
stops taking work, lets what is in flight finish, and closes.

## 5. The client

<!-- doc-check: compile -->
```java
// src/main/java/app/Client.java
package app;

import calculator.AddRequest;
import calculator.DivideRequest;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.RemoteError;

public class Client {
    public static void main(String[] args) {
        try (Context context = new Context()) {
            context.init(System.getenv("AMQP_URL"));
            ServiceProtobus.Proxy calculator = new ServiceProtobus.Proxy(context);
            calculator.init();

            System.out.println("2 + 3 = " + calculator.add(AddRequest.newBuilder().setA(2).setB(3).build()).getResult());
            try {
                calculator.divide(DivideRequest.newBuilder().setDividend(1).setDivisor(0).build());
            } catch (RemoteError e) {
                System.out.println(e.code() + ": " + e.getMessage()); // DIVISION_BY_ZERO: cannot divide by zero
            }
        }
    }
}
```

```bash
./gradlew installDist
build/install/app/bin/app &                          # the service
java -cp 'build/install/app/lib/*' app.Client        # the client
```

A service in another language calls `Calculator.Service` the same way, and a
Java client calls a TypeScript, Python, Go or C++ `Calculator.Service`
unchanged.

## 6. A unit test, without RabbitMQ

The in-memory broker implements what protobus relies on (exchanges, queues,
prefetch, acknowledgements, TTL, dead-lettering, confirms), so the service and
a client run in-process.

<!-- doc-check: compile -->
```java
// src/test/java/app/CalculatorServiceTest.java
package app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import calculator.AddRequest;
import calculator.DivideRequest;
import calculator.ServiceProtobus;
import io.github.ariellaub.protobus.Context;
import io.github.ariellaub.protobus.ContextOptions;
import io.github.ariellaub.protobus.MessageServiceOptions;
import io.github.ariellaub.protobus.RemoteError;
import io.github.ariellaub.protobus.testing.MemoryBroker;
import org.junit.jupiter.api.Test;

class CalculatorServiceTest {
    @Test
    void addsAndRefusesToDivideByZero() {
        try (MemoryBroker broker = new MemoryBroker();
             Context context = new Context(ContextOptions.DEFAULT.withTransport(broker))) {
            context.init("amqp://memory/");
            new CalculatorService(context, MessageServiceOptions.DEFAULT).init();

            ServiceProtobus.Proxy calculator = new ServiceProtobus.Proxy(context);
            calculator.init();
            assertEquals(5, calculator.add(AddRequest.newBuilder().setA(2).setB(3).build()).getResult());
            RemoteError e = assertThrows(RemoteError.class,
                    () -> calculator.divide(DivideRequest.newBuilder().setDivisor(0).build()));
            assertEquals("DIVISION_BY_ZERO", e.code());
        }
    }
}
```

## Next

- [Services](services.md): concurrency, retries, timeouts, priority, lifecycle.
- [Clients](clients.md): call options, async calls, errors.
- [Streaming](streaming.md) and [Events](events.md).
