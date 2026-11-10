package dev.release.llm;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** A latch-controlled saturation demonstration, not a load test or benchmark. */
public final class GatewayDemo {
    private GatewayDemo() {}

    public static void runOverload() throws Exception {
        var entered = new CountDownLatch(1);
        var queued = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var config = new GatewayConfig(1, 1, 100, 10_000, Duration.ofMinutes(1),
                1, Duration.ofMillis(10), Duration.ofMillis(100), 3, Duration.ofSeconds(1));
        var gateway = new LlmGateway((request, remaining) -> {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) throw new LlmProvider.Failure(LlmProvider.FailureKind.PERMANENT);
            return new LlmProvider.Response("Synthetic synthesis completed", OptionalInt.of(32));
        }, config, System::nanoTime, (condition, nanos) -> {
            queued.countDown();
            condition.awaitNanos(nanos);
        }, () -> 0.5);
        var request = new LlmProvider.Request("Investigate synthetic v42", 128);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var first = callers.submit(() -> gateway.generate(request, Duration.ofSeconds(4)));
                require(entered, "Provider did not start");
                var second = callers.submit(() -> gateway.generate(request, Duration.ofSeconds(4)));
                require(queued, "Second request did not queue");
                var saturated = gateway.snapshot();
                System.out.printf("Gateway limits: in-flight=1, waiting=1%nSaturated: in-flight=%d, waiting=%d%n",
                        saturated.inFlight(), saturated.waiting());
                var overflow = gateway.generate(request, Duration.ofSeconds(1));
                System.out.printf("Third request: %s, attempts=%d%n", overflow.status(), overflow.attempts());
                release.countDown();
                System.out.println("First request: " + first.get(2, TimeUnit.SECONDS).status());
                System.out.println("Second request: " + second.get(2, TimeUnit.SECONDS).status());
                var drained = gateway.snapshot();
                System.out.printf("After completion: admitted=%d, in-flight=%d, waiting=%d%n",
                        drained.admitted(), drained.inFlight(), drained.waiting());
            } finally { release.countDown(); }
        }
    }

    private static void require(CountDownLatch latch, String message) throws InterruptedException {
        if (!latch.await(2, TimeUnit.SECONDS)) throw new IllegalStateException(message);
    }
}
