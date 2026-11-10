package dev.release.llm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static dev.release.llm.GatewayResult.Status.*;
import static dev.release.llm.LlmGateway.Circuit.*;
import static dev.release.llm.LlmProvider.FailureKind.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class GatewayCircuitTest {
    private static final Duration BUDGET = Duration.ofSeconds(5);
    private static final LlmProvider.Request REQUEST = new LlmProvider.Request("Synthetic evidence", 20);
    private static final long SECOND = Duration.ofSeconds(1).toNanos();

    private static GatewayConfig config(int failures, int attempts) {
        return new GatewayConfig(2, 2, 100, 1_000, Duration.ofSeconds(10), attempts,
                Duration.ofMillis(10), Duration.ofMillis(100), failures, Duration.ofSeconds(1));
    }
    private static LlmProvider.Response response(int usage) { return new LlmProvider.Response("synthetic finding", OptionalInt.of(usage)); }
    private static LlmGateway gateway(LlmProvider provider, GatewayConfig config, AtomicLong now) {
        return new LlmGateway(provider, config, now::get, (changed, nanos) -> changed.awaitNanos(nanos), () -> 0.5);
    }
    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Latch was not released");
    }

    @Test void thresholdOpensCircuitAndRejectsWithoutSpendingQuota() throws Exception {
        var now = new AtomicLong();
        var calls = new AtomicInteger();
        var gateway = gateway((request, remaining) -> {
            calls.incrementAndGet(); throw new LlmProvider.Failure(TRANSIENT);
        }, config(2, 1), now);
        assertEquals(RETRY_EXHAUSTED, gateway.generate(REQUEST, BUDGET).status());
        assertEquals(CLOSED, gateway.snapshot().circuit());
        assertEquals(RETRY_EXHAUSTED, gateway.generate(REQUEST, BUDGET).status());
        assertEquals(OPEN, gateway.snapshot().circuit());
        var rejected = gateway.generate(REQUEST, BUDGET);
        assertEquals(CIRCUIT_OPEN, rejected.status()); assertEquals(0, rejected.attempts());
        assertEquals(2, calls.get()); assertEquals(2, gateway.snapshot().requestsCharged());
        assertEquals(0, gateway.snapshot().admitted());
    }

    @Test void openCircuitStopsRetriesBeforeAttemptBudgetIsExhausted() throws Exception {
        var now = new AtomicLong();
        var gateway = gateway((request, remaining) -> { throw new LlmProvider.Failure(TRANSIENT); }, config(1, 3), now);
        var result = gateway.generate(REQUEST, BUDGET);
        assertEquals(CIRCUIT_OPEN, result.status()); assertEquals(1, result.attempts());
        assertEquals(0, gateway.snapshot().waiting()); assertEquals(0, gateway.snapshot().inFlight());
    }

    @Test void cooldownAllowsExactlyOneProbeAndSuccessClosesCircuit() throws Exception {
        var now = new AtomicLong();
        var calls = new AtomicInteger();
        var probeEntered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var gateway = gateway((request, remaining) -> {
            if (calls.incrementAndGet() == 1) throw new LlmProvider.Failure(TRANSIENT);
            probeEntered.countDown(); release.await(); return response(5);
        }, config(1, 1), now);
        gateway.generate(REQUEST, BUDGET);
        now.addAndGet(SECOND);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var probe = callers.submit(() -> gateway.generate(REQUEST, BUDGET)); await(probeEntered);
                assertEquals(HALF_OPEN, gateway.snapshot().circuit());
                for (int i = 0; i < 20; i++) assertEquals(CIRCUIT_OPEN, gateway.generate(REQUEST, BUDGET).status());
                assertEquals(2, calls.get()); assertEquals(1, gateway.snapshot().inFlight());
                release.countDown(); assertEquals(SUCCESS, probe.get(2, TimeUnit.SECONDS).status());
                assertEquals(CLOSED, gateway.snapshot().circuit());
                assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
            } finally { release.countDown(); }
        }
    }

    @Test void failedAndPermanentProbesReopenAndStartANewCooldown() throws Exception {
        var now = new AtomicLong();
        var calls = new AtomicInteger();
        var gateway = gateway((request, remaining) -> {
            int attempt = calls.incrementAndGet();
            if (attempt <= 2) throw new LlmProvider.Failure(TRANSIENT);
            if (attempt == 3) throw new LlmProvider.Failure(PERMANENT);
            return response(5);
        }, config(1, 1), now);
        gateway.generate(REQUEST, BUDGET);
        now.addAndGet(SECOND); gateway.generate(REQUEST, BUDGET);
        assertEquals(OPEN, gateway.snapshot().circuit());
        assertEquals(CIRCUIT_OPEN, gateway.generate(REQUEST, BUDGET).status());
        now.addAndGet(SECOND);
        assertEquals(PROVIDER_FAILED, gateway.generate(REQUEST, BUDGET).status());
        assertEquals(OPEN, gateway.snapshot().circuit());
        now.addAndGet(SECOND);
        assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
        assertEquals(CLOSED, gateway.snapshot().circuit());
    }

    @Test void interruptedProbeReopensAndDoesNotStrandRecoveryCapacity() throws Exception {
        var now = new AtomicLong();
        var calls = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var owner = new AtomicReference<Thread>();
        var gateway = gateway((request, remaining) -> {
            int attempt = calls.incrementAndGet();
            if (attempt == 1) throw new LlmProvider.Failure(TRANSIENT);
            if (attempt == 2) { entered.countDown(); new CountDownLatch(1).await(); }
            return response(5);
        }, config(1, 1), now);
        gateway.generate(REQUEST, BUDGET); now.addAndGet(SECOND);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var probe = callers.submit(() -> {
                owner.set(Thread.currentThread());
                try { gateway.generate(REQUEST, BUDGET); return false; }
                catch (InterruptedException e) { return Thread.currentThread().isInterrupted(); }
            });
            await(entered); owner.get().interrupt(); assertTrue(probe.get(2, TimeUnit.SECONDS));
            assertEquals(OPEN, gateway.snapshot().circuit()); assertEquals(0, gateway.snapshot().inFlight());
            assertEquals(0, gateway.snapshot().admitted());
            assertEquals(CIRCUIT_OPEN, gateway.generate(REQUEST, BUDGET).status());
            now.addAndGet(SECOND);
            assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
            assertEquals(CLOSED, gateway.snapshot().circuit());
        }
    }

    @Test void staleSuccessCannotCloseACircuitOpenedByAnotherCall() throws Exception {
        var now = new AtomicLong();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var gateway = gateway((request, remaining) -> {
            if (request.prompt().equals("slow")) { entered.countDown(); release.await(); return response(5); }
            throw new LlmProvider.Failure(TRANSIENT);
        }, config(1, 1), now);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var stale = callers.submit(() -> gateway.generate(new LlmProvider.Request("slow", 20), BUDGET));
                await(entered); gateway.generate(REQUEST, BUDGET);
                assertEquals(OPEN, gateway.snapshot().circuit());
                release.countDown(); assertEquals(SUCCESS, stale.get(2, TimeUnit.SECONDS).status());
                assertEquals(OPEN, gateway.snapshot().circuit());
                assertEquals(CIRCUIT_OPEN, gateway.generate(REQUEST, BUDGET).status());
            } finally { release.countDown(); }
        }
    }

    @Test void staleFailureCannotReopenARecoveredCircuit() throws Exception {
        var now = new AtomicLong();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var gateway = gateway((request, remaining) -> {
            if (request.prompt().equals("slow")) { entered.countDown(); release.await(); throw new LlmProvider.Failure(TRANSIENT); }
            if (request.prompt().equals("fail")) throw new LlmProvider.Failure(TRANSIENT);
            return response(5);
        }, config(1, 1), now);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var stale = callers.submit(() -> gateway.generate(new LlmProvider.Request("slow", 20), BUDGET));
                await(entered); gateway.generate(new LlmProvider.Request("fail", 20), BUDGET);
                now.addAndGet(SECOND);
                assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
                assertEquals(CLOSED, gateway.snapshot().circuit());
                release.countDown(); stale.get(2, TimeUnit.SECONDS);
                assertEquals(CLOSED, gateway.snapshot().circuit());
            } finally { release.countDown(); }
        }
    }

    @Test void successfulCallResetsConsecutiveTransientFailureCount() throws Exception {
        var now = new AtomicLong();
        var calls = new AtomicInteger();
        var gateway = gateway((request, remaining) -> {
            if (calls.incrementAndGet() == 2) return response(5);
            throw new LlmProvider.Failure(TRANSIENT);
        }, config(2, 1), now);
        gateway.generate(REQUEST, BUDGET); gateway.generate(REQUEST, BUDGET); gateway.generate(REQUEST, BUDGET);
        assertEquals(CLOSED, gateway.snapshot().circuit());
        gateway.generate(REQUEST, BUDGET); assertEquals(OPEN, gateway.snapshot().circuit());
    }

    @Test void lateUsageCannotRefundTheNextQuotaWindow() throws Exception {
        var now = new AtomicLong();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var configuration = new GatewayConfig(2, 1, 100, 100, Duration.ofSeconds(1),
                1, Duration.ofMillis(10), Duration.ofMillis(10), 3, Duration.ofSeconds(1));
        var gateway = gateway((request, remaining) -> {
            if (request.prompt().equals("old")) { entered.countDown(); release.await(); return response(0); }
            return response(request.estimatedTokens());
        }, configuration, now);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var old = callers.submit(() -> gateway.generate(new LlmProvider.Request("old", 80), BUDGET));
                await(entered); assertEquals(80, gateway.snapshot().tokensCharged());
                now.addAndGet(SECOND);
                assertEquals(SUCCESS, gateway.generate(new LlmProvider.Request("new", 70), BUDGET).status());
                release.countDown(); old.get(2, TimeUnit.SECONDS);
                assertEquals(70, gateway.snapshot().tokensCharged()); assertEquals(1, gateway.snapshot().requestsCharged());
                assertEquals(SUCCESS, gateway.generate(new LlmProvider.Request("new", 30), BUDGET).status());
                assertEquals(100, gateway.snapshot().tokensCharged());
            } finally { release.countDown(); }
        }
    }

    @Test void failedOrBlockedProviderCannotConsumeAnotherGatewaysCapacity() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var configuration = new GatewayConfig(1, 0, 100, 1_000, Duration.ofSeconds(10),
                1, Duration.ofMillis(10), Duration.ofMillis(10), 1, Duration.ofSeconds(1));
        var bad = new LlmGateway((request, remaining) -> {
            entered.countDown(); release.await(); throw new LlmProvider.Failure(TRANSIENT);
        }, configuration);
        var healthy = new LlmGateway((request, remaining) -> response(5), configuration);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var failing = callers.submit(() -> bad.generate(REQUEST, BUDGET)); await(entered);
                assertEquals(OVERLOADED, bad.generate(REQUEST, BUDGET).status());
                assertEquals(SUCCESS, healthy.generate(REQUEST, BUDGET).status());
                release.countDown(); failing.get(2, TimeUnit.SECONDS);
                assertEquals(OPEN, bad.snapshot().circuit());
                assertEquals(CIRCUIT_OPEN, bad.generate(REQUEST, BUDGET).status());
                assertEquals(SUCCESS, healthy.generate(REQUEST, BUDGET).status());
                assertEquals(CLOSED, healthy.snapshot().circuit()); assertEquals(2, healthy.snapshot().requestsCharged());
            } finally { release.countDown(); }
        }
    }
}
