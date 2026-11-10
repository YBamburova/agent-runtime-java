package dev.release.llm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static dev.release.llm.GatewayResult.Status.*;
import static dev.release.llm.LlmGateway.Circuit.*;
import static dev.release.llm.LlmProvider.FailureKind.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class LlmGatewayTest {
    private static final Duration BUDGET = Duration.ofSeconds(5);
    private static final LlmProvider.Request REQUEST = request(20);

    private static LlmProvider.Request request(int tokens) { return new LlmProvider.Request("Synthetic v42 evidence", tokens); }
    private static LlmProvider.Response response(int tokens) { return new LlmProvider.Response("synthetic finding", OptionalInt.of(tokens)); }
    private static GatewayConfig config(int active, int waiting, int requests, int tokens, int attempts, int failures) {
        return new GatewayConfig(active, waiting, requests, tokens, Duration.ofSeconds(1), attempts,
                Duration.ofMillis(100), Duration.ofMillis(250), failures, Duration.ofSeconds(1));
    }
    private static GatewayConfig defaults() { return config(2, 2, 100, 1_000, 3, 10); }
    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Latch was not released");
    }

    private static final class PolicyTime {
        final AtomicLong now = new AtomicLong();
        final List<Long> waits = new ArrayList<>();
        LlmGateway gateway(LlmProvider provider, GatewayConfig config) {
            return new LlmGateway(provider, config, now::get, (changed, nanos) -> {
                waits.add(nanos);
                now.addAndGet(nanos);
            }, () -> 0.5);
        }
    }

    @Test void fakeProviderIsDeterministicAndDoesNotInventMissingEvidence() throws Exception {
        var gateway = new LlmGateway(new FakeLlmProvider(), GatewayConfig.defaults());
        var evidence = new LlmProvider.Request("logs [required] SUCCESS: synthetic\ndeployments [required] SUCCESS: synthetic", 256);
        var first = gateway.generate(evidence, BUDGET);
        var second = gateway.generate(evidence, BUDGET);
        assertEquals(first, second);
        assertEquals(SUCCESS, first.status());
        assertTrue(first.text().contains("controlled replay"));
        assertTrue(gateway.generate(REQUEST, BUDGET).text().contains("incomplete"));
        assertEquals(64 * 3, gateway.snapshot().tokensCharged());
    }

    @Test void boundsActiveCallsWaitingAndAdmissionAndRejectsWithoutCallingProvider() throws Exception {
        var entered = new CountDownLatch(2);
        var queued = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var active = new AtomicInteger();
        var peak = new AtomicInteger();
        var calls = new AtomicInteger();
        var seen = ConcurrentHashMap.<Thread>newKeySet();
        var gateway = new LlmGateway((request, remaining) -> {
            assertTrue(Thread.currentThread().isVirtual());
            calls.incrementAndGet();
            peak.accumulateAndGet(active.incrementAndGet(), Math::max);
            entered.countDown();
            try { release.await(); return response(7); }
            finally { active.decrementAndGet(); }
        }, defaults(), System::nanoTime, (changed, nanos) -> {
            if (seen.add(Thread.currentThread())) queued.countDown();
            changed.awaitNanos(nanos);
        }, () -> 0.5);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var a = callers.submit(() -> gateway.generate(REQUEST, BUDGET));
                var b = callers.submit(() -> gateway.generate(REQUEST, BUDGET));
                await(entered);
                var c = callers.submit(() -> gateway.generate(REQUEST, BUDGET));
                var d = callers.submit(() -> gateway.generate(REQUEST, BUDGET));
                await(queued);
                var snapshot = gateway.snapshot();
                assertEquals(2, snapshot.inFlight()); assertEquals(2, snapshot.waiting()); assertEquals(4, snapshot.admitted());
                for (int i = 0; i < 50; i++) {
                    var rejected = gateway.generate(REQUEST, BUDGET);
                    assertEquals(OVERLOADED, rejected.status()); assertEquals(0, rejected.attempts());
                }
                assertEquals(2, calls.get());
                release.countDown();
                for (var result : List.of(a, b, c, d)) assertEquals(SUCCESS, result.get(2, TimeUnit.SECONDS).status());
                assertEquals(2, peak.get()); assertEquals(4, calls.get());
                assertEmpty(gateway);
            } finally { release.countDown(); }
        }
    }

    @Test void requestQuotaStopsDispatchUntilNextWindow() throws Exception {
        var time = new PolicyTime();
        var dispatchedAt = new ArrayList<Long>();
        var gateway = time.gateway((request, remaining) -> { dispatchedAt.add(time.now.get()); return response(5); },
                config(1, 1, 1, 1_000, 1, 10));
        assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
        var timeout = gateway.generate(REQUEST, Duration.ofMillis(500));
        assertEquals(DEADLINE_EXCEEDED, timeout.status()); assertEquals(0, timeout.attempts());
        assertEquals(1, gateway.snapshot().requestsCharged());
        assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
        assertEquals(List.of(0L, Duration.ofSeconds(1).toNanos()), dispatchedAt);
        assertEquals(List.of(Duration.ofMillis(500).toNanos(), Duration.ofMillis(500).toNanos()), time.waits);
        assertEmpty(gateway);
    }

    @Test void usageReconciliationRefundsOnlyTokenEstimatesNotRequestQuota() throws Exception {
        var time = new PolicyTime();
        var gateway = time.gateway((request, remaining) -> response(20), config(1, 1, 2, 100, 1, 10));
        assertEquals(SUCCESS, gateway.generate(request(80), BUDGET).status());
        assertEquals(20, gateway.snapshot().tokensCharged());
        assertEquals(SUCCESS, gateway.generate(request(80), BUDGET).status());
        assertEquals(40, gateway.snapshot().tokensCharged()); assertEquals(2, gateway.snapshot().requestsCharged());
        assertTrue(time.waits.isEmpty());
    }

    @Test void unknownUsageRetainsReservationAndCannotBypassTokenQuota() throws Exception {
        var time = new PolicyTime();
        var calls = new AtomicInteger();
        var gateway = time.gateway((request, remaining) -> {
            calls.incrementAndGet(); return new LlmProvider.Response("unknown usage", OptionalInt.empty());
        }, config(1, 1, 100, 100, 1, 10));
        gateway.generate(request(80), BUDGET);
        assertEquals(80, gateway.snapshot().tokensCharged());
        assertEquals(DEADLINE_EXCEEDED, gateway.generate(request(30), Duration.ofMillis(100)).status());
        assertEquals(1, calls.get()); assertEquals(80, gateway.snapshot().tokensCharged());
        assertEmpty(gateway);
    }

    @Test void underestimatedTokensCreateDebtUntilWindowReset() throws Exception {
        var time = new PolicyTime();
        var calls = new AtomicInteger();
        var gateway = time.gateway((request, remaining) -> response(calls.incrementAndGet() == 1 ? 120 : 20),
                config(1, 1, 100, 100, 1, 10));
        gateway.generate(request(10), BUDGET);
        assertEquals(120, gateway.snapshot().tokensCharged());
        assertEquals(SUCCESS, gateway.generate(request(1), BUDGET).status());
        assertEquals(Duration.ofSeconds(1).toNanos(), time.now.get());
        assertEquals(20, gateway.snapshot().tokensCharged());
    }

    @Test void retriesReserveEachAttemptAndUseExponentialJitteredBackoff() throws Exception {
        var time = new PolicyTime();
        var calls = new AtomicInteger();
        var remainingBudgets = new ArrayList<Duration>();
        var gateway = time.gateway((request, remaining) -> {
            remainingBudgets.add(remaining);
            if (calls.incrementAndGet() < 3) throw new LlmProvider.Failure(TRANSIENT);
            return response(7);
        }, defaults());
        var result = gateway.generate(REQUEST, BUDGET);
        assertEquals(SUCCESS, result.status()); assertEquals(3, result.attempts());
        assertEquals(List.of(Duration.ofMillis(50).toNanos(), Duration.ofMillis(100).toNanos()), time.waits);
        assertEquals(List.of(BUDGET, BUDGET.minusMillis(50), BUDGET.minusMillis(150)), remainingBudgets);
        assertEquals(3, gateway.snapshot().requestsCharged()); assertEquals(47, gateway.snapshot().tokensCharged());
        assertEmpty(gateway);
    }

    @Test void attemptBudgetAndBackoffCapAreEnforced() throws Exception {
        var time = new PolicyTime();
        var gateway = time.gateway((request, remaining) -> { throw new LlmProvider.Failure(TRANSIENT); },
                config(1, 1, 100, 1_000, 4, 10));
        var result = gateway.generate(REQUEST, BUDGET);
        assertEquals(RETRY_EXHAUSTED, result.status()); assertEquals(4, result.attempts());
        assertEquals(List.of(50_000_000L, 100_000_000L, 125_000_000L), time.waits);
        assertEquals(4, gateway.snapshot().requestsCharged()); assertEquals(80, gateway.snapshot().tokensCharged());
        assertEmpty(gateway);
    }

    @Test void jitterSamplesChangeDelayWithinTheExponentialCap() throws Exception {
        var time = new PolicyTime();
        var samples = new double[] {0.0, 0.75, 0.999};
        var index = new AtomicInteger();
        var gateway = new LlmGateway((request, remaining) -> { throw new LlmProvider.Failure(TRANSIENT); },
                config(1, 1, 100, 1_000, 4, 10), time.now::get,
                (changed, nanos) -> { time.waits.add(nanos); time.now.addAndGet(nanos); },
                () -> samples[index.getAndIncrement()]);
        assertEquals(RETRY_EXHAUSTED, gateway.generate(REQUEST, BUDGET).status());
        assertEquals(List.of(1L, 150_000_000L, 249_750_000L), time.waits);
    }

    @Test void retriesAlsoWaitForFreshRequestQuota() throws Exception {
        var time = new PolicyTime();
        var dispatchedAt = new ArrayList<Long>();
        var gateway = time.gateway((request, remaining) -> {
            dispatchedAt.add(time.now.get());
            if (dispatchedAt.size() == 1) throw new LlmProvider.Failure(TRANSIENT);
            return response(5);
        }, config(1, 1, 1, 1_000, 2, 10));
        assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
        assertEquals(List.of(0L, 1_000_000_000L), dispatchedAt);
        assertEquals(1, gateway.snapshot().requestsCharged());
        assertEquals(5, gateway.snapshot().tokensCharged());
    }

    @Test void permanentAndUnclassifiedFailuresDoNotRetryOrOpenCircuit() throws Exception {
        var time = new PolicyTime();
        var permanent = time.gateway((request, remaining) -> { throw new LlmProvider.Failure(PERMANENT); },
                config(1, 1, 100, 1_000, 3, 1));
        var result = permanent.generate(REQUEST, BUDGET);
        assertEquals(PROVIDER_FAILED, result.status()); assertEquals(1, result.attempts());
        assertEquals(CLOSED, permanent.snapshot().circuit()); assertTrue(time.waits.isEmpty());
        var bug = time.gateway((request, remaining) -> { throw new IllegalStateException("secret prompt"); }, defaults());
        var failure = bug.generate(REQUEST, BUDGET);
        assertEquals(PROVIDER_FAILED, failure.status()); assertEquals(1, failure.attempts());
        assertFalse(failure.toString().contains("secret")); assertEmpty(bug);
    }

    @Test void retryIsSkippedWhenBackoffCannotFitDeadline() throws Exception {
        var time = new PolicyTime();
        var gateway = time.gateway((request, remaining) -> { throw new LlmProvider.Failure(TRANSIENT); }, defaults());
        var result = gateway.generate(REQUEST, Duration.ofMillis(40));
        assertEquals(DEADLINE_EXCEEDED, result.status()); assertEquals(1, result.attempts());
        assertTrue(time.waits.isEmpty()); assertEmpty(gateway);
    }

    @Test void lateProviderResponseIsDiscardedButKnownUsageIsCharged() throws Exception {
        var time = new PolicyTime();
        var gateway = time.gateway((request, remaining) -> {
            time.now.addAndGet(remaining.toNanos()); return response(9);
        }, defaults());
        var result = gateway.generate(REQUEST, Duration.ofMillis(10));
        assertEquals(DEADLINE_EXCEEDED, result.status()); assertEquals("", result.text());
        assertEquals(9, gateway.snapshot().tokensCharged()); assertEmpty(gateway);
    }

    @Test void waitingDeadlineExpiresWithoutDispatchOrCapacityLeak() throws Exception {
        var time = new PolicyTime();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var gateway = time.gateway((request, remaining) -> {
            calls.incrementAndGet(); entered.countDown(); release.await(); return response(5);
        }, config(1, 1, 100, 1_000, 1, 10));
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var holder = callers.submit(() -> gateway.generate(REQUEST, BUDGET));
                await(entered);
                var expired = gateway.generate(REQUEST, Duration.ofMillis(100));
                assertEquals(DEADLINE_EXCEEDED, expired.status()); assertEquals(0, expired.attempts());
                var snapshot = gateway.snapshot();
                assertEquals(1, snapshot.admitted()); assertEquals(1, snapshot.inFlight()); assertEquals(0, snapshot.waiting());
                assertEquals(1, calls.get()); assertEquals(1, snapshot.requestsCharged());
                release.countDown(); assertEquals(SUCCESS, holder.get(2, TimeUnit.SECONDS).status());
                assertEmpty(gateway);
            } finally { release.countDown(); }
        }
    }

    @Test void waitingCancellationPreservesInterruptAndFreesAdmission() throws Exception {
        var entered = new CountDownLatch(1);
        var queued = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var queueThread = new AtomicReference<Thread>();
        var gateway = new LlmGateway((request, remaining) -> {
            entered.countDown(); release.await(); return response(5);
        }, config(1, 1, 100, 1_000, 1, 10), System::nanoTime,
                (changed, nanos) -> { queued.countDown(); changed.awaitNanos(nanos); }, () -> 0.5);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var holder = callers.submit(() -> gateway.generate(REQUEST, BUDGET)); await(entered);
                var cancelled = callers.submit(() -> {
                    queueThread.set(Thread.currentThread());
                    try { gateway.generate(REQUEST, BUDGET); return false; }
                    catch (InterruptedException e) { return Thread.currentThread().isInterrupted(); }
                });
                await(queued); queueThread.get().interrupt();
                assertTrue(cancelled.get(2, TimeUnit.SECONDS));
                assertEquals(0, gateway.snapshot().waiting()); assertEquals(1, gateway.snapshot().admitted());
                release.countDown(); holder.get(2, TimeUnit.SECONDS);
                assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status()); assertEmpty(gateway);
            } finally { release.countDown(); }
        }
    }

    @Test void cancellationDuringBackoffHoldsNoProviderPermitAndDoesNotRetry() throws Exception {
        var queued = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var owner = new AtomicReference<Thread>();
        var config = new GatewayConfig(1, 1, 100, 1_000, Duration.ofSeconds(10),
                3, Duration.ofSeconds(1), Duration.ofSeconds(1), 10, Duration.ofSeconds(1));
        var gateway = new LlmGateway((request, remaining) -> {
            if (calls.incrementAndGet() == 1) throw new LlmProvider.Failure(TRANSIENT);
            return response(5);
        }, config, System::nanoTime, (changed, nanos) -> {
            queued.countDown(); changed.awaitNanos(nanos);
        }, () -> 0.5);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cancelled = callers.submit(() -> {
                owner.set(Thread.currentThread());
                try { gateway.generate(REQUEST, BUDGET); return false; }
                catch (InterruptedException e) { return Thread.currentThread().isInterrupted(); }
            });
            await(queued);
            assertEquals(0, gateway.snapshot().inFlight()); assertEquals(1, gateway.snapshot().waiting());
            owner.get().interrupt(); assertTrue(cancelled.get(2, TimeUnit.SECONDS));
            assertEquals(1, calls.get()); assertEquals(20, gateway.snapshot().tokensCharged()); assertEmpty(gateway);
            assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
        }
    }

    @Test void cooperativeProviderCancellationReleasesCapacityAndRunsCleanup() throws Exception {
        var entered = new CountDownLatch(1);
        var cleaned = new CountDownLatch(1);
        var owner = new AtomicReference<Thread>();
        var calls = new AtomicInteger();
        var gateway = new LlmGateway((request, remaining) -> {
            if (calls.incrementAndGet() > 1) return response(5);
            try { entered.countDown(); new CountDownLatch(1).await(); return response(5); }
            finally { cleaned.countDown(); }
        }, defaults());
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cancelled = callers.submit(() -> {
                owner.set(Thread.currentThread());
                try { gateway.generate(REQUEST, BUDGET); return false; }
                catch (InterruptedException e) { return Thread.currentThread().isInterrupted(); }
            });
            await(entered); owner.get().interrupt(); await(cleaned);
            assertTrue(cancelled.get(2, TimeUnit.SECONDS)); assertEmpty(gateway);
            assertEquals(20, gateway.snapshot().tokensCharged());
            assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
        }
    }

    @Test void uncooperativeProviderRetainsCapacityUntilActualExit() throws Exception {
        var entered = new CountDownLatch(1);
        var ignored = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var gateway = new LlmGateway((request, remaining) -> {
            if (calls.incrementAndGet() > 1) return response(5);
            entered.countDown();
            boolean done = false;
            while (!done) {
                try { release.await(); done = true; }
                catch (InterruptedException swallowed) { ignored.countDown(); }
            }
            return response(5);
        }, config(1, 0, 100, 1_000, 1, 10));
        var result = new FutureTask<>(() -> gateway.generate(REQUEST, BUDGET));
        Thread.ofVirtual().start(() -> { try { result.run(); } finally { exited.countDown(); } });
        try {
            await(entered); result.cancel(true); await(ignored);
            assertTrue(result.isDone()); assertEquals(1, gateway.snapshot().inFlight()); assertEquals(1, gateway.snapshot().admitted());
            assertEquals(OVERLOADED, gateway.generate(REQUEST, BUDGET).status());
        } finally { release.countDown(); }
        await(exited); assertEmpty(gateway);
        assertEquals(SUCCESS, gateway.generate(REQUEST, BUDGET).status());
    }

    @Test void zeroWaitingLimitRejectsQuotaWaitingInsteadOfRetainingWork() throws Exception {
        var time = new PolicyTime();
        var gateway = time.gateway((request, remaining) -> response(5), config(1, 0, 1, 1_000, 1, 10));
        gateway.generate(REQUEST, BUDGET);
        assertEquals(OVERLOADED, gateway.generate(REQUEST, BUDGET).status());
        assertTrue(time.waits.isEmpty()); assertEmpty(gateway);
    }

    @Test void quotaBlockedWaitingIsBoundedEvenWhenNoProviderIsRunning() throws Exception {
        var queued = new CountDownLatch(1);
        var owner = new AtomicReference<Thread>();
        var gateway = new LlmGateway((request, remaining) -> response(5),
                config(2, 1, 1, 1_000, 1, 10), System::nanoTime,
                (changed, nanos) -> { queued.countDown(); changed.awaitNanos(nanos); }, () -> 0.5);
        gateway.generate(REQUEST, BUDGET);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var waiting = callers.submit(() -> {
                owner.set(Thread.currentThread());
                try { gateway.generate(REQUEST, BUDGET); return false; }
                catch (InterruptedException e) { return Thread.currentThread().isInterrupted(); }
            });
            await(queued);
            try {
                assertEquals(0, gateway.snapshot().inFlight()); assertEquals(1, gateway.snapshot().waiting());
                assertEquals(OVERLOADED, gateway.generate(REQUEST, BUDGET).status());
                assertEquals(1, gateway.snapshot().requestsCharged());
            } finally { owner.get().interrupt(); }
            assertTrue(waiting.get(2, TimeUnit.SECONDS)); assertEmpty(gateway);
        }
    }

    @Test void spuriousWakeupCannotBypassQuotaOrDeadline() throws Exception {
        var now = new AtomicLong();
        var waits = new AtomicInteger();
        var calls = new AtomicInteger();
        var gateway = new LlmGateway((request, remaining) -> { calls.incrementAndGet(); return response(5); },
                config(1, 1, 1, 1_000, 1, 10), now::get, (changed, nanos) -> {
                    if (waits.incrementAndGet() > 1) now.addAndGet(nanos);
                }, () -> 0.5);
        gateway.generate(REQUEST, BUDGET);
        assertEquals(DEADLINE_EXCEEDED, gateway.generate(REQUEST, Duration.ofMillis(100)).status());
        assertEquals(2, waits.get()); assertEquals(1, calls.get()); assertEmpty(gateway);
    }

    @Test void invalidInputsAndOversizedReservationsDoNotReachProvider() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new LlmProvider.Request(" ", 1));
        assertThrows(IllegalArgumentException.class, () -> new LlmProvider.Request("x".repeat(65_537), 1));
        assertThrows(IllegalArgumentException.class, () -> request(0));
        assertThrows(IllegalArgumentException.class, () -> response(-1));
        assertThrows(IllegalArgumentException.class, () -> config(0, 1, 100, 100, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> config(1, -1, 100, 100, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> config(1, 1, 0, 100, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> config(1, 1, 100, 100, 11, 1));
        assertThrows(IllegalArgumentException.class, () -> new GatewayConfig(1, 1, 1, 100, Duration.ofSeconds(1),
                1, Duration.ofSeconds(2), Duration.ofSeconds(1), 1, Duration.ofSeconds(1)));
        var gateway = new LlmGateway((request, remaining) -> { fail("Invalid input must not dispatch"); return response(0); }, defaults());
        var invalid = gateway.generate(request(1_001), BUDGET);
        assertEquals(INVALID_REQUEST, invalid.status()); assertEquals(0, invalid.attempts());
        assertThrows(IllegalArgumentException.class, () -> gateway.generate(REQUEST, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> gateway.generate(REQUEST, Duration.ofDays(2)));
        assertEquals(0, gateway.snapshot().requestsCharged()); assertEmpty(gateway);
    }

    @Test void alreadyInterruptedCallerDoesNotReserveCapacity() throws Exception {
        var gateway = new LlmGateway((request, remaining) -> { fail("Interrupted input must not dispatch"); return response(0); }, defaults());
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = callers.submit(() -> {
                Thread.currentThread().interrupt();
                try { gateway.generate(REQUEST, BUDGET); return false; }
                catch (InterruptedException e) { return Thread.currentThread().isInterrupted(); }
            });
            assertTrue(result.get(2, TimeUnit.SECONDS)); assertEmpty(gateway);
            assertEquals(0, gateway.snapshot().requestsCharged());
        }
    }

    private static void assertEmpty(LlmGateway gateway) {
        var snapshot = gateway.snapshot();
        assertEquals(0, snapshot.admitted()); assertEquals(0, snapshot.inFlight()); assertEquals(0, snapshot.waiting());
    }
}
