package dev.release.jobs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static dev.release.jobs.Job.State.*;
import static org.junit.jupiter.api.Assertions.*;

/** Shared behavioral contract. Passing the mock subclass does not establish any SQL concurrency claim. */
@Timeout(15)
abstract class JobStoreContract {
    protected JobStore store;
    protected static final Duration LEASE = Duration.ofSeconds(30);
    protected static final Job.Spec SPEC = new Job.Spec(Job.Scenario.HEALTHY, Duration.ofSeconds(2), 3);
    protected abstract void advance(Duration elapsed) throws Exception;
    protected UUID enqueue() throws Exception { return store.enqueue(UUID.randomUUID().toString(), SPEC); }
    protected Job.Claim claim() throws Exception { return store.claim("worker-a", LEASE).orElseThrow(); }

    @Test void sameSubmissionKeyIsIdempotentAndConflictingPayloadIsRejected() throws Exception {
        var id = store.enqueue("same-key", SPEC);
        assertEquals(id, store.enqueue("same-key", SPEC));
        assertThrows(IllegalArgumentException.class, () -> store.enqueue("same-key",
                new Job.Spec(Job.Scenario.PARTIAL, SPEC.budget(), SPEC.maxAttempts())));
        assertEquals(id, claim().jobId()); assertTrue(store.claim("other", LEASE).isEmpty());
    }
    @Test void competingEnqueuesReturnOneJob() throws Exception {
        var race = new CountDownLatch(1);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<java.util.concurrent.Future<UUID>>();
            for (int i = 0; i < 8; i++) futures.add(callers.submit(() -> {
                assertTrue(race.await(2, TimeUnit.SECONDS)); return store.enqueue("competing", SPEC);
            }));
            race.countDown();
            var ids = new HashSet<UUID>();
            for (var future : futures) ids.add(future.get(5, TimeUnit.SECONDS));
            assertEquals(1, ids.size()); claim(); assertTrue(store.claim("other", LEASE).isEmpty());
        }
    }
    @Test void competingClaimersDoNotClaimTheSameLiveAttempt() throws Exception {
        var expected = new HashSet<UUID>();
        for (int i = 0; i < 20; i++) expected.add(enqueue());
        var claimed = ConcurrentHashMap.<UUID>newKeySet();
        var race = new CountDownLatch(1);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 4; i++) {
                String worker = "claimant-" + i;
                futures.add(callers.submit(() -> {
                    assertTrue(race.await(2, TimeUnit.SECONDS));
                    while (true) {
                        var claim = store.claim(worker, LEASE);
                        if (claim.isEmpty()) return null;
                        assertTrue(claimed.add(claim.get().jobId()), "Live attempt was claimed twice");
                        assertEquals(1, claim.get().attempt());
                    }
                }));
            }
            race.countDown(); for (var future : futures) future.get(5, TimeUnit.SECONDS);
        }
        assertEquals(expected, claimed);
        for (UUID id : claimed) assertEquals(1, store.attempts(id).size());
    }
    @Test void expiredAttemptIsRecoveredAndEveryOldWriteIsFenced() throws Exception {
        var id = enqueue(); var old = claim(); advance(LEASE.plusSeconds(1));
        var replacement = store.claim("worker-b", LEASE).orElseThrow();
        assertEquals(2, replacement.attempt());
        assertFalse(store.renew(old, LEASE)); assertFalse(store.complete(old, "stale"));
        assertFalse(store.fail(old, true, Duration.ofSeconds(1), Job.FailureCode.TASK_FAILED));
        assertTrue(store.result(id).isEmpty());
        assertTrue(store.complete(replacement, "new report"));
        assertEquals(2, store.result(id).orElseThrow().attempt());
        assertEquals(List.of("EXPIRED", "SUCCEEDED"), store.attempts(id).stream().map(Job.Attempt::state).toList());
    }
    @Test void expiredLeaseCannotBeRevivedEvenBeforeReclaim() throws Exception {
        enqueue(); var old = claim(); advance(LEASE.plusSeconds(1));
        assertFalse(store.renew(old, LEASE)); assertFalse(store.complete(old, "late"));
        assertFalse(store.fail(old, false, Duration.ofSeconds(1), Job.FailureCode.TASK_FAILED));
        assertEquals(2, store.claim("replacement", LEASE).orElseThrow().attempt());
    }
    @Test void renewalKeepsOwnershipWhileLeaseIsValid() throws Exception {
        enqueue(); var owner = claim(); advance(Duration.ofSeconds(20));
        assertTrue(store.renew(owner, LEASE)); advance(Duration.ofSeconds(20));
        assertTrue(store.claim("other", LEASE).isEmpty()); assertTrue(store.complete(owner, "renewed report"));
    }
    @Test void completionIsIdempotentAndNeverOverwritesADifferentEffect() throws Exception {
        var id = enqueue(); var owner = claim();
        assertTrue(store.complete(owner, "one report")); assertTrue(store.complete(owner, "one report"));
        assertFalse(store.complete(owner, "changed report")); assertFalse(store.cancel(id));
        assertFalse(store.fail(owner, true, Duration.ofSeconds(1), Job.FailureCode.TASK_FAILED));
        assertEquals("one report", store.result(id).orElseThrow().report());
        assertEquals(SUCCEEDED, store.find(id).orElseThrow().state()); assertEquals(1, store.attempts(id).size());
    }
    @Test void delayedRetriesConsumeTheAttemptBudget() throws Exception {
        var id = store.enqueue("retry", new Job.Spec(Job.Scenario.HEALTHY, SPEC.budget(), 2));
        var first = claim(); assertTrue(store.fail(first, true, Duration.ofSeconds(10), Job.FailureCode.TASK_FAILED));
        assertEquals(QUEUED, store.find(id).orElseThrow().state()); assertTrue(store.claim("early", LEASE).isEmpty());
        advance(Duration.ofSeconds(11)); var second = claim(); assertEquals(2, second.attempt());
        assertTrue(store.fail(second, true, Duration.ofSeconds(10), Job.FailureCode.TASK_FAILED));
        advance(Duration.ofSeconds(11)); assertTrue(store.claim("exhausted", LEASE).isEmpty());
        assertEquals(FAILED, store.find(id).orElseThrow().state());
        assertEquals(List.of("FAILED", "FAILED"), store.attempts(id).stream().map(Job.Attempt::state).toList());
    }
    @Test void permanentFailureDoesNotScheduleRetry() throws Exception {
        var id = enqueue(); var owner = claim();
        assertTrue(store.fail(owner, false, Duration.ofSeconds(1), Job.FailureCode.TASK_FAILED));
        advance(Duration.ofSeconds(2)); assertTrue(store.claim("other", LEASE).isEmpty());
        assertEquals(FAILED, store.find(id).orElseThrow().state()); assertTrue(store.result(id).isEmpty());
    }
    @Test void expirationOfFinalAttemptBecomesTerminalFailure() throws Exception {
        var id = store.enqueue("last", new Job.Spec(Job.Scenario.HEALTHY, SPEC.budget(), 1));
        claim(); advance(LEASE.plusSeconds(1)); assertTrue(store.claim("replacement", LEASE).isEmpty());
        var job = store.find(id).orElseThrow(); assertEquals(FAILED, job.state());
        assertEquals("ATTEMPTS_EXHAUSTED", job.failureCode()); assertEquals("EXPIRED", store.attempts(id).getFirst().state());
    }
    @Test void queuedCancellationPreventsExecutionWithoutSpendingAnAttempt() throws Exception {
        var id = enqueue(); assertTrue(store.cancel(id)); assertFalse(store.cancel(id));
        assertTrue(store.claim("worker", LEASE).isEmpty()); assertEquals(CANCELLED, store.find(id).orElseThrow().state());
        assertEquals(0, store.find(id).orElseThrow().attempts()); assertTrue(store.attempts(id).isEmpty());
    }
    @Test void runningCancellationRevokesOwnershipAndRejectsLateResult() throws Exception {
        var id = enqueue(); var owner = claim(); assertTrue(store.cancel(id));
        assertFalse(store.complete(owner, "late")); assertFalse(store.renew(owner, LEASE));
        assertFalse(store.fail(owner, true, Duration.ofSeconds(1), Job.FailureCode.INTERRUPTED));
        assertEquals(CANCELLED, store.find(id).orElseThrow().state()); assertTrue(store.result(id).isEmpty());
        assertEquals("CANCELLED", store.attempts(id).getFirst().state());
    }
    @Test void completionRacingCancellationHasOneTerminalState() throws Exception {
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 20; i++) {
                var id = enqueue(); var owner = claim(); var race = new CountDownLatch(1);
                var completed = callers.submit(() -> { assertTrue(race.await(2, TimeUnit.SECONDS)); return store.complete(owner, "race report"); });
                var cancelled = callers.submit(() -> { assertTrue(race.await(2, TimeUnit.SECONDS)); return store.cancel(id); });
                race.countDown(); boolean won = completed.get(5, TimeUnit.SECONDS); boolean cancellation = cancelled.get(5, TimeUnit.SECONDS);
                assertNotEquals(won, cancellation); assertEquals(won ? SUCCEEDED : CANCELLED, store.find(id).orElseThrow().state());
                assertEquals(won, store.result(id).isPresent());
            }
        }
    }
}
