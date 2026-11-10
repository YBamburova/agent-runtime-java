package dev.release.jobs;

import dev.release.ToolRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Worker ownership tests against an explicit mock. These do not establish database correctness. */
@Timeout(10)
class JobWorkersTest {
    private static final Job.Spec SPEC = new Job.Spec(Job.Scenario.HEALTHY, Duration.ofSeconds(2), 3);
    private static JobWorkers.Config config(int workers) {
        return new JobWorkers.Config(workers, Duration.ofSeconds(1), Duration.ofMillis(10),
                Duration.ofMillis(5), Duration.ofMillis(20), Duration.ofSeconds(2));
    }
    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(3, TimeUnit.SECONDS), "Latch was not released");
    }
    @Test void noPrefetchAndActiveHandlersStayWithinWorkerBound() throws Exception {
        var store = new InMemoryJobStore(Clock.systemUTC());
        var ids = new ArrayList<UUID>();
        for (int i = 0; i < 6; i++) ids.add(store.enqueue("bounded-" + i, SPEC));
        var entered = new CountDownLatch(2); var release = new CountDownLatch(1); var committed = new CountDownLatch(6);
        var active = new AtomicInteger(); var peak = new AtomicInteger();
        try (var workers = new JobWorkers(store, claim -> {
            peak.accumulateAndGet(active.incrementAndGet(), Math::max); entered.countDown();
            try { release.await(); return "report"; } finally { active.decrementAndGet(); }
        }, config(2), event -> { if (event.kind().equals("COMMITTED")) committed.countDown(); })) {
            try {
                workers.start(); await(entered);
                long running = ids.stream().filter(id -> store.find(id).orElseThrow().state() == Job.State.RUNNING).count();
                assertEquals(2, running);
                assertEquals(4, ids.stream().filter(id -> store.find(id).orElseThrow().state() == Job.State.QUEUED).count());
                release.countDown(); await(committed); assertEquals(2, peak.get());
                for (var id : ids) assertEquals(1, store.result(id).orElseThrow().attempt());
            } finally { release.countDown(); }
        }
    }
    @Test void gracefulShutdownInterruptsCleansUpAndReschedulesOwnedWork() throws Exception {
        var store = new InMemoryJobStore(Clock.systemUTC()); var id = store.enqueue("shutdown", SPEC);
        var entered = new CountDownLatch(1); var cleaned = new CountDownLatch(1);
        try (var workers = new JobWorkers(store, claim -> {
            try { entered.countDown(); new CountDownLatch(1).await(); return "no"; }
            finally { cleaned.countDown(); }
        }, config(1), event -> {})) {
            workers.start(); await(entered); assertTrue(workers.shutdown(Duration.ofSeconds(2))); await(cleaned);
            assertEquals(Job.State.QUEUED, store.find(id).orElseThrow().state());
            assertEquals("SHUTDOWN", store.find(id).orElseThrow().failureCode());
            assertTrue(store.result(id).isEmpty()); assertEquals(1, store.find(id).orElseThrow().attempts());
        }
    }
    @Test void runningCancellationIsObservedByHeartbeatAndLateReportIsDiscarded() throws Exception {
        var store = new InMemoryJobStore(Clock.systemUTC()); var id = store.enqueue("cancel", SPEC);
        var entered = new CountDownLatch(1); var lost = new CountDownLatch(1);
        try (var workers = new JobWorkers(store, claim -> {
            entered.countDown();
            try { new CountDownLatch(1).await(); }
            catch (InterruptedException swallowed) { /* Deliberately returns despite cancellation. */ }
            return "late";
        }, config(1), event -> { if (event.kind().equals("OWNERSHIP_LOST")) lost.countDown(); })) {
            workers.start(); await(entered); assertTrue(store.cancel(id)); await(lost);
            assertEquals(Job.State.CANCELLED, store.find(id).orElseThrow().state()); assertTrue(store.result(id).isEmpty());
        }
    }
    @Test void databaseUncertaintyStopsPublicationEvenIfHandlerSwallowsInterruption() throws Exception {
        var time = new AtomicReference<>(Instant.parse("2026-01-15T10:00:00Z"));
        var memory = new InMemoryJobStore(MemoryJobStoreTest.clock(time));
        var id = memory.enqueue("uncertain", SPEC); var entered = new CountDownLatch(1); var lost = new CountDownLatch(1);
        JobStore store = new ForwardingStore(memory) {
            @Override public boolean renew(Job.Claim claim, Duration lease) throws SQLException { throw new SQLException("synthetic outage"); }
        };
        try (var workers = new JobWorkers(store, claim -> {
            entered.countDown();
            try { new CountDownLatch(1).await(); } catch (InterruptedException swallowed) { }
            return "not safe to publish";
        }, config(1), event -> { if (event.kind().equals("OWNERSHIP_LOST")) lost.countDown(); })) {
            workers.start(); await(entered); await(lost);
            assertTrue(memory.result(id).isEmpty()); assertEquals(Job.State.RUNNING, memory.find(id).orElseThrow().state());
            assertTrue(workers.shutdown(Duration.ofSeconds(2)));
        }
        time.updateAndGet(now -> now.plusSeconds(2));
        assertEquals(2, memory.claim("replacement", Duration.ofSeconds(1)).orElseThrow().attempt());
    }
    @Test void shutdownIsBoundedForAnUncooperativeHandler() throws Exception {
        var store = new InMemoryJobStore(Clock.systemUTC()); var id = store.enqueue("stubborn", SPEC);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var stopped = new CountDownLatch(1);
        try (var workers = new JobWorkers(store, claim -> {
            entered.countDown(); boolean done = false;
            while (!done) { try { release.await(); done = true; } catch (InterruptedException ignored) { } }
            return "late";
        }, config(1), event -> { if (event.kind().equals("STOPPED")) stopped.countDown(); })) {
            try {
                workers.start(); await(entered); assertFalse(workers.shutdown(Duration.ofMillis(20)));
                assertTrue(store.result(id).isEmpty());
            } finally { release.countDown(); }
            await(stopped); assertTrue(workers.shutdown(Duration.ofSeconds(1)));
            assertNotEquals(Job.State.SUCCEEDED, store.find(id).orElseThrow().state());
        }
    }
    @Test void explicitlyRetryableFailureRunsAgainAfterItsDelay() throws Exception {
        var time = new AtomicReference<>(Instant.parse("2026-01-15T10:00:00Z"));
        var store = new InMemoryJobStore(MemoryJobStoreTest.clock(time)); var id = store.enqueue("retry", SPEC);
        var calls = new AtomicInteger(); var committed = new CountDownLatch(1);
        try (var workers = new JobWorkers(store, claim -> {
            if (calls.incrementAndGet() == 1) throw new JobHandler.Failure(true);
            return "recovered";
        }, config(1), event -> {
            if (event.kind().equals("ATTEMPT_FAILED")) time.updateAndGet(now -> now.plusMillis(30));
            if (event.kind().equals("COMMITTED")) committed.countDown();
        })) {
            workers.start(); await(committed); assertEquals(2, calls.get());
            assertEquals(2, store.result(id).orElseThrow().attempt());
            assertEquals(List.of("FAILED", "SUCCEEDED"), store.attempts(id).stream().map(Job.Attempt::state).toList());
        }
    }
    @Test void unknownHandlerFailureIsPermanentAndDoesNotLeakExceptionContents() throws Exception {
        var store = new InMemoryJobStore(Clock.systemUTC()); var id = store.enqueue("bug", SPEC);
        var failed = new CountDownLatch(1); var calls = new AtomicInteger();
        try (var workers = new JobWorkers(store, claim -> { calls.incrementAndGet(); throw new IllegalStateException("private prompt"); },
                config(1), event -> { if (event.kind().equals("ATTEMPT_FAILED")) failed.countDown(); })) {
            workers.start(); await(failed); assertEquals(1, calls.get());
            var job = store.find(id).orElseThrow(); assertEquals(Job.State.FAILED, job.state());
            assertEquals("TASK_FAILED", job.failureCode()); assertFalse(job.toString().contains("private"));
        }
    }
    @Test void persistedExecutionSuccessCanContainAFailedInvestigationReport() throws Exception {
        var store = new InMemoryJobStore(Clock.systemUTC());
        var id = store.enqueue("failed-evidence", new Job.Spec(Job.Scenario.REQUIRED_FAILURE, Duration.ofSeconds(2), 3));
        var committed = new CountDownLatch(1);
        try (var tools = new ToolRuntime(3, 3);
             var workers = new JobWorkers(store, new InvestigationJobHandler(tools), config(1), event -> {
                 if (event.kind().equals("COMMITTED")) committed.countDown();
             })) {
            workers.start(); await(committed); assertEquals(Job.State.SUCCEEDED, store.find(id).orElseThrow().state());
            assertTrue(store.result(id).orElseThrow().report().contains("Release Investigator — FAILED"));
        }
    }
    @Test void heartbeatRenewsAStillLiveAttemptWhileHandlerIsBlocked() throws Exception {
        var time = new AtomicReference<>(Instant.parse("2026-01-15T10:00:00Z"));
        var memory = new InMemoryJobStore(MemoryJobStoreTest.clock(time)); var id = memory.enqueue("renew", SPEC);
        var entered = new CountDownLatch(1); var renewed = new CountDownLatch(1); var release = new CountDownLatch(1);
        var committed = new CountDownLatch(1); var advanced = new java.util.concurrent.atomic.AtomicBoolean();
        JobStore store = new ForwardingStore(memory) {
            @Override public boolean renew(Job.Claim claim, Duration lease) throws SQLException {
                boolean result = delegate.renew(claim, lease);
                if (advanced.get() && result && delegate.find(claim.jobId()).orElseThrow()
                        .leaseUntil().isAfter(time.get().plusMillis(900))) renewed.countDown();
                return result;
            }
        };
        try (var workers = new JobWorkers(store, claim -> { entered.countDown(); release.await(); return "renewed report"; },
                config(1), event -> { if (event.kind().equals("COMMITTED")) committed.countDown(); })) {
            try {
                workers.start(); await(entered); time.updateAndGet(now -> now.plusMillis(500)); advanced.set(true);
                await(renewed); time.updateAndGet(now -> now.plusMillis(700));
                assertTrue(memory.claim("other", Duration.ofSeconds(1)).isEmpty());
                release.countDown(); await(committed); assertEquals(Job.State.SUCCEEDED, memory.find(id).orElseThrow().state());
            } finally { release.countDown(); }
        }
    }

    @Test void uncertainCommitDoesNotTriggerAnUnfencedRetryOrOverwrite() throws Exception {
        for (boolean committed : List.of(false, true)) {
            var memory = new InMemoryJobStore(Clock.systemUTC()); var id = memory.enqueue("uncertain-commit", SPEC);
            var error = new CountDownLatch(1); var calls = new AtomicInteger();
            JobStore store = new ForwardingStore(memory) {
                @Override public boolean complete(Job.Claim claim, String report) throws SQLException {
                    if (committed) delegate.complete(claim, report);
                    throw new SQLException("commit acknowledgement lost");
                }
            };
            try (var workers = new JobWorkers(store, claim -> { calls.incrementAndGet(); return "one report"; }, config(1), event -> {
                if (event.kind().equals("STORE_ERROR")) error.countDown();
            })) {
                workers.start(); await(error);
                assertEquals(committed ? Job.State.SUCCEEDED : Job.State.RUNNING, memory.find(id).orElseThrow().state());
                assertEquals(committed, memory.result(id).isPresent()); assertEquals(1, calls.get());
            }
        }
    }

    @Test void validatesLifecycleAndConfiguration() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new Job.Spec(Job.Scenario.HEALTHY, Duration.ofNanos(1), 3));
        assertThrows(IllegalArgumentException.class, () -> new Job.Spec(Job.Scenario.HEALTHY, Duration.ofMillis(1), 0));
        assertThrows(IllegalArgumentException.class, () -> new JobWorkers.Config(0, Duration.ofSeconds(1), Duration.ofMillis(10),
                Duration.ofMillis(5), Duration.ofMillis(5), Duration.ofSeconds(1)));
        try (var workers = new JobWorkers(new InMemoryJobStore(Clock.systemUTC()), claim -> "report", config(1), event -> {})) {
            assertTrue(workers.shutdown(Duration.ofSeconds(1))); assertThrows(IllegalStateException.class, workers::start);
        }
    }
    private static class ForwardingStore implements JobStore {
        final JobStore delegate;
        ForwardingStore(JobStore delegate) { this.delegate = delegate; }
        public UUID enqueue(String key, Job.Spec spec) throws SQLException { return delegate.enqueue(key, spec); }
        public Optional<Job> find(UUID id) throws SQLException { return delegate.find(id); }
        public Optional<Job.Claim> claim(String worker, Duration lease) throws SQLException { return delegate.claim(worker, lease); }
        public boolean renew(Job.Claim claim, Duration lease) throws SQLException { return delegate.renew(claim, lease); }
        public boolean complete(Job.Claim claim, String report) throws SQLException { return delegate.complete(claim, report); }
        public boolean fail(Job.Claim claim, boolean retryable, Duration delay, Job.FailureCode code) throws SQLException { return delegate.fail(claim, retryable, delay, code); }
        public boolean cancel(UUID id) throws SQLException { return delegate.cancel(id); }
        public Optional<Job.Result> result(UUID id) throws SQLException { return delegate.result(id); }
        public List<Job.Attempt> attempts(UUID id) throws SQLException { return delegate.attempts(id); }
    }
}
