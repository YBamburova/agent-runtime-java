package dev.release;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.release.Report.State.*;
import static dev.release.Report.Status;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class InvestigationTest {
    private static final Duration BUDGET = Duration.ofSeconds(3);

    private static EvidenceTool tool(String name, boolean required, EvidenceTool.Call call) {
        return new EvidenceTool(name, required, BUDGET, call);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Latch was not released");
    }

    @Test void independentCallsRunInParallelOnVirtualThreads() {
        var entered = new CountDownLatch(3);
        try (var runtime = new ToolRuntime(3, 1)) {
            EvidenceTool.Call call = () -> {
                assertTrue(Thread.currentThread().isVirtual());
                entered.countDown();
                await(entered); // Sequential execution would fail this barrier.
                return "evidence";
            };
            var report = new Investigation(runtime, List.of(tool("a", true, call), tool("b", true, call), tool("c", false, call)), BUDGET).run();
            assertEquals(COMPLETED, report.state());
            assertEquals(List.of("a", "b", "c"), report.outcomes().stream().map(Report.Outcome::tool).toList());
            assertThrows(UnsupportedOperationException.class, () -> report.outcomes().clear());
        }
    }

    @Test void optionalFailurePreservesSuccessfulEvidence() {
        try (var runtime = new ToolRuntime(2, 1)) {
            var report = new Investigation(runtime, List.of(tool("logs", true, () -> "logs"),
                    tool("dependencies", false, () -> { throw new IOException("secret"); })), BUDGET).run();
            assertEquals(PARTIAL, report.state());
            assertEquals(Status.SUCCESS, report.outcomes().getFirst().status());
            assertEquals(Status.FAILED, report.outcomes().getLast().status());
            assertFalse(report.render().contains("secret"));
        }
    }

    @Test void requiredFailureInterruptsSiblingAndClosesItsResource() throws Exception {
        var entered = new CountDownLatch(1);
        var closed = new CountDownLatch(1);
        try (var runtime = new ToolRuntime(2, 1)) {
            var report = new Investigation(runtime, List.of(
                    tool("required", true, () -> { await(entered); throw new IOException(); }),
                    tool("sibling", false, () -> {
                        try { entered.countDown(); new CountDownLatch(1).await(); return "unreachable"; }
                        finally { closed.countDown(); }
                    })), BUDGET).run();
            assertEquals(FAILED, report.state());
            await(closed);
            assertEquals(Status.CANCELLED, report.outcomes().getLast().status());
        }
    }

    @Test void deadlineReturnsPartialEvidenceAndInterruptsBlockedCall() throws Exception {
        var closed = new CountDownLatch(1);
        try (var runtime = new ToolRuntime(2, 1)) {
            var report = new Investigation(runtime, List.of(tool("fast", true, () -> "evidence"),
                    tool("blocked", false, () -> {
                        try { new CountDownLatch(1).await(); return "unreachable"; }
                        finally { closed.countDown(); }
                    })), Duration.ofMillis(200)).run();
            assertEquals(TIMED_OUT, report.state());
            assertEquals(Status.SUCCESS, report.outcomes().getFirst().status());
            assertEquals(Status.TIMED_OUT, report.outcomes().getLast().status());
            await(closed);
        }
    }

    @Test void perToolTimeoutDoesNotCancelHealthySibling() {
        try (var runtime = new ToolRuntime(2, 1)) {
            var report = new Investigation(runtime, List.of(tool("fast", true, () -> "evidence"),
                    new EvidenceTool("blocked", false, Duration.ofMillis(100), () -> {
                        new CountDownLatch(1).await(); return "unreachable";
                    })), BUDGET).run();
            assertEquals(PARTIAL, report.state());
            assertEquals(Status.TIMED_OUT, report.outcomes().getLast().status());
        }
    }

    @Test void toolBudgetIncludesQueueWaitAndExpiryRemovesQueuedCall() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = new ToolRuntime(1, 1)) {
            var occupying = new FutureTask<>(() -> {
                entered.countDown(); await(release); return "holder";
            });
            runtime.submit(occupying); await(entered);
            var report = new Investigation(runtime, List.of(new EvidenceTool("queued", false,
                    Duration.ofMillis(100), () -> { fail("Expired queued call must not execute"); return "no"; })), BUDGET).run();
            assertEquals(PARTIAL, report.state());
            assertEquals(Status.TIMED_OUT, report.outcomes().getFirst().status());
            assertEquals(0, runtime.waitingCalls());
            assertEquals(1, runtime.activeCalls());
            release.countDown();
            assertEquals("holder", occupying.get(2, TimeUnit.SECONDS));
        } finally { release.countDown(); }
    }

    @Test void activeAndWaitingWorkAreBoundedAndRejectionIsTyped() throws Exception {
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(2);
        var active = new AtomicInteger();
        var peak = new AtomicInteger();
        try (var runtime = new ToolRuntime(2, 1)) {
            EvidenceTool.Call call = () -> {
                peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                entered.countDown();
                try { await(release); return "ok"; }
                finally { active.decrementAndGet(); }
            };
            var first = new FutureTask<>(call::gather);
            var second = new FutureTask<>(call::gather);
            runtime.submit(first); runtime.submit(second);
            await(entered);
            var queued = new FutureTask<>(call::gather);
            runtime.submit(queued);
            assertEquals(1, runtime.waitingCalls());
            var report = new Investigation(runtime, List.of(tool("overflow", true, () -> "must not run")), BUDGET).run();
            assertEquals(FAILED, report.state());
            assertEquals(Status.REJECTED, report.outcomes().getFirst().status());
            runtime.cancel(queued);
            assertEquals(0, runtime.waitingCalls());
            release.countDown();
            assertEquals("ok", first.get(2, TimeUnit.SECONDS));
            assertEquals("ok", second.get(2, TimeUnit.SECONDS));
            assertEquals(2, peak.get());
        } finally { release.countDown(); }
    }

    @Test void uncooperativeCallRetainsCapacityUntilActualExitAndCannotChangeReport() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        try (var runtime = new ToolRuntime(1, 1); var owners = Executors.newVirtualThreadPerTaskExecutor()) {
            var investigation = new Investigation(runtime, List.of(tool("stubborn", true, () -> {
                entered.countDown();
                try {
                    boolean done = false;
                    while (!done) {
                        try { release.await(); done = true; }
                        catch (InterruptedException ignored) { /* Deliberately uncooperative test boundary. */ }
                    }
                    return "late evidence";
                } finally { exited.countDown(); }
            })), BUDGET);
            var result = owners.submit(investigation::run);
            await(entered);
            assertTrue(investigation.cancel());
            var report = result.get(2, TimeUnit.SECONDS);
            assertEquals(CANCELLED, report.state());
            assertEquals(1, runtime.activeCalls());
            var queued = new FutureTask<>(() -> "queued");
            runtime.submit(queued);
            var rejected = new Investigation(runtime, List.of(tool("rejected", false, () -> "no")), BUDGET).run();
            assertEquals(Status.REJECTED, rejected.outcomes().getFirst().status());
            runtime.cancel(queued);
            release.countDown(); await(exited);
            var next = new FutureTask<>(() -> "capacity recovered");
            runtime.submit(next);
            assertEquals("capacity recovered", next.get(2, TimeUnit.SECONDS));
            assertEquals(Status.CANCELLED, report.outcomes().getFirst().status());
            assertFalse(investigation.cancel());
        } finally { release.countDown(); }
    }

    @Test void deadlineDoesNotWaitForAnUncooperativeTool() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = new ToolRuntime(1, 1); var owners = Executors.newVirtualThreadPerTaskExecutor()) {
            var investigation = new Investigation(runtime, List.of(tool("stubborn", true, () -> {
                entered.countDown();
                boolean finished = false;
                while (!finished) {
                    try { release.await(); finished = true; }
                    catch (InterruptedException ignored) { /* Models an external call ignoring cancellation. */ }
                }
                return "late";
            })), Duration.ofMillis(250));
            var result = owners.submit(investigation::run);
            await(entered);
            try {
                var report = result.get(2, TimeUnit.SECONDS);
                assertEquals(TIMED_OUT, report.state());
                assertEquals(Status.TIMED_OUT, report.outcomes().getFirst().status());
                assertEquals(1, runtime.activeCalls());
                runtime.close();
                assertFalse(runtime.awaitTermination(Duration.ofMillis(25)));
            } finally { release.countDown(); }
            assertTrue(runtime.awaitTermination(BUDGET));
        } finally { release.countDown(); }
    }

    @Test void completionRacingCancellationHasOneTerminalState() throws Exception {
        try (var runtime = new ToolRuntime(2, 2); var owners = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 100; i++) {
                var entered = new CountDownLatch(1);
                var race = new CountDownLatch(1);
                var investigation = new Investigation(runtime, List.of(tool("race", true, () -> {
                    entered.countDown(); await(race); return "evidence";
                })), BUDGET);
                var result = owners.submit(investigation::run);
                await(entered);
                var cancel = owners.submit(() -> { await(race); return investigation.cancel(); });
                race.countDown();
                boolean accepted = cancel.get(2, TimeUnit.SECONDS);
                var report = result.get(2, TimeUnit.SECONDS);
                assertEquals(accepted ? CANCELLED : COMPLETED, report.state());
                assertFalse(investigation.cancel());
                assertThrows(IllegalStateException.class, investigation::run);
            }
        }
    }

    @Test void ownerInterruptionIsPreservedAndCancelsChild() throws Exception {
        var entered = new CountDownLatch(1);
        var closed = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        var preserved = new java.util.concurrent.atomic.AtomicBoolean();
        try (var runtime = new ToolRuntime(1, 1)) {
            var investigation = new Investigation(runtime, List.of(tool("blocked", true, () -> {
                try { entered.countDown(); new CountDownLatch(1).await(); return "no"; }
                finally { closed.countDown(); }
            })), BUDGET);
            var owner = Thread.ofVirtual().start(() -> {
                try {
                    var report = investigation.run();
                    preserved.set(Thread.currentThread().isInterrupted() && report.state() == CANCELLED);
                } finally { finished.countDown(); }
            });
            await(entered); owner.interrupt(); await(finished); await(closed);
            assertTrue(preserved.get());
        }
    }

    @Test void cancelBeforeStartNeverCallsTool() {
        try (var runtime = new ToolRuntime(1, 1)) {
            var investigation = new Investigation(runtime, List.of(tool("unused", true, () -> {
                fail("Must not execute"); return "no";
            })), BUDGET);
            assertTrue(investigation.cancel());
            assertEquals(CANCELLED, investigation.run().state());
        }
    }

    @Test void shutdownCancelsQueueAndRejectsNewWork() throws Exception {
        var entered = new CountDownLatch(1);
        var runtime = new ToolRuntime(1, 1);
        var active = new FutureTask<>(() -> { entered.countDown(); new CountDownLatch(1).await(); return "no"; });
        runtime.submit(active); await(entered);
        var queued = new FutureTask<>(() -> "no"); runtime.submit(queued);
        runtime.close();
        assertTrue(queued.isCancelled());
        assertTrue(runtime.awaitTermination(BUDGET));
        assertThrows(java.util.concurrent.RejectedExecutionException.class,
                () -> runtime.submit(new FutureTask<>(() -> "no")));
    }

    @Test void validatesBoundaries() {
        assertThrows(IllegalArgumentException.class, () -> new ToolRuntime(0, 1));
        assertThrows(IllegalArgumentException.class, () -> tool(" ", true, () -> "no"));
        try (var runtime = new ToolRuntime(1, 1)) {
            assertThrows(IllegalArgumentException.class, () -> new Investigation(runtime, List.of(), BUDGET));
            var tool = tool("same", true, () -> "ok");
            assertThrows(IllegalArgumentException.class, () -> new Investigation(runtime, List.of(tool, tool), BUDGET));
            assertThrows(IllegalArgumentException.class, () -> new Investigation(runtime, List.of(tool), Duration.ZERO));
        }
    }
}
