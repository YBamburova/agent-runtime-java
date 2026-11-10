package dev.release;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static dev.release.Report.State;
import static dev.release.Report.Status;

/** One-shot coordinator. Only run() aggregates outcomes; cancel and terminal publication share a lock. */
public final class Investigation {
    private final ToolRuntime runtime;
    private final List<EvidenceTool> tools;
    private final Duration budget;
    // At most one completion per tool plus one cancellation wakeup.
    private final LinkedBlockingQueue<Object> events;
    private final Object cancellation = new Object();
    private record CallResult(Status status, String detail, long completedAt) {}

    private boolean started;
    private boolean cancelled;
    private Report report;

    public Investigation(ToolRuntime runtime, List<EvidenceTool> tools, Duration budget) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.tools = List.copyOf(tools);
        if (this.tools.isEmpty() || this.tools.size() > 16) throw new IllegalArgumentException("Use 1..16 tools");
        var names = new HashSet<String>();
        for (var tool : this.tools) if (!names.add(tool.name())) throw new IllegalArgumentException("Duplicate tool name");
        validateBudget(budget);
        this.budget = budget;
        events = new LinkedBlockingQueue<>(tools.size() + 1);
    }

    static void validateBudget(Duration budget) {
        if (budget == null || budget.isZero() || budget.isNegative() || budget.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Budget must be positive and at most one day");
    }

    /** True only when cancellation wins before terminal publication. Idempotent after acceptance. */
    public synchronized boolean cancel() {
        if (report != null) return false;
        if (!cancelled) {
            cancelled = true;
            events.add(cancellation);
        }
        return true;
    }

    private synchronized boolean isCancelled() { return cancelled; }

    private synchronized Report finish(State state, List<Report.Outcome> outcomes) {
        // Linearization point: an accepted cancel overrides a concurrent completion.
        report = new Report(cancelled ? State.CANCELLED : state, outcomes);
        return report;
    }

    public Report run() {
        synchronized (this) {
            if (started) throw new IllegalStateException("Investigation is one-shot");
            started = true;
        }
        long deadline = System.nanoTime() + budget.toNanos();
        var pending = new LinkedHashMap<EvidenceTool, FutureTask<CallResult>>();
        var expires = new LinkedHashMap<EvidenceTool, Long>();
        var outcomes = new LinkedHashMap<EvidenceTool, Report.Outcome>();
        State state = State.COMPLETED;
        boolean interrupted = false;
        try {
            for (var tool : tools) {
                if (isCancelled() || Thread.currentThread().isInterrupted() || System.nanoTime() - deadline >= 0) break;
                var task = new FutureTask<CallResult>(() -> invoke(tool)) {
                    @Override protected void done() { events.add(this); }
                };
                pending.put(tool, task);
                expires.put(tool, System.nanoTime() + Math.min(tool.timeout().toNanos(), budget.toNanos()));
                try { runtime.submit(task); }
                catch (RejectedExecutionException rejected) {
                    pending.remove(tool);
                    outcomes.put(tool, outcome(tool, Status.REJECTED, "Tool capacity unavailable"));
                }
            }
            while (true) {
                if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                if (isCancelled()) { state = State.CANCELLED; break; }
                long now = System.nanoTime();
                if (now - deadline >= 0) { state = State.TIMED_OUT; break; }
                for (var iterator = pending.entrySet().iterator(); iterator.hasNext();) {
                    var entry = iterator.next();
                    var tool = entry.getKey();
                    var task = entry.getValue();
                    if (task.isDone()) {
                        try {
                            var result = task.get();
                            if (result.completedAt() - expires.get(tool) >= 0) {
                                outcomes.put(tool, outcome(tool, Status.TIMED_OUT, "Result arrived after per-tool budget"));
                            } else {
                                outcomes.put(tool, outcome(tool, result.status(), result.detail()));
                            }
                        }
                        catch (CancellationException e) { outcomes.put(tool, outcome(tool, Status.CANCELLED, "Runtime stopped")); }
                        catch (java.util.concurrent.ExecutionException e) {
                            // Do not expose arbitrary exception messages, which may contain secrets.
                            outcomes.put(tool, outcome(tool, Status.FAILED, e.getCause().getClass().getSimpleName()));
                        }
                        iterator.remove();
                    } else if (now - expires.get(tool) >= 0) {
                        outcomes.put(tool, outcome(tool, Status.TIMED_OUT, "Per-tool budget expired (includes queue wait)"));
                        runtime.cancel(task);
                        iterator.remove();
                    }
                }
                if (outcomes.values().stream().anyMatch(o -> o.required() && o.status() != Status.SUCCESS)) {
                    state = State.FAILED; break;
                }
                if (pending.isEmpty()) {
                    state = outcomes.values().stream().allMatch(o -> o.status() == Status.SUCCESS) ? State.COMPLETED : State.PARTIAL;
                    break;
                }
                long wait = deadline - System.nanoTime();
                for (var tool : pending.keySet()) wait = Math.min(wait, expires.get(tool) - System.nanoTime());
                if (wait > 0) events.poll(wait, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            interrupted = true;
            cancel();
            state = State.CANCELLED;
        } finally {
            for (var task : pending.values()) runtime.cancel(task);
            if (interrupted) Thread.currentThread().interrupt();
        }
        var ordered = new ArrayList<Report.Outcome>();
        for (var tool : tools) {
            ordered.add(outcomes.getOrDefault(tool, outcome(tool,
                    state == State.TIMED_OUT ? Status.TIMED_OUT : Status.CANCELLED,
                    "No result accepted before investigation ended")));
        }
        return finish(state, ordered);
    }

    private static CallResult invoke(EvidenceTool tool) {
        try {
            String evidence = Objects.requireNonNull(tool.call().gather(), "Tool returned null");
            return new CallResult(Status.SUCCESS, evidence, System.nanoTime());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new CallResult(Status.CANCELLED, "Tool interrupted", System.nanoTime());
        } catch (Exception e) {
            return new CallResult(Status.FAILED, e.getClass().getSimpleName(), System.nanoTime());
        }
    }

    private static Report.Outcome outcome(EvidenceTool tool, Status status, String detail) {
        return new Report.Outcome(tool.name(), tool.required(), status, detail);
    }
}
