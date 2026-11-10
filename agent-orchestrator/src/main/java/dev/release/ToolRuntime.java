package dev.release;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Process-wide capacity owner. A running call keeps its slot until its body actually exits. */
public final class ToolRuntime implements AutoCloseable {
    private final ThreadPoolExecutor executor;

    public ToolRuntime(int activeLimit, int waitingLimit) {
        if (activeLimit < 1 || waitingLimit < 1) throw new IllegalArgumentException("Limits must be positive");
        executor = new ThreadPoolExecutor(activeLimit, activeLimit, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(waitingLimit), Thread.ofVirtual().name("evidence-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    void submit(FutureTask<?> task) { executor.execute(task); }

    void cancel(FutureTask<?> task) {
        task.cancel(true);
        executor.remove(task); // Cancelled waiting work must not retain queue capacity.
    }

    public int waitingCalls() { return executor.getQueue().size(); }
    public int activeCalls() { return executor.getActiveCount(); }

    public boolean awaitTermination(Duration budget) throws InterruptedException {
        Investigation.validateBudget(budget);
        return executor.awaitTermination(budget.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Nonblocking: callers can separately await bounded cleanup; uncooperative work may outlive close. */
    @Override public void close() {
        for (Runnable queued : executor.shutdownNow()) {
            if (queued instanceof FutureTask<?> task) task.cancel(false);
        }
    }
}
