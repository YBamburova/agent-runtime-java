package dev.release.jobs;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public record Job(UUID id, String key, Spec spec, State state, int attempts, Instant availableAt,
                  Instant leaseUntil, String worker, String failureCode) {
    public enum State { QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED }
    public enum Scenario { HEALTHY, PARTIAL, TIMEOUT, REQUIRED_FAILURE }
    public enum FailureCode { TASK_FAILED, INTERRUPTED, SHUTDOWN, LEASE_EXPIRED, ATTEMPTS_EXHAUSTED, CANCELLED }
    public record Spec(Scenario scenario, Duration budget, int maxAttempts) {
        public Spec {
            if (scenario == null) throw new IllegalArgumentException("Scenario is required");
            millis(budget);
            if (maxAttempts < 1 || maxAttempts > 10) throw new IllegalArgumentException("Use 1..10 attempts");
        }
    }
    /** Monotonically increasing attempt number is the fencing token; worker identity is additional validation. */
    public record Claim(UUID jobId, int attempt, String worker, Spec spec) {}
    public record Attempt(int number, String worker, String state, Instant startedAt, Instant finishedAt, String failureCode) {}
    public record Result(int attempt, String report) {}

    public static long millis(Duration duration) {
        if (duration == null || duration.compareTo(Duration.ofMillis(1)) < 0 || duration.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Duration must be 1ms..1day");
        if (duration.toNanos() % 1_000_000 != 0) throw new IllegalArgumentException("Use whole milliseconds");
        return duration.toMillis();
    }
    static void key(String key) {
        if (key == null || key.isBlank() || key.length() > 128) throw new IllegalArgumentException("Key must contain 1..128 characters");
    }
    static void worker(String worker) {
        if (worker == null || worker.isBlank() || worker.length() > 128) throw new IllegalArgumentException("Worker id must contain 1..128 characters");
    }
    static void report(String report) {
        if (report == null || report.isBlank() || report.length() > 100_000) throw new IllegalArgumentException("Report must contain 1..100000 characters");
    }
}
