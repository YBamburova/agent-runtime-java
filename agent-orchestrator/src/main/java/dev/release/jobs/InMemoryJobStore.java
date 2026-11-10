package dev.release.jobs;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Runnable mock for policy/worker tests. A JVM monitor does NOT model PostgreSQL transactions or crash durability. */
public final class InMemoryJobStore implements JobStore {
    private static final class Entry {
        Job job;
        Job.Result result;
        final List<Job.Attempt> attempts = new ArrayList<>();
        Entry(Job job) { this.job = job; }
    }
    private final Map<UUID, Entry> jobs = new LinkedHashMap<>();
    private final Clock clock;
    public InMemoryJobStore(Clock clock) { this.clock = Objects.requireNonNull(clock); }

    @Override public synchronized UUID enqueue(String key, Job.Spec spec) {
        Job.key(key); Objects.requireNonNull(spec);
        for (var entry : jobs.values()) {
            if (entry.job.key().equals(key)) {
                if (!entry.job.spec().equals(spec)) throw new IllegalArgumentException("Key already used for different job settings");
                return entry.job.id();
            }
        }
        UUID id = UUID.randomUUID();
        jobs.put(id, new Entry(new Job(id, key, spec, Job.State.QUEUED, 0, clock.instant(), null, null, null)));
        return id;
    }
    @Override public synchronized Optional<Job> find(UUID id) { return Optional.ofNullable(jobs.get(id)).map(e -> e.job); }

    @Override public synchronized Optional<Job.Claim> claim(String worker, Duration lease) {
        Job.worker(worker); Job.millis(lease);
        Instant now = clock.instant();
        for (var entry : jobs.values()) {
            Job job = entry.job;
            boolean expired = job.state() == Job.State.RUNNING && !job.leaseUntil().isAfter(now);
            if (job.state() != Job.State.QUEUED && !expired) continue;
            if (!expired && job.availableAt().isAfter(now)) continue;
            if (expired) finishAttempt(entry, "EXPIRED", Job.FailureCode.LEASE_EXPIRED);
            if (job.attempts() >= job.spec().maxAttempts()) {
                update(entry, Job.State.FAILED, job.attempts(), now, null, null, Job.FailureCode.ATTEMPTS_EXHAUSTED);
                return Optional.empty();
            }
            int attempt = job.attempts() + 1;
            update(entry, Job.State.RUNNING, attempt, job.availableAt(), now.plus(lease), worker, null);
            entry.attempts.add(new Job.Attempt(attempt, worker, "RUNNING", now, null, null));
            return Optional.of(new Job.Claim(job.id(), attempt, worker, job.spec()));
        }
        return Optional.empty();
    }
    @Override public synchronized boolean renew(Job.Claim claim, Duration lease) {
        Job.millis(lease);
        var entry = owned(claim);
        if (entry == null) return false;
        Job job = entry.job;
        update(entry, job.state(), job.attempts(), job.availableAt(), clock.instant().plus(lease), job.worker(), null);
        return true;
    }
    @Override public synchronized boolean complete(Job.Claim claim, String report) {
        Job.report(report);
        var existing = jobs.get(claim.jobId());
        if (existing != null && existing.job.state() == Job.State.SUCCEEDED)
            return existing.result.equals(new Job.Result(claim.attempt(), report))
                    && existing.attempts.getLast().worker().equals(claim.worker());
        var entry = owned(claim);
        if (entry == null) return false;
        entry.result = new Job.Result(claim.attempt(), report);
        finishAttempt(entry, "SUCCEEDED", null);
        update(entry, Job.State.SUCCEEDED, entry.job.attempts(), clock.instant(), null, null, null);
        return true;
    }
    @Override public synchronized boolean fail(Job.Claim claim, boolean retryable, Duration delay, Job.FailureCode code) {
        Job.millis(delay); Objects.requireNonNull(code);
        var entry = owned(claim);
        if (entry == null) return false;
        finishAttempt(entry, "FAILED", code);
        Job.State state = retryable && entry.job.attempts() < entry.job.spec().maxAttempts() ? Job.State.QUEUED : Job.State.FAILED;
        update(entry, state, entry.job.attempts(), clock.instant().plus(delay), null, null, code);
        return true;
    }
    @Override public synchronized boolean cancel(UUID id) {
        var entry = jobs.get(id);
        if (entry == null || (entry.job.state() != Job.State.QUEUED && entry.job.state() != Job.State.RUNNING)) return false;
        if (entry.job.state() == Job.State.RUNNING) finishAttempt(entry, "CANCELLED", Job.FailureCode.CANCELLED);
        update(entry, Job.State.CANCELLED, entry.job.attempts(), clock.instant(), null, null, Job.FailureCode.CANCELLED);
        return true;
    }
    @Override public synchronized Optional<Job.Result> result(UUID id) {
        return Optional.ofNullable(jobs.get(id)).map(e -> e.result);
    }
    @Override public synchronized List<Job.Attempt> attempts(UUID id) {
        var entry = jobs.get(id);
        return entry == null ? List.of() : List.copyOf(entry.attempts);
    }
    private Entry owned(Job.Claim claim) {
        var entry = jobs.get(claim.jobId());
        if (entry == null) return null;
        Job job = entry.job;
        return job.state() == Job.State.RUNNING && job.attempts() == claim.attempt()
                && job.spec().equals(claim.spec()) && job.worker().equals(claim.worker()) && job.leaseUntil().isAfter(clock.instant()) ? entry : null;
    }
    private void finishAttempt(Entry entry, String state, Job.FailureCode code) {
        var old = entry.attempts.getLast();
        entry.attempts.set(entry.attempts.size() - 1, new Job.Attempt(old.number(), old.worker(), state,
                old.startedAt(), clock.instant(), code == null ? null : code.name()));
    }
    private static void update(Entry entry, Job.State state, int attempts, Instant available, Instant lease,
                               String worker, Job.FailureCode code) {
        Job old = entry.job;
        entry.job = new Job(old.id(), old.key(), old.spec(), state, attempts, available, lease, worker, code == null ? null : code.name());
    }
}
