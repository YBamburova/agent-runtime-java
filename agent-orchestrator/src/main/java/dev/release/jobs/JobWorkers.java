package dev.release.jobs;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Fixed number of caller-owned virtual workers; jobs remain in the store until a worker is free. */
public final class JobWorkers implements AutoCloseable {
    public record Config(int workers, Duration lease, Duration heartbeat, Duration poll,
                         Duration retryDelay, Duration shutdownGrace) {
        public Config {
            if (workers < 1 || workers > 32) throw new IllegalArgumentException("Use 1..32 workers");
            Job.millis(lease); Job.millis(heartbeat); Job.millis(poll); Job.millis(retryDelay); Job.millis(shutdownGrace);
            if (heartbeat.multipliedBy(3).compareTo(lease) > 0)
                throw new IllegalArgumentException("Lease must cover at least three heartbeat intervals");
        }
        public static Config defaults() {
            return new Config(2, Duration.ofSeconds(10), Duration.ofSeconds(2), Duration.ofMillis(200),
                    Duration.ofSeconds(1), Duration.ofSeconds(5));
        }
    }
    public record Event(String kind, UUID jobId, int attempt) {}
    private final JobStore store;
    private final JobHandler handler;
    private final Config config;
    private final Consumer<Event> events;
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private boolean started;

    public JobWorkers(JobStore store, JobHandler handler, Config config, Consumer<Event> events) {
        this.store = Objects.requireNonNull(store); this.handler = Objects.requireNonNull(handler);
        this.config = Objects.requireNonNull(config); this.events = Objects.requireNonNull(events);
    }
    public synchronized void start() {
        if (started || stopping.get()) throw new IllegalStateException("Workers are one-shot");
        started = true;
        String process = UUID.randomUUID().toString();
        for (int i = 0; i < config.workers(); i++) {
            String worker = process + "/" + i;
            threads.add(Thread.ofVirtual().name("job-" + i).start(() -> loop(worker)));
        }
    }
    private void loop(String worker) {
        try {
            while (!stopping.get()) {
                try {
                    var claim = store.claim(worker, config.lease());
                    if (claim.isEmpty()) { Thread.sleep(config.poll()); continue; }
                    emit("CLAIMED", claim.get());
                    execute(claim.get());
                } catch (SQLException e) {
                    emit("STORE_ERROR", null); // Never print credentials, JDBC URLs or arbitrary exception messages.
                    if (!stopping.get()) Thread.sleep(config.poll());
                } catch (InterruptedException e) {
                    if (stopping.get()) break;
                } finally {
                    // This reusable worker owns the interruption boundary. Handler interruption is translated below.
                    Thread.interrupted();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally { emit("STOPPED", null); }
    }
    private void execute(Job.Claim claim) throws SQLException {
        var lost = new AtomicBoolean();
        var finished = new AtomicBoolean();
        Thread owner = Thread.currentThread();
        Thread heartbeat = Thread.ofVirtual().name("lease-" + claim.jobId()).start(() -> {
            try {
                while (!finished.get() && !stopping.get()) {
                    Thread.sleep(config.heartbeat());
                    if (!finished.get() && !stopping.get()) {
                        boolean renewed = store.renew(claim, config.lease());
                        if (!finished.get() && !renewed) { lost.set(true); owner.interrupt(); return; }
                    }
                }
            } catch (SQLException | RuntimeException e) {
                if (!finished.get()) { lost.set(true); owner.interrupt(); }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        String report = null;
        boolean retryable = false;
        Job.FailureCode failure = null;
        boolean interrupted = false;
        try {
            if (stopping.get() || Thread.currentThread().isInterrupted()) throw new InterruptedException();
            report = handler.execute(claim);
            if (report == null || report.isBlank() || report.length() > 100_000) {
                report = null; failure = Job.FailureCode.TASK_FAILED;
            }
        } catch (InterruptedException e) {
            interrupted = true; retryable = true; failure = Job.FailureCode.INTERRUPTED;
        } catch (JobHandler.Failure e) {
            retryable = e.retryable(); failure = Job.FailureCode.TASK_FAILED;
        } catch (Exception e) {
            failure = Job.FailureCode.TASK_FAILED; // Unknown/programming failures are permanent.
        } finally {
            finished.set(true);
            heartbeat.interrupt();
            interrupted |= Thread.interrupted();
            // Do not detach heartbeat I/O and then start another job: bound actual thread/resource lifetime too.
            boolean joined = false;
            while (!joined) {
                try { heartbeat.join(); joined = true; }
                catch (InterruptedException e) { interrupted = true; }
            }
        }
        if (lost.get()) { emit("OWNERSHIP_LOST", claim); return; }
        if (stopping.get() || interrupted) {
            retryable = true;
            failure = stopping.get() ? Job.FailureCode.SHUTDOWN : Job.FailureCode.INTERRUPTED;
        }
        boolean accepted;
        if (failure == null) accepted = store.complete(claim, report);
        else accepted = store.fail(claim, retryable, config.retryDelay(), failure);
        emit(accepted ? (failure == null ? "COMMITTED" : "ATTEMPT_FAILED") : "FENCED", claim);
    }
    private void emit(String kind, Job.Claim claim) {
        try { events.accept(new Event(kind, claim == null ? null : claim.jobId(), claim == null ? 0 : claim.attempt())); }
        catch (RuntimeException ignored) { /* Observability must not change persisted execution semantics. */ }
    }
    public boolean shutdown(Duration grace) throws InterruptedException {
        long deadline = System.nanoTime() + Job.millis(grace) * 1_000_000;
        stopping.set(true);
        List<Thread> copy;
        synchronized (this) { copy = List.copyOf(threads); }
        for (var thread : copy) thread.interrupt();
        for (var thread : copy) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return copy.stream().noneMatch(Thread::isAlive);
            try { if (!thread.join(Duration.ofNanos(remaining))) return false; }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw e; }
        }
        return true;
    }
    @Override public void close() {
        try { shutdown(config.shutdownGrace()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
