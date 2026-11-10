package dev.release.llm;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

import static dev.release.llm.GatewayResult.Status;
import static dev.release.llm.LlmProvider.FailureKind;

/** Provider-specific policy owner. No provider call runs under the policy lock. */
public final class LlmGateway {
    public enum Circuit { CLOSED, OPEN, HALF_OPEN }
    public record Snapshot(int admitted, int inFlight, int waiting, int requestsCharged,
                           long tokensCharged, Circuit circuit) {}

    @FunctionalInterface
    interface Waiter { void await(Condition changed, long nanos) throws InterruptedException; }

    private record Reservation(long quotaEpoch, long circuitGeneration, boolean probe, int tokens) {}
    private record Admission(Reservation reservation, Status rejection) {}

    private final LlmProvider provider;
    private final GatewayConfig config;
    private final LongSupplier clock;
    private final DoubleSupplier jitter;
    private final Waiter waiter;
    // Immediate admission also bounds callers contending for the short-lived policy lock.
    private final Semaphore slots;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    // Everything below is guarded by lock, including circuit epochs and quota reconciliation.
    private int inFlight;
    private int waiting;
    private int requestsCharged;
    private long tokensCharged;
    private long windowStarted;
    private long quotaEpoch;
    private Circuit circuit = Circuit.CLOSED;
    private long circuitGeneration;
    private long openedAt;
    private int consecutiveFailures;

    public LlmGateway(LlmProvider provider, GatewayConfig config) {
        this(provider, config, System::nanoTime, (condition, nanos) -> condition.awaitNanos(nanos),
                () -> ThreadLocalRandom.current().nextDouble());
    }

    // Package-visible policy dependencies keep deterministic tests independent of wall-clock scheduling.
    LlmGateway(LlmProvider provider, GatewayConfig config, LongSupplier clock, Waiter waiter, DoubleSupplier jitter) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.jitter = Objects.requireNonNull(jitter, "jitter");
        slots = new Semaphore(config.maxInFlight() + config.maxWaiting());
        windowStarted = clock.getAsLong();
    }

    /** Cancellation is interruption. The caller owns its thread; no detached transport task is created. */
    public GatewayResult generate(LlmProvider.Request request, Duration budget) throws InterruptedException {
        Objects.requireNonNull(request, "request");
        long deadline = clock.getAsLong() + GatewayConfig.budgetNanos(budget);
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
        if (request.estimatedTokens() > config.tokensPerWindow()) return result(Status.INVALID_REQUEST, 0);
        if (!slots.tryAcquire()) return result(Status.OVERLOADED, 0);
        int attempts = 0;
        long notBefore = clock.getAsLong();
        try {
            while (true) {
                var admission = acquire(request.estimatedTokens(), deadline, notBefore);
                if (admission.rejection() != null) return result(admission.rejection(), attempts);
                var reservation = admission.reservation();
                LlmProvider.Response response = null;
                FailureKind failure = null;
                boolean dispatched = false;
                try {
                    checkInterrupted();
                    long remaining = deadline - clock.getAsLong();
                    if (remaining <= 0) return result(Status.DEADLINE_EXCEEDED, attempts);
                    dispatched = true;
                    attempts++;
                    response = Objects.requireNonNull(provider.generate(request, Duration.ofNanos(remaining)), "response");
                } catch (LlmProvider.Failure e) {
                    failure = e.kind();
                } catch (RuntimeException e) {
                    // Programming/contract failures are not eligible for transport retries; never print their messages.
                    failure = FailureKind.PERMANENT;
                } finally {
                    release(reservation, response, failure, dispatched);
                }
                checkInterrupted();
                if (deadline - clock.getAsLong() <= 0) return result(Status.DEADLINE_EXCEEDED, attempts);
                if (response != null) return new GatewayResult(Status.SUCCESS, response.text(), attempts);
                if (failure == FailureKind.PERMANENT) return result(Status.PROVIDER_FAILED, attempts);
                if (attempts >= config.maxAttempts()) return result(Status.RETRY_EXHAUSTED, attempts);
                long cap = Math.min(config.maxBackoff().toNanos(),
                        config.initialBackoff().toNanos() * (1L << (attempts - 1)));
                double sample = jitter.getAsDouble();
                if (!(sample >= 0 && sample < 1)) throw new IllegalArgumentException("Jitter must be in [0,1)");
                long delay = Math.max(1, (long) (cap * sample));
                long now = clock.getAsLong();
                if (delay >= deadline - now) return result(Status.DEADLINE_EXCEEDED, attempts);
                notBefore = now + delay;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            slots.release();
        }
    }

    private Admission acquire(int tokens, long deadline, long notBefore) throws InterruptedException {
        lock.lockInterruptibly();
        boolean queued = false;
        try {
            while (true) {
                checkInterrupted();
                long now = clock.getAsLong();
                if (deadline - now <= 0) return new Admission(null, Status.DEADLINE_EXCEEDED);
                refreshWindow(now);
                if (circuit == Circuit.HALF_OPEN || (circuit == Circuit.OPEN && now - openedAt < config.openDuration().toNanos()))
                    return new Admission(null, Status.CIRCUIT_OPEN);
                boolean quotaAvailable = requestsCharged < config.requestsPerWindow()
                        && tokensCharged + tokens <= config.tokensPerWindow();
                if (inFlight < config.maxInFlight() && quotaAvailable && now - notBefore >= 0) {
                    boolean probe = circuit == Circuit.OPEN;
                    if (probe) { circuit = Circuit.HALF_OPEN; circuitGeneration++; }
                    inFlight++;
                    requestsCharged++;
                    tokensCharged += tokens;
                    return new Admission(new Reservation(quotaEpoch, circuitGeneration, probe, tokens), null);
                }
                if (!queued) {
                    if (waiting == config.maxWaiting()) return new Admission(null, Status.OVERLOADED);
                    waiting++;
                    queued = true;
                }
                long wait = deadline - now;
                if (!quotaAvailable) wait = Math.min(wait, config.quotaWindow().toNanos() - (now - windowStarted));
                if (notBefore - now > 0) wait = Math.min(wait, notBefore - now);
                // await releases lock; signals and spurious wakeups always recheck every predicate.
                waiter.await(changed, wait);
            }
        } finally {
            if (queued) waiting--;
            lock.unlock();
        }
    }

    private void release(Reservation reservation, LlmProvider.Response response, FailureKind failure, boolean dispatched) {
        lock.lock();
        try {
            long now = clock.getAsLong();
            refreshWindow(now);
            inFlight--;
            if (!dispatched) {
                if (reservation.quotaEpoch() == quotaEpoch) {
                    requestsCharged--;
                    tokensCharged -= reservation.tokens();
                }
                if (reservation.probe() && reservation.circuitGeneration() == circuitGeneration) {
                    circuit = Circuit.OPEN;
                    circuitGeneration++;
                }
                return;
            }
            // Reconcile only the dispatch window. Old completions cannot refund a newer window's budget.
            if (reservation.quotaEpoch() == quotaEpoch && response != null && response.totalTokens().isPresent())
                tokensCharged += (long) response.totalTokens().getAsInt() - reservation.tokens();
            if (reservation.circuitGeneration() == circuitGeneration) {
                boolean abandoned = Thread.currentThread().isInterrupted() || (response == null && failure == null);
                if (reservation.probe()) {
                    if (response != null && !abandoned) {
                        circuit = Circuit.CLOSED;
                        consecutiveFailures = 0;
                        circuitGeneration++;
                    } else openCircuit(now);
                } else if (!abandoned && response != null) {
                    consecutiveFailures = 0;
                } else if (!abandoned && failure == FailureKind.TRANSIENT) {
                    if (++consecutiveFailures >= config.failuresToOpen()) openCircuit(now);
                }
            }
        } finally {
            changed.signalAll();
            lock.unlock();
        }
    }

    private void openCircuit(long now) {
        circuit = Circuit.OPEN;
        openedAt = now;
        circuitGeneration++;
    }

    private void refreshWindow(long now) {
        if (now - windowStarted >= config.quotaWindow().toNanos()) {
            windowStarted = now;
            quotaEpoch++;
            requestsCharged = 0;
            tokensCharged = 0;
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try {
            refreshWindow(clock.getAsLong());
            return new Snapshot(config.maxInFlight() + config.maxWaiting() - slots.availablePermits(),
                    inFlight, waiting, requestsCharged, tokensCharged, circuit);
        } finally { lock.unlock(); }
    }

    private static GatewayResult result(Status status, int attempts) { return new GatewayResult(status, "", attempts); }
    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
    }
}
