package dev.release.jobs;

import org.junit.jupiter.api.BeforeEach;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

class MemoryJobStoreTest extends JobStoreContract {
    private final AtomicReference<Instant> time = new AtomicReference<>(Instant.parse("2026-01-15T10:00:00Z"));
    @BeforeEach void setup() { store = new InMemoryJobStore(clock(time)); }
    @Override protected void advance(Duration elapsed) { time.updateAndGet(now -> now.plus(elapsed)); }
    static Clock clock(AtomicReference<Instant> time) {
        return new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { if (!zone.equals(ZoneOffset.UTC)) throw new UnsupportedOperationException(); return this; }
            @Override public Instant instant() { return time.get(); }
        };
    }
}
