package dev.release.jobs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Real PostgreSQL only. Run explicitly with -Ppostgres-it; absence of a database is a failure, not a skip. */
class PostgresJobStoreIT extends JobStoreContract {
    private Database database;
    private String schema;
    @BeforeAll static void configured() {
        assertNotNull(System.getenv("RELEASE_DB_URL"), "Set RELEASE_DB_URL to a real PostgreSQL test database");
    }
    @BeforeEach void setup() {
        schema = "it_" + UUID.randomUUID().toString().replace("-", "");
        database = new Database(System.getenv("RELEASE_DB_URL"), System.getenv().getOrDefault("RELEASE_DB_USER", "investigator"),
                System.getenv().getOrDefault("RELEASE_DB_PASSWORD", "local-demo-only"), schema, 8);
        database.migrate(); store = database.store();
    }
    @AfterEach void cleanup() throws Exception {
        if (database == null) return;
        try (var c = database.dataSource().getConnection(); var statement = c.createStatement()) {
            statement.execute("DROP SCHEMA " + schema + " CASCADE"); // Only this test's generated identifier.
        } finally { database.close(); }
    }
    @Override protected void advance(Duration elapsed) throws Exception {
        try (var c = database.dataSource().getConnection(); var statement = c.prepareStatement("""
                UPDATE investigation_job SET available_at = available_at - ? * interval '1 millisecond',
                lease_until = lease_until - ? * interval '1 millisecond'
                """)) {
            statement.setLong(1, elapsed.toMillis()); statement.setLong(2, elapsed.toMillis()); statement.executeUpdate();
        }
    }
    @Test void lockedJobIsSkippedWhileAnotherReadyJobCanBeClaimed() throws Exception {
        var locked = enqueue(); var ready = enqueue();
        try (var blocker = database.dataSource().getConnection()) {
            blocker.setAutoCommit(false);
            try (var query = blocker.prepareStatement("SELECT id FROM investigation_job WHERE id = ? FOR UPDATE")) {
                query.setObject(1, locked); try (var rows = query.executeQuery()) { assertTrue(rows.next()); }
            }
            try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                var claimed = callers.submit(() -> store.claim("nonblocking", LEASE));
                assertEquals(ready, claimed.get(2, java.util.concurrent.TimeUnit.SECONDS).orElseThrow().jobId());
            } finally { blocker.rollback(); }
        }
    }
    @Test void leaseExpiryIsCheckedAfterWaitingForTheJobRowLock() throws Exception {
        var id = enqueue(); var owner = claim();
        try (var blocker = database.dataSource().getConnection()) {
            blocker.setAutoCommit(false);
            try (var query = blocker.prepareStatement("SELECT id FROM investigation_job WHERE id = ? FOR UPDATE")) {
                query.setObject(1, id); try (var rows = query.executeQuery()) { assertTrue(rows.next()); }
            }
            try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                var completion = callers.submit(() -> store.complete(owner, "must be fenced"));
                boolean observed = false;
                long deadline = System.nanoTime() + Duration.ofMillis(750).toNanos();
                try (var observer = database.dataSource().getConnection(); var query = observer.createStatement()) {
                    query.setQueryTimeout(1);
                    while (!observed && System.nanoTime() - deadline < 0) {
                        try (var rows = query.executeQuery("""
                                SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid <> pg_backend_pid()
                                AND wait_event_type = 'Lock' AND query LIKE 'SELECT * FROM investigation_job WHERE id =%')
                                """)) { assertTrue(rows.next()); observed = rows.getBoolean(1); }
                    }
                }
                assertTrue(observed, "Writer did not reach PostgreSQL's lock wait");
                try (var expire = blocker.prepareStatement("UPDATE investigation_job SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?")) {
                    expire.setObject(1, id); expire.executeUpdate();
                }
                blocker.commit(); assertFalse(completion.get(2, java.util.concurrent.TimeUnit.SECONDS));
                assertTrue(store.result(id).isEmpty());
            } finally { blocker.rollback(); }
        }
    }

    @Test void resultAndTerminalStateRollBackTogetherOnDatabaseFailure() throws Exception {
        var id = enqueue(); var owner = claim();
        try (var c = database.dataSource().getConnection(); var statement = c.createStatement()) {
            statement.execute("ALTER TABLE investigation_result ADD CONSTRAINT reject_result CHECK (false)");
        }
        assertThrows(java.sql.SQLException.class, () -> store.complete(owner, "must roll back"));
        assertEquals(Job.State.RUNNING, store.find(id).orElseThrow().state());
        assertTrue(store.result(id).isEmpty()); assertEquals("RUNNING", store.attempts(id).getFirst().state());
    }
    @Test void migrationIsRepeatableAndPersistedResultIsUnique() throws Exception {
        database.migrate(); var id = enqueue(); var owner = claim(); store.complete(owner, "persisted");
        var reopened = new PostgresJobStore(database.dataSource());
        assertTrue(reopened.complete(owner, "persisted")); assertEquals("persisted", reopened.result(id).orElseThrow().report());
        try (var c = database.dataSource().getConnection(); var query = c.prepareStatement("SELECT count(*) FROM investigation_result WHERE job_id = ?")) {
            query.setObject(1, id); try (var rows = query.executeQuery()) { assertTrue(rows.next()); assertEquals(1, rows.getInt(1)); }
        }
    }
}
