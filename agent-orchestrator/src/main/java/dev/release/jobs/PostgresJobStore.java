package dev.release.jobs;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Every state transition is a short READ COMMITTED transaction; tool/provider I/O is outside it. */
public final class PostgresJobStore implements JobStore {
    private final DataSource dataSource;
    public PostgresJobStore(DataSource dataSource) { this.dataSource = Objects.requireNonNull(dataSource); }

    @Override public UUID enqueue(String key, Job.Spec spec) throws SQLException {
        Job.key(key); Objects.requireNonNull(spec);
        return transaction(c -> {
            UUID id = UUID.randomUUID();
            execute(c, """
                    INSERT INTO investigation_job(id, submission_key, scenario, budget_ms, max_attempts)
                    VALUES (?, ?, ?, ?, ?) ON CONFLICT (submission_key) DO NOTHING
                    """, id, key, spec.scenario().name(), Job.millis(spec.budget()), spec.maxAttempts());
            try (var query = statement(c, "SELECT * FROM investigation_job WHERE submission_key = ?", key);
                 var rows = query.executeQuery()) {
                if (!rows.next()) throw new SQLException("Enqueued job not visible");
                Job job = job(rows);
                if (!job.spec().equals(spec)) throw new IllegalArgumentException("Key already used for different job settings");
                return job.id();
            }
        });
    }

    @Override public Optional<Job> find(UUID id) throws SQLException {
        return transaction(c -> selectJob(c, id, false));
    }

    @Override public Optional<Job.Claim> claim(String worker, Duration lease) throws SQLException {
        Job.worker(worker); long leaseMs = Job.millis(lease);
        return transaction(c -> {
            Job job;
            try (var query = statement(c, """
                    SELECT * FROM investigation_job
                    WHERE (state = 'QUEUED' AND available_at <= clock_timestamp())
                       OR (state = 'RUNNING' AND lease_until <= clock_timestamp())
                    ORDER BY available_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
                    """); var rows = query.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                job = job(rows);
            }
            if (job.state() == Job.State.RUNNING) finishAttempt(c, job.id(), job.attempts(), "EXPIRED", Job.FailureCode.LEASE_EXPIRED);
            if (job.attempts() >= job.spec().maxAttempts()) {
                execute(c, """
                        UPDATE investigation_job SET state = 'FAILED', worker_id = NULL, lease_until = NULL,
                        failure_code = 'ATTEMPTS_EXHAUSTED' WHERE id = ?
                        """, job.id());
                return Optional.empty();
            }
            int attempt = job.attempts() + 1;
            execute(c, """
                    INSERT INTO investigation_attempt(job_id, number, worker_id, state) VALUES (?, ?, ?, 'RUNNING')
                    """, job.id(), attempt, worker);
            execute(c, """
                    UPDATE investigation_job SET state = 'RUNNING', attempt_count = ?, worker_id = ?,
                    lease_until = clock_timestamp() + ? * interval '1 millisecond', failure_code = NULL WHERE id = ?
                    """, attempt, worker, leaseMs, job.id());
            return Optional.of(new Job.Claim(job.id(), attempt, worker, job.spec()));
        });
    }

    @Override public boolean renew(Job.Claim claim, Duration lease) throws SQLException {
        long leaseMs = Job.millis(lease);
        return transaction(c -> owned(c, claim) && guarded(c, claim,
                "SET lease_until = clock_timestamp() + ? * interval '1 millisecond'", leaseMs) == 1);
    }

    @Override public boolean complete(Job.Claim claim, String report) throws SQLException {
        Job.report(report);
        return transaction(c -> {
            var job = selectJob(c, claim.jobId(), true);
            if (job.isEmpty()) return false;
            if (job.get().state() == Job.State.SUCCEEDED) {
                try (var query = statement(c, """
                        SELECT 1 FROM investigation_result r JOIN investigation_attempt a
                        ON a.job_id = r.job_id AND a.number = r.attempt_number
                        WHERE r.job_id = ? AND r.attempt_number = ? AND r.report = ? AND a.worker_id = ?
                        """, claim.jobId(), claim.attempt(), report, claim.worker()); var rows = query.executeQuery()) {
                    return rows.next(); // Same committed effect is idempotent; a different body is never overwritten.
                }
            }
            if (!identity(job.get(), claim)) return false;
            if (guarded(c, claim, "SET state = 'SUCCEEDED', worker_id = NULL, lease_until = NULL, failure_code = NULL") != 1) return false;
            execute(c, "INSERT INTO investigation_result(job_id, attempt_number, report) VALUES (?, ?, ?)",
                    claim.jobId(), claim.attempt(), report);
            finishAttempt(c, claim.jobId(), claim.attempt(), "SUCCEEDED", null);
            return true;
        });
    }

    @Override public boolean fail(Job.Claim claim, boolean retryable, Duration delay, Job.FailureCode code) throws SQLException {
        long delayMs = Job.millis(delay); Objects.requireNonNull(code);
        return transaction(c -> {
            if (!owned(c, claim)) return false;
            String state = retryable && claim.attempt() < claim.spec().maxAttempts() ? "QUEUED" : "FAILED";
            if (guarded(c, claim, """
                    SET state = ?, available_at = clock_timestamp() + ? * interval '1 millisecond',
                    worker_id = NULL, lease_until = NULL, failure_code = ?
                    """, state, delayMs, code.name()) != 1) return false;
            finishAttempt(c, claim.jobId(), claim.attempt(), "FAILED", code);
            return true;
        });
    }

    @Override public boolean cancel(UUID id) throws SQLException {
        return transaction(c -> {
            var found = selectJob(c, id, true);
            if (found.isEmpty()) return false;
            Job job = found.get();
            if (job.state() != Job.State.RUNNING && job.state() != Job.State.QUEUED) return false;
            if (job.state() == Job.State.RUNNING) finishAttempt(c, id, job.attempts(), "CANCELLED", Job.FailureCode.CANCELLED);
            execute(c, """
                    UPDATE investigation_job SET state = 'CANCELLED', worker_id = NULL,
                    lease_until = NULL, failure_code = 'CANCELLED' WHERE id = ?
                    """, id);
            return true;
        });
    }

    @Override public Optional<Job.Result> result(UUID id) throws SQLException {
        return transaction(c -> {
            try (var query = statement(c, "SELECT attempt_number, report FROM investigation_result WHERE job_id = ?", id);
                 var rows = query.executeQuery()) {
                return rows.next() ? Optional.of(new Job.Result(rows.getInt(1), rows.getString(2))) : Optional.empty();
            }
        });
    }

    @Override public List<Job.Attempt> attempts(UUID id) throws SQLException {
        return transaction(c -> {
            var attempts = new ArrayList<Job.Attempt>();
            try (var query = statement(c, "SELECT * FROM investigation_attempt WHERE job_id = ? ORDER BY number", id);
                 var rows = query.executeQuery()) {
                while (rows.next()) attempts.add(new Job.Attempt(rows.getInt("number"), rows.getString("worker_id"),
                        rows.getString("state"), instant(rows, "started_at"), instant(rows, "finished_at"), rows.getString("failure_code")));
            }
            return List.copyOf(attempts);
        });
    }

    private static Optional<Job> selectJob(Connection c, UUID id, boolean lock) throws SQLException {
        try (var query = statement(c, "SELECT * FROM investigation_job WHERE id = ?" + (lock ? " FOR UPDATE" : ""), id);
             var rows = query.executeQuery()) {
            return rows.next() ? Optional.of(job(rows)) : Optional.empty();
        }
    }
    private static boolean owned(Connection c, Job.Claim claim) throws SQLException {
        var job = selectJob(c, claim.jobId(), true);
        return job.isPresent() && identity(job.get(), claim);
    }
    private static boolean identity(Job job, Job.Claim claim) {
        return job.state() == Job.State.RUNNING && job.attempts() == claim.attempt() && claim.worker().equals(job.worker())
                && job.spec().equals(claim.spec());
    }
    private static int guarded(Connection c, Job.Claim claim, String update, Object... values) throws SQLException {
        var arguments = new ArrayList<>(List.of(values));
        arguments.add(claim.jobId()); arguments.add(claim.attempt()); arguments.add(claim.worker());
        // Caller already holds the row lock. This NEW statement checks wall time after any lock wait.
        return execute(c, "UPDATE investigation_job " + update + """
                 WHERE id = ? AND state = 'RUNNING' AND attempt_count = ? AND worker_id = ?
                   AND lease_until > clock_timestamp()
                """, arguments.toArray());
    }
    private static void finishAttempt(Connection c, UUID id, int number, String state, Job.FailureCode code) throws SQLException {
        int changed = execute(c, """
                UPDATE investigation_attempt SET state = ?, finished_at = clock_timestamp(), failure_code = ?
                WHERE job_id = ? AND number = ? AND state = 'RUNNING'
                """, state, code == null ? null : code.name(), id, number);
        if (changed != 1) throw new SQLException("Attempt state invariant violated");
    }
    private static Job job(ResultSet row) throws SQLException {
        var spec = new Job.Spec(Job.Scenario.valueOf(row.getString("scenario")), Duration.ofMillis(row.getLong("budget_ms")), row.getInt("max_attempts"));
        return new Job(row.getObject("id", UUID.class), row.getString("submission_key"), spec,
                Job.State.valueOf(row.getString("state")), row.getInt("attempt_count"), instant(row, "available_at"),
                instant(row, "lease_until"), row.getString("worker_id"), row.getString("failure_code"));
    }
    private static Instant instant(ResultSet rows, String column) throws SQLException {
        var timestamp = rows.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
    private static PreparedStatement statement(Connection c, String sql, Object... arguments) throws SQLException {
        var statement = c.prepareStatement(sql);
        try {
            statement.setQueryTimeout(3);
            for (int i = 0; i < arguments.length; i++) statement.setObject(i + 1, arguments[i]);
            return statement;
        } catch (SQLException | RuntimeException e) { statement.close(); throw e; }
    }
    private static int execute(Connection c, String sql, Object... arguments) throws SQLException {
        try (var statement = statement(c, sql, arguments)) { return statement.executeUpdate(); }
    }
    @FunctionalInterface private interface Transaction<T> { T run(Connection connection) throws SQLException; }
    private <T> T transaction(Transaction<T> work) throws SQLException {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try {
                try (var settings = connection.createStatement()) {
                    settings.execute("SET LOCAL lock_timeout = '1000ms'");
                    settings.execute("SET LOCAL statement_timeout = '3000ms'");
                }
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error e) {
                try { connection.rollback(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
                throw e;
            }
        }
    }
}
