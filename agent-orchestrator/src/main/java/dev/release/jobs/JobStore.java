package dev.release.jobs;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Durable boundary. False on a fenced write means ownership was lost; callers must not retry it unfenced. */
public interface JobStore {
    UUID enqueue(String key, Job.Spec spec) throws SQLException;
    Optional<Job> find(UUID id) throws SQLException;
    Optional<Job.Claim> claim(String worker, Duration lease) throws SQLException;
    boolean renew(Job.Claim claim, Duration lease) throws SQLException;
    boolean complete(Job.Claim claim, String report) throws SQLException;
    boolean fail(Job.Claim claim, boolean retryable, Duration delay, Job.FailureCode code) throws SQLException;
    boolean cancel(UUID id) throws SQLException;
    Optional<Job.Result> result(UUID id) throws SQLException;
    List<Job.Attempt> attempts(UUID id) throws SQLException;
}
