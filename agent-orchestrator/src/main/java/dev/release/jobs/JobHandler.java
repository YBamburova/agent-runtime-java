package dev.release.jobs;

@FunctionalInterface
public interface JobHandler {
    String execute(Job.Claim claim) throws Exception;

    /** Only an explicitly retryable execution failure schedules another durable attempt. */
    final class Failure extends Exception {
        private final boolean retryable;
        public Failure(boolean retryable) { super("Job execution failed"); this.retryable = retryable; }
        public boolean retryable() { return retryable; }
    }
}
