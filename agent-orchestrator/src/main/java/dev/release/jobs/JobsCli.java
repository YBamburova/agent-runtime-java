package dev.release.jobs;

import dev.release.ToolRuntime;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class JobsCli {
    private JobsCli() {}
    public static void run(String[] args) throws Exception {
        if (args.length == 0) throw usage();
        if (args[0].equals("mock-demo")) {
            if (args.length != 1) throw usage();
            mockDemo(); return;
        }
        if (!List.of("migrate", "submit", "status", "cancel", "work").contains(args[0])) throw usage();
        try (var database = Database.fromEnvironment()) {
            JobStore store = database.store();
            switch (args[0]) {
                case "migrate" -> { if (args.length != 1) throw usage(); database.migrate(); System.out.println("Migrations applied"); }
                case "submit" -> {
                    if (args.length < 2 || args.length > 5) throw usage();
                    var scenario = args.length < 3 ? Job.Scenario.HEALTHY
                            : Job.Scenario.valueOf(args[2].toUpperCase(java.util.Locale.ROOT).replace('-', '_'));
                    var spec = new Job.Spec(scenario, Duration.ofMillis(args.length < 4 ? 20_000 : Long.parseLong(args[3])),
                            args.length < 5 ? 3 : Integer.parseInt(args[4]));
                    System.out.println(store.enqueue(args[1], spec));
                }
                case "status" -> { if (args.length != 2) throw usage(); print(store, UUID.fromString(args[1])); }
                case "cancel" -> { if (args.length != 2) throw usage(); System.out.println("cancelled=" + store.cancel(UUID.fromString(args[1]))); }
                case "work" -> {
                    if (args.length > 2 || (args.length == 2 && !args[1].equals("--pause-before-commit"))) throw usage();
                    work(database, args.length == 2);
                }
                default -> throw usage();
            }
        } catch (java.sql.SQLException | org.flywaydb.core.api.FlywayException e) {
            throw new IllegalStateException("Database operation failed; check database setup and PostgreSQL logs");
        }
    }
    private static void work(Database database, boolean pause) throws Exception {
        var config = JobWorkers.Config.defaults();
        try (var tools = new ToolRuntime(config.workers() * 3, config.workers() * 3)) {
            JobHandler handler = new InvestigationJobHandler(tools);
            JobHandler execution = claim -> {
                String report = handler.execute(claim);
                System.out.printf("EXECUTED %s attempt=%d%n", claim.jobId(), claim.attempt());
                if (pause) new CountDownLatch(1).await(); // Deliberate crash window for the recovery demonstration.
                return report;
            };
            try (var workers = new JobWorkers(database.store(), execution, config, JobsCli::event)) {
                var stop = new CountDownLatch(1);
                Thread hook = new Thread(() -> {
                    workers.close(); tools.close(); database.close(); stop.countDown();
                }, "job-shutdown");
                Runtime.getRuntime().addShutdownHook(hook);
                try { workers.start(); stop.await(); }
                finally {
                    try { Runtime.getRuntime().removeShutdownHook(hook); }
                    catch (IllegalStateException ignored) { /* JVM shutdown already owns hook execution. */ }
                }
            }
        }
    }
    private static void mockDemo() throws Exception {
        System.out.println("IN-MEMORY MOCK: no PostgreSQL, no durable storage, no process-recovery claim.");
        JobStore store = new InMemoryJobStore(Clock.systemUTC());
        UUID healthy = store.enqueue("mock-healthy", new Job.Spec(Job.Scenario.HEALTHY, Duration.ofSeconds(2), 3));
        UUID partial = store.enqueue("mock-partial", new Job.Spec(Job.Scenario.PARTIAL, Duration.ofSeconds(2), 3));
        UUID cancelled = store.enqueue("mock-cancelled", new Job.Spec(Job.Scenario.HEALTHY, Duration.ofSeconds(2), 3));
        store.cancel(cancelled);
        var committed = new CountDownLatch(2);
        var config = JobWorkers.Config.defaults();
        try (var tools = new ToolRuntime(6, 6);
             var workers = new JobWorkers(store, new InvestigationJobHandler(tools), config, event -> {
                 event(event);
                 if (event.kind().equals("COMMITTED")) committed.countDown();
             })) {
            workers.start();
            if (!committed.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Mock demo did not complete");
        }
        print(store, healthy); print(store, partial); print(store, cancelled);
    }
    private static void print(JobStore store, UUID id) throws java.sql.SQLException {
        var job = store.find(id).orElseThrow(() -> new IllegalArgumentException("Unknown job"));
        System.out.printf("id=%s state=%s attempts=%d%n", id, job.state(), job.attempts());
        for (var attempt : store.attempts(id))
            System.out.printf("attempt=%d state=%s failure=%s%n", attempt.number(), attempt.state(), attempt.failureCode());
        var result = store.result(id);
        if (result.isPresent()) System.out.printf("RESULT attempt=%d%n%s%n", result.get().attempt(), result.get().report());
    }
    private static void event(JobWorkers.Event event) {
        System.out.printf("%s job=%s attempt=%d%n", event.kind(), event.jobId(), event.attempt());
    }
    private static IllegalArgumentException usage() {
        return new IllegalArgumentException("jobs migrate | submit <key> [scenario] [budget-ms] [max-attempts] | status <id> | cancel <id> | work [--pause-before-commit] | mock-demo");
    }
}
