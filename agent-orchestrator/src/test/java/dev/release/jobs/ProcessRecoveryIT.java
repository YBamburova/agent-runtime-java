package dev.release.jobs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ProcessRecoveryIT {
    @Test @Timeout(100) void killedProcessIsRecoveredWithOnePersistedOutcome() throws Exception {
        assertNotNull(System.getenv("RELEASE_DB_URL"), "Set RELEASE_DB_URL to a real PostgreSQL test database");
        String schema = "recovery_it_" + UUID.randomUUID().toString().replace("-", "");
        Path output = Files.createTempFile("recovery-it-", ".log");
        var process = new ProcessBuilder("python3", "scripts/demo_recovery.py", "--schema", schema,
                "--java", Path.of(System.getProperty("java.home"), "bin", "java").toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(90, TimeUnit.SECONDS), "Process demo exceeded its bound");
            assertEquals(0, process.exitValue(), "Recovery demo failed; inspect " + output);
            String observed = Files.readString(output);
            assertTrue(observed.contains("\"persisted_results\": 1"));
            try (var database = new Database(System.getenv("RELEASE_DB_URL"),
                    System.getenv().getOrDefault("RELEASE_DB_USER", "investigator"),
                    System.getenv().getOrDefault("RELEASE_DB_PASSWORD", "local-demo-only"), schema, 2)) {
                try (var c = database.dataSource().getConnection(); var query = c.createStatement();
                     var rows = query.executeQuery("SELECT count(*) FROM investigation_result")) {
                    assertTrue(rows.next()); assertEquals(1, rows.getInt(1));
                }
                try (var c = database.dataSource().getConnection(); var drop = c.createStatement()) {
                    drop.execute("DROP SCHEMA " + schema + " CASCADE");
                }
            }
        } finally {
            if (process.isAlive()) {
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }
}
