# Milestone 3 verification record

Date: 2026-11-10. The user requested mock verification after local PostgreSQL access was unavailable. **This record does not claim PostgreSQL concurrency, transaction, migration or crash-recovery checks passed.**

## Environment and scope

macOS 27.0.1 arm64, Homebrew OpenJDK 27 build 27, Maven Wrapper / Maven 3.10.0, compiler release 25. Exact JDK 25 remains unavailable and unverified. Dependencies for the default mock build were resolved from a writable copy of the local Maven cache at `/tmp/release-m2/repository`.

Docker client access to the configured Colima socket returned permission denied. No local `postgres`, `initdb`, `pg_ctl` or `psql` executable/server and no database environment configuration were available. Shell access to Maven Central failed DNS resolution. The user then explicitly answered “jus do the mock.” No attempt was made to bypass those restrictions or turn a mock into database verification.

The PostgreSQL driver is an opt-in runtime dependency in `-Ppostgres`: version 42.7.14, checked against official documentation. Only 42.7.11 was cached, so the real profile was not built or represented as successfully resolved. Flyway/Hikari and the store implementation compile in the normal build without a driver. All real integration-test sources also compile in the normal test-compile phase, but their `*IT` classes execute only with the integration profile.

## Implemented changes

- Store boundary, job/spec/claim/history/result values, explicitly non-durable in-memory store and JDBC PostgreSQL adapter.
- Flyway V1 schema with submission-key uniqueness, distinct attempt history, lease metadata, partial claim indexes and one result row per job.
- Fenced renewal/completion/failure and serialized cancellation. Lease time is checked after acquiring the row lock.
- Bounded virtual worker loops, one heartbeat per actual execution, fixed delayed retry eligibility, bounded attempts, shutdown and uncertain-ownership handling.
- Existing evidence tools and fake gateway reused by the durable job handler; execution success is distinguished from investigation outcome.
- Mock CLI, PostgreSQL CLI, local Compose file, environment example, real recovery script and opt-in integration profile.
- Shaded executable JAR with migration resources, merged services and merged third-party notices.

## Executed checks

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home \
  ./mvnw -o -Dmaven.repo.local=/tmp/release-m2/repository clean verify
```

Result: **BUILD SUCCESS; 71 tests, 0 failures, 0 errors, 0 skipped.**

| Suite | Tests executed | Meaning |
| --- | ---: | --- |
| Existing investigation and gateway suites | 47 | Previous milestone regressions |
| `MemoryJobStoreTest` | 13 | Store policy contract against the explicit mock; includes competing submissions/claims and 20 completion/cancellation races |
| `JobWorkersTest` | 11 | Bounded handlers/no prefetch, heartbeats, cancellation, cooperative/uncooperative shutdown, retry classification, uncertain ownership/commit and integration with synthetic investigations |

Policy time advances explicitly in mock lease/retry tests. Worker tests coordinate with latches, observed events and bounded waits. Production heartbeat/poll scheduling uses interruptible timed waits; correctness assertions do not depend on arbitrary sleeps. None of these tests models PostgreSQL's MVCC or proves durable recovery.

Packaged CLI commands were executed using `/opt/homebrew/opt/openjdk/bin/java`:

```sh
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar jobs mock-demo
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar overload
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar healthy
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar partial
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar timeout 200
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar required-failure
```

Observed: the mock prints its in-memory/non-durable label, commits healthy and partial investigation reports at attempt 1, stops both workers and prints the queued cancelled job at attempt count 0. Overload still reports one in-flight/one waiting request, rejects the third with zero attempts, completes the first two and drains live counters. Healthy still produces `COMPLETED` with successful deterministic fake synthesis. Final packaged smoke checks also confirmed `PARTIAL`, `TIMED_OUT` and `FAILED` for the remaining original scenarios. The default mock artifact rejects a database command with the explicit `build with -Ppostgres` diagnostic, without falling back to memory.

Additional executed checks:

- `docker-compose -f compose.yaml config`: passed static configuration validation. This did not connect to PostgreSQL or start a container.
- `python3 scripts/demo_recovery.py --help` and Python AST parsing: passed. These did not execute recovery.
- Packaged JAR inspection: V1 migration and merged Flyway plugin descriptor present; default mock artifact contains no pgJDBC driver.
- Local Markdown targets checked to resolve.

An offline clean-build attempt also exposed Maven's uncached implicit Clean 3.5.0; pinning the documented cached Clean 3.4.1 resolved it. The final clean build recompiled all 20 application and 8 test sources.

The first packaging attempt failed because Shade's original Commons Compress 1.26.2 / Codec 1.17.0 artifacts were not cached. Pinning its build-only dependencies to documented cached 1.28.0 / 1.19.0 resolved packaging. License and notice resources are merged. Shade still notes overlapping dependency manifests; the executable's main class was verified through the CLI runs.

## Supplied but not executed

With a real dedicated PostgreSQL database, a current pgJDBC download and JDK 25:

```sh
./mvnw -Ppostgres,postgres-it clean verify
python3 scripts/demo_recovery.py
```

`PostgresJobStoreIT` supplies the 13 shared contract cases and four SQL-specific cases: locked-row skipping, expired lease after a row-lock wait, rollback of result/state/attempt together, and migration repeatability/persisted-result uniqueness. `ProcessRecoveryIT` runs the actual kill/restart script and counts persisted results. These checks are **pending**, not passing or silently skipped.

The standalone script kills a process only after observing completed external execution and before result commit. It waits for real lease expiry and verifies attempt 1 expired, replacement attempt 2 succeeded and one result was persisted. It leaves a generated schema for inspection. It must not be described as demonstrated recovery until executed successfully against PostgreSQL.

Also not run: Docker image startup, real database profile packaging, real migration execution, driver/pool cancellation behavior, Windows execution, exact JDK 25 execution, CI/stress workflows, JMH or load benchmarks. No performance numbers exist.

## Design decisions to understand

The attempt number fences database effects even when old external work continues. Job row locking serializes cancellation and completion; the post-lock time predicate prevents expired owners from writing after a lock wait. An uncertain commit is not followed by a compensating retry update. One unique result prevents duplicate persisted outcomes, not duplicate provider calls. Shutdown and cancellation request interruption without claiming arbitrary external work has stopped.

## Interview questions

1. Why must lease validity be checked after acquiring the row lock, and what does the attempt fence prevent that a lease alone does not?
2. How does the worker handle a commit that might have succeeded even though its acknowledgement was lost?
3. Why can recovery produce one persisted report while repeating investigation and LLM calls, and how are jobs, attempts and child tasks different?
