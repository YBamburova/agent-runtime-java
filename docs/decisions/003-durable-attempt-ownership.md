# ADR 003: durable attempt ownership, fencing and explicit mock verification

Status: implementation accepted for review; mock policy checks passed. Real PostgreSQL acceptance checks remain pending.

## Decision

Model one durable job, a bounded series of separately audited execution attempts, and at most one persisted result. Use PostgreSQL transactions and row locks for ownership; a monotonically increasing attempt number is the fencing token. Reuse the existing bounded investigation and fake-provider components inside the job handler. Keep the application in one process and one Maven artifact.

`JobStore` is a meaningful persistence boundary shared by the PostgreSQL adapter and the user-requested in-memory mock. The mock is never selected silently when PostgreSQL fails. It is reachable only through `jobs mock-demo` and the mock tests, and clearly says its data is lost on exit. No mock test is labeled as proof of PostgreSQL locking, durability, migration or recovery behavior.

## Why PostgreSQL row locks and a fence?

`FOR UPDATE SKIP LOCKED` permits independent workers to claim available rows without waiting behind an already locked candidate. It is suitable for a queue rather than a consistent general-purpose read. Selecting the row, ending an expired attempt, incrementing its number and installing a lease happen in one short transaction.

A lease allows another worker to recover after failure, but the old worker may still run. Checking only the worker id, or only whether a lease was once acquired, does not protect later effects. Every write validates the active attempt, worker and unexpired lease under a row lock. The time predicate runs in a subsequent statement after lock acquisition, so time spent waiting for that lock cannot authorize an expired owner.

Result insertion, attempt completion and terminal job state commit atomically. A result's primary key prevents a second persisted outcome; it does not prevent repeated external calls. Repeated completion with the same authorized attempt/body is idempotent; changed or stale bodies cannot overwrite it. An uncertain commit is left to persisted state and lease recovery rather than followed by an unfenced compensating update.

## Why explicit workers and heartbeats?

A fixed number of virtual worker loops claim only when free, with no executor backlog or detached scheduled retry tasks. Each running handler owns one heartbeat. The worker joins that heartbeat before starting another job, bounding actual resource lifetime when interruption does not immediately stop JDBC work.

Shutdown requests cancellation, stops further renewal, and waits within one grace budget. Cooperative work reschedules using its current fence. Uncooperative work can outlive grace and lease expiry; a replacement may repeat external execution. Fencing covers persisted effects, not remote calls. Heartbeat uncertainty is stored in an atomic flag so a handler clearing interruption cannot accidentally authorize publication.

## Established components and dependencies

Use Flyway for migration history/validation and HikariCP for bounded connection pooling. Hand-writing either would add infrastructure unrelated to the project's concurrency lesson. The JDBC store contains straightforward SQL and a private transaction helper; there is no generic persistence framework or workflow engine.

The default build supports mock operation without pgJDBC. The opt-in `postgres` profile pins pgJDBC 42.7.14, verified against its official download documentation, rather than selecting an older cached driver just to make a database build appear verified offline. Flyway 11.7.2 and HikariCP 6.3.3 are documented cached releases, not latest-version claims. Jackson BOM 2.21.4 aligns Flyway's transitive modules, and SLF4J 2.0.18 aligns the pool's logging API. Shade 3.6.0 packages runtime dependencies and merges services/notices; its build-only Commons Compress 1.28.0 / Codec 1.19.0 overrides resolve missing offline plugin dependencies. Clean Plugin 3.4.1 is pinned rather than relying on an uncached Maven lifecycle default. No network or paid-provider adapter is involved in the mock.

## Alternatives and limits

- A single jobs table with no attempt history is smaller but obscures whether a retry, worker replacement or child task performed an operation. Separate attempts make failure recovery explainable.
- Holding a transaction while an investigation runs would make database locks and connections depend on arbitrary external latency. All execution happens after claim commit, with renewable ownership.
- Redis/Kafka or multiple services are unnecessary for this bounded local portfolio scope.
- Exactly-once execution cannot be inferred from a unique result row. External idempotency keys would require support from each external service; that is not implemented here.
- Durable retries use a configurable fixed delay and bounded attempt count. LLM transient retries remain owned by the gateway inside an attempt; restarting the whole durable attempt can repeat those calls. The two budgets are distinct and do not provide billing idempotency.
- This is a small educational scheduler, with no global worker quota, queue-size cap, retention/dead-letter operations, job-wide expiry or fairness guarantee.

## Official references checked

- [PostgreSQL SELECT and locking clauses](https://www.postgresql.org/docs/current/sql-select.html)
- [PostgreSQL date/time functions](https://www.postgresql.org/docs/current/functions-datetime.html), including actual wall-clock `clock_timestamp()` rather than transaction-start time
- [pgJDBC downloads and versions](https://jdbc.postgresql.org/download/) and [connection parameters/timeouts](https://jdbc.postgresql.org/documentation/use/)
- [Flyway Java API](https://documentation.red-gate.com/flyway/reference/usage/api-java) and [11.7.2 release](https://github.com/flyway/flyway/releases/tag/flyway-11.7.2)
- [HikariCP 6.3.3 release](https://github.com/brettwooldridge/HikariCP/releases/tag/HikariCP-6.3.3)
- [Jackson 2.21.4 release](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.21.4), [SLF4J releases](https://www.slf4j.org/news.html)
- [Maven Clean 3.4.1](https://maven.apache.org/plugins-archives/maven-clean-plugin-3.4.1/plugin-info.html)
- [Maven Shade 3.6.0](https://maven.apache.org/components/plugins-archives/maven-shade-plugin-3.6.0/plugin-info.html)
- [PostgreSQL 17.6 release](https://www.postgresql.org/docs/17/release-17-6.html), the pinned local Compose target; image startup was not verified here
