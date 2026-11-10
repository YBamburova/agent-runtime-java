# agent-runtime-java — Release Investigator

A Java concurrency portfolio project investigating a synthetic incident: **why did checkout errors increase after release v42?** It gathers logs, deployment history and dependency status concurrently, then reports evidence, a hypothesis, source failures and limitations.

The engineering subject is reliable concurrent execution: ownership, bounded resources, cancellation and deadlines. Everything currently runs in one process, in one Maven application artifact, with no API credentials or separately installed application libraries. The default demo needs no database.

## Current scope

Milestone 1 is implemented: CLI, packaged synthetic fixtures, parallel blocking tools on virtual threads, bounded active calls and waiting work, investigation and tool budgets, required/optional failure policies, immutable partial reports and concurrency tests. Verification passed locally on JDK 27 targeting Java 25; **exact JDK 25 execution remains unverified** in this environment.

Milestone 2 is implemented: a deterministic fake LLM provider adds optional synthesis through a provider-specific gateway with bounded admission, request/token quotas, retries and a circuit breaker. The evidence report keeps its transparent v42 rule and remains usable without synthesis. No SDK or runtime dependency was added. Milestones 4–5 and the optional Anthropic adapter remain planned. See the [roadmap](docs/architecture/roadmap.md) and [gateway architecture and acceptance criteria](docs/architecture/milestone-2.md).

Milestone 3 adds a PostgreSQL job store, Flyway migration, bounded workers, leases, fencing, retries and cancellation. **Only the in-memory mock and worker policies are verified here**, as requested when local database access was unavailable. The SQL path and real process-recovery tests are implemented but unverified. See [scheduling architecture and acceptance status](docs/architecture/milestone-3.md).

## Run locally

Install **JDK 25** and set `JAVA_HOME` to its installation. The Maven Wrapper pins Maven 3.10.0; the first build needs internet access for Maven and test/build dependencies. No database or paid service is needed.

```sh
java -version
./mvnw verify
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar
```

On Windows use `mvnw.cmd verify`. No preview flag is needed: both implemented milestones use stable APIs only. When structured concurrency is introduced, its selected JDK API and preview requirements must be explicitly configured for compilation, tests and execution.

## Five-minute demonstration

1. Run the default healthy investigation. The report connects a payment timeout reduction from 2000ms to 500ms with observed payment latency of 850ms and increased checkout errors. This is a hypothesis, not causal proof. The gateway then prints deterministic fake synthesis, labeled explicitly, using the remaining investigation budget.
2. Fail the optional dependency source and observe retained logs and deployment evidence:

   ```sh
   java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar partial
   ```

3. Block that source and give the whole investigation 200ms:

   ```sh
   java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar timeout 200
   ```

   Expect `TIMED_OUT`; fast evidence is retained when accepted before the deadline. OS scheduling can affect which evidence is observed before a very short budget.
4. Fail the required log source:

   ```sh
   java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar required-failure
   ```

   Expect `FAILED`; outstanding siblings receive interruption. A sibling already observed as successful remains evidence.
5. Saturate the gateway with one active and one waiting request:

   ```sh
   java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar overload
   ```

   The third request gets `OVERLOADED` with zero attempts. Releasing the provider allows both admitted requests to succeed and returns all live counters to zero. Latches make this reproducible; it is not a load benchmark. Run `./mvnw test` to explore quota, retry, circuit and cancellation behavior as well.

CLI arguments are `[healthy|partial|timeout|required-failure] [deadline-ms]`, or `overload`. The default deadline is 20 seconds; each tool has a five-second budget including queue wait. `timeout` with the default overall budget produces `PARTIAL` after the optional source's five-second budget expires. The CLI prints a report for all terminal states; a report's failure state currently does not change the process exit code. Ctrl-C requests evidence-investigation cancellation during JVM shutdown; the tests demonstrate cancellation independent of shutdown output timing.

## Scheduling: runnable mock and optional PostgreSQL

The database-free worker demo runs two investigations and cancels a queued job:

```sh
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar jobs mock-demo
```

It explicitly prints `IN-MEMORY MOCK`. Its jobs disappear when the process exits; it does not establish crash durability or PostgreSQL concurrency correctness.

To use the PostgreSQL implementation on a machine with Docker access:

```sh
docker compose up -d --wait
./mvnw -Ppostgres clean package
export RELEASE_DB_URL=jdbc:postgresql://localhost:55432/investigator
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar jobs migrate
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar jobs submit checkout-v42 healthy 20000 3
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar jobs work
```

Use the returned UUID with `jobs status <uuid>` or `jobs cancel <uuid>` in another terminal. Ctrl-C requests bounded worker shutdown. `docker-compose` can be used where the standalone Compose command is installed instead of the Docker plugin. The Compose file binds PostgreSQL only to loopback and uses synthetic local credentials; see [.env.example](.env.example) for environment settings. Environment files are not loaded automatically.

The `postgres` build profile adds pgJDBC 42.7.14. It is opt-in so the mock build stays runnable with cached dependencies and no database driver. The ordinary `verify` command does not execute real database tests. To verify the real path on a dedicated test database:

```sh
./mvnw -Ppostgres,postgres-it clean verify
python3 scripts/demo_recovery.py
```

These commands have **not** run successfully here. The integration suite creates/drops generated test schemas; the standalone recovery script leaves its generated schema for inspection. It kills one worker after external execution but before result commit, then waits for lease expiry and replacement execution. [Detailed setup, lifecycle and limitations](docs/architecture/milestone-3.md) distinguish this real recovery demonstration from the mock.

## Concurrency guarantees

- `ToolRuntime(activeLimit, waitingLimit)` bounds executing tool bodies and physically queued work across investigations sharing that runtime. The demo uses 3 active and 3 queued calls. A full or closed runtime rejects immediately with a typed `REJECTED` source outcome. Both configured limits must be positive.
- Each investigation contains 1–16 distinct tools and runs once. Its calling thread owns aggregation. The runtime does not create coordinator threads; callers must bound concurrent investigation callers when embedding it.
- `cancel()` and terminal publication share a monitor. A cancellation accepted before publication determines the final `CANCELLED` state; a later cancellation returns false. There is exactly one immutable report.
- Cancelling queued tasks removes them from the queue. Running tasks retain their active capacity until their bodies actually exit, even if their futures are already cancelled. Late evidence cannot mutate a published report.
- Optional source failure permits a partial report. Required source failure, timeout, rejection or interruption fails the investigation and cancels pending siblings. Overall deadline expiration produces `TIMED_OUT`; accepted cancellation takes precedence at publication.
- Deadlines use monotonic elapsed time. The coordinator stops accepting evidence when it observes the overall deadline; per-tool completion timestamps prevent accepting results completed after the tool budget. This is a scheduling budget, not a hard real-time return guarantee.
- Tools own resources they open. Fixtures use try-with-resources. Cooperative tools respond to interruption; uncooperative external work can outlive the report and runtime shutdown. `close()` requests shutdown without waiting indefinitely; `awaitTermination(budget)` separately observes bounded cleanup. A future's cancelled state does not prove cleanup finished.

Gateway guarantees:

- `GatewayConfig` independently bounds in-flight provider attempts and waiting requests. Immediate semaphore admission also bounds callers contending for the policy lock. Retry backoff and quota waiting count toward the same waiting limit; an admitted request can still be rejected if a later retry cannot obtain waiting capacity.
- Every dispatched attempt consumes request quota and an estimated total-token reservation. Known usage refunds overestimates or creates debt for underestimates; failed/unknown-usage calls retain their estimate. Old-window completions cannot refund a new window.
- Only typed transient failures retry, with capped exponential full jitter and at most 10 configurable attempts. Every attempt rechecks the same caller deadline and reserves quotas again. The gateway owns all retries.
- A circuit opens after the configured transient-failure threshold. A cooldown permits one half-open probe; generation checks prevent older calls from changing newer circuit state. Separate gateways isolate provider policy and capacity.
- Cancellation is caller-thread interruption. Waiting cleanup is immediate when observed; active capacity is released only after provider code exits. Blocking providers should be called on caller-owned virtual threads. The gateway creates no executor or background timer.

Default gateway limits: 2 in-flight, 4 waiting, 60 requests and 10,000 estimated/reconciled tokens per one-minute window, 3 attempts, 50ms initial / 1s maximum backoff, 3 failures to open, 5s cooldown. These are demo settings, not provider-specific production quotas. Reuse one gateway per provider/quota scope; the single-shot CLI creates one for its one synthesis call. Configuration and policy trade-offs are described in [ADR 002](docs/decisions/002-provider-gateway.md).

The [evidence architecture](docs/architecture/milestone-1.md) explains execution and state policy. [ADR 001](docs/decisions/001-bounded-tool-runtime.md) explains why this milestone uses a bounded executor and defers structured concurrency at untrusted cancellation boundaries.

## Verification and measurements

```sh
./mvnw verify
./mvnw -Dtest=InvestigationTest test
./mvnw '-Dtest=LlmGatewayTest,GatewayCircuitTest' test
```

JUnit uses latches and barriers, bounded waits and a suite-level per-test timeout. Deadline tests use actual short deadlines with blocked calls; correctness assertions do not depend on arbitrary sleeps. Tests cover overlap, active/queue bounds, overload, interruption, cleanup, cancellation races, partial evidence and uncooperative work. Gateway policy tests inject time, waiting and jitter, including a simulated spurious wakeup; concurrent transport tests use latches. The race test performs 100 iterations, which is regression evidence rather than a proof of all possible schedules.

See the [Milestone 1 verification](docs/architecture/verification.md), [Milestone 2 verification](docs/architecture/verification-milestone-2.md) and [Milestone 3 verification](docs/architecture/verification-milestone-3.md) for exact commands, environment and unavailable checks. No performance claims or benchmark results exist yet. JMH, application load scenarios and bounded stress CI are planned for Milestone 5; unit-test elapsed time is not a benchmark. Exact JDK 25 CI is still to be added and run.

## Boundaries and limitations

This is an educational incident simulator, not a production incident-response system. Evidence and the report hypothesis are tailored to a single synthetic release. There is no HTTP layer, cross-process provider-capacity control, general workflow graph or three-agent coordination yet. PostgreSQL persistence is implemented but its acceptance checks remain unverified; the default mock is not persistent. The optional official Anthropic adapter is deferred; no real-provider compatibility or SDK retry configuration has been tested. There is no guarantee of fair scheduling between investigations. Evidence tools share one downstream capacity pool. Provider gateways isolate their own limits; a shared caller executor or shared transport could still create unrelated bottlenecks. Report strings are suitable only for these trusted fixtures; arbitrary external evidence would need size limits and sanitization.

The gateway uses local fixed quota windows, permits boundary bursts and does not implement a tokenizer, distributed quotas, `Retry-After`, streaming, or retry idempotency. Retries can repeat billed calls; failed attempts with unknown usage keep their estimate. Actual usage from a previous dispatch window cannot amend the current ledger. A synchronous uncooperative provider can exceed its supplied budget and swallow interruption; late responses are discarded when the gateway regains control, but forced termination is impossible. These custom policies are isolated for teaching and are not a general resilience library.

The bounded evidence executor deliberately reuses a small number of virtual worker threads; this trades per-request virtual-thread lifecycle semantics for the JDK executor's physical queue and worker-exit accounting. See ADR 001 before extending this mechanism. A permanently stuck call permanently occupies one configured slot until the process exits; interruption cannot force third-party cleanup.

## Repository map

- `agent-orchestrator/`: application and tests; root `pom.xml` builds the single artifact.
- `scripts/`: real process-recovery demonstration (requires PostgreSQL).
- `compose.yaml`: optional local PostgreSQL startup.
- `fixtures/`: synthetic application evidence packaged into the JAR.
- `docs/architecture/`: ownership, acceptance criteria, roadmap and executed checks.
- `docs/decisions/`: design choices and alternatives.

Labs, benchmarks, failure studies and workflows will be created when they contain implemented and verified work.

## AI assistance

Codex assisted with the design, implementation, tests and documentation for the first three milestones. Mock verification for Milestone 3 was explicitly requested; database verification gaps remain documented. The design rationale and verification gaps are recorded for review. No real incident data, secrets, paid API calls or remote resources were used. AI assistance does not substitute for understanding or reviewing the concurrency invariants.
