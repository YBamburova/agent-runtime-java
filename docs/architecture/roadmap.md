# Incremental roadmap

Milestones 1 and 2 are implemented and locally tested. Milestone 3 is implemented with mock verification; real PostgreSQL and kill/restart acceptance checks remain pending. Later milestones require their own reviewable changes, tests, execution record, trade-offs and interview questions. No remote resources or deployment are required by this plan.

## Milestone 2 — gateway and overload (implemented)

A deterministic fake LLM provider adds optional synthesis while preserving evidence behavior. The provider boundary performs one blocking attempt; the gateway owns retries. See [implementation and acceptance criteria](milestone-2.md) and [executed verification](verification-milestone-2.md). The optional Anthropic adapter remains deferred; explicitly disable any SDK retries when adding the optional official Anthropic Java SDK later. Verify the current official SDK API and version at implementation time.

Acceptance criteria: separate request rate, token reservation/reconciliation and in-flight limits; bounded waiting and typed rejection; cancellation-safe release; eligible transient retries with exponential backoff, jitter, maximum attempts and deadline budget; circuit states with controlled half-open probes. Inject monotonic time and scheduling/randomness where needed. Tests must prove failed providers cannot consume unrelated provider capacity and waiting work remains bounded. Reproduce overload locally, without paid APIs. Choose small established components if they reduce risk; do not build a general resilience library.

## Milestone 3 — durable execution (implemented; mock verified, PostgreSQL verification pending)

Added a PostgreSQL store, Flyway migration, local database startup configuration, bounded worker loop and an explicitly non-durable in-memory mock. See [implementation and acceptance status](milestone-3.md) and [executed checks](verification-milestone-3.md). Model durable jobs separately from execution attempts and in-process scoped tasks. Use atomic claiming and lease/fencing ownership checks on every persisted effect; make terminal results unique per job. Define queued cancellation, running cancellation, graceful shutdown, delayed retries and bounded attempts.

Acceptance: real PostgreSQL integration tests for competing claims, lease expiry, stale-worker rejection, retries and idempotent results. Kill and restart a process to demonstrate recovery. Promise at-least-once attempts: preventing duplicate persisted outcomes cannot prevent repeated external calls if a process dies after an external call but before committing its result. Mocks do not verify these database claims.

## Milestone 4 — agent coordination (planned)

Introduce log, deployment and dependency investigators returning immutable findings through the existing tool/gateway components. The coordinator owns aggregation and transitions. Begin with independent tasks; only introduce a small explicit DAG when evidence dependencies require it, and reject cycles before execution. Use structured concurrency for cooperative scoped children where it fits, with the selected JDK's preview configuration if required.

Acceptance: consistent reports under concurrent agent failure, deadline and cancellation; one terminal state under races; DAG validation where used; no shared mutable agent-owned report. Preserve limits and distinguish agent child tasks from durable attempts.

## Milestone 5 — portfolio evidence (planned)

Add focused runnable labs, a reproduced failure study and regression verification, JMH for isolated mechanisms, and a bounded application load harness. Include a five-minute walkthrough and fast PR checks plus separate scheduled/manually triggered stress checks. The current README already supplies a Milestone 1 walkthrough.

Acceptance: all examples run with documented commands; deliberately broken labs are labeled; raw results include hardware, JDK, workload and configuration. Report throughput, latency percentiles, queue wait, rejection rate and resource usage where relevant. No invented numbers, unexecuted CI claims or test timings presented as benchmarks.
