# ADR 001: bounded blocking tools and explicit cancellation ownership

Status: accepted for Milestone 1. Revisit when real transports and agent scopes are added.

## Decision

Use one `ThreadPoolExecutor` with equal core/max worker count, an `ArrayBlockingQueue`, `AbortPolicy` and a virtual-thread factory. Each worker performs blocking tool calls. The runtime is shared across investigations; the CLI owns one runtime. An investigation runs on its caller, owns its futures and aggregates immutable outcomes.

This is a deliberately small bounded worker design. Pooling virtual threads is not the normal virtual-thread-per-request pattern. Virtual threads do not themselves make capacity control unnecessary. Here the fixed workers represent downstream capacity, and the JDK executor supplies exact physical queue bounds, removal of cancelled queued work and worker-exit accounting even when a future is cancelled early. Reuse limits the demonstration's coverage of virtual-thread lifecycle semantics and offers little benefit for the three tiny fixture reads. It is documented rather than presented as the universal virtual-thread design.

The alternative virtual-thread-per-task executor plus semaphores would be idiomatic for high fan-out blocking I/O. It would require separate bounds and cancellation-safe ownership for admission, semaphore waiting and actual external execution. Releasing admission in `FutureTask.done()` is incorrect for an uncooperative running call. Rather than introduce a custom admission state machine in Milestone 1, use the existing executor's tested accounting. Provider-specific limits will be separately justified in Milestone 2.

## Why not structured concurrency here?

Java 25 `StructuredTaskScope` is preview and `close()` waits for all children, including ones ignoring interruption. Using it directly around arbitrary blocking tools would make report completion wait indefinitely. A timed `join` does not make `close` time-bounded.

This milestone explicitly demonstrates both cooperative and uncooperative tools and prioritizes bounded report waiting with truthful capacity accounting. Scoped concurrency remains a candidate for later agents whose child tasks have an enforceable cooperative lifecycle. If introduced on Java 25, configure `--enable-preview` for the compiler, Surefire and runtime, and run against that exact JDK. No preview API is currently used.

## Alternatives

- Sequential tools: simplest, but independent blocking calls unnecessarily serialize.
- Unbounded virtual-thread executor: cheap threads do not bound downstream work or waiting callers.
- `CompletableFuture.orTimeout`: a timed-out result does not establish transport termination or resource release; additional ownership is still needed.
- Platform worker threads: same bounds and familiar lifecycle, but this portfolio explicitly explores blocking work on virtual threads. The chosen limits matter more than thread cost in this milestone.
- Generic workflow/resilience framework: unnecessary for three independent sources.

## Consequences

Reports can return while uncooperative work still occupies a slot. A permanently stuck call requires transport-specific abort or process replacement. Runtime shutdown is a cancellation request plus optional bounded observation, not a promise of forced termination. There is no fairness between investigations or downstream isolation beyond this single pool. The coordinator never mistakes a cancelled future for proof that its call has stopped.

## Official references checked

- [Java 25 StructuredTaskScope](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/StructuredTaskScope.html): preview status, interruption and unbounded close waiting.
- [Java 25 ThreadPoolExecutor](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html): bounded queues, worker limits and rejection.
- [Java 25 FutureTask](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/FutureTask.html): cancellation and task completion semantics.
- [JEP 444](https://openjdk.org/jeps/444): normal virtual-thread-per-task usage and the recommendation to use semaphores rather than pool virtual threads for concurrency limits. This ADR records the limited executor trade-off above explicitly.
