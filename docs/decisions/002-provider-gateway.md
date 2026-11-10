# ADR 002: one explicit policy owner per LLM provider

Status: accepted for Milestone 2.

## Decision and scope

Use a synchronous provider boundary and one provider-specific gateway that owns admission, quota accounting, retries and circuit transitions. The gateway owns no threads; blocking callers use virtual threads. Keep its custom policy implementation inside `dev.release.llm` and use JDK semaphores, a lock and a condition for synchronization. No runtime dependency or generic resilience framework is introduced.

The purpose is to make the interactions explainable and testable: a retry is another quota-consuming attempt, a cancelled future is not proof of stopped I/O, and stale completions must not modify new quota or circuit generations. The gateway has one immutable configuration and a fixed set of policies; it does not expose decorators, plugin registries or arbitrary workflows.

## Why two capacity bounds?

An immediate semaphore bounds all retained logical requests before they contend for the policy lock. A separate counter bounds provider I/O. Waiting has its own bound because exhausted quota and retry delay can leave *all* admitted work waiting, even when provider I/O is zero. Treating `maxInFlight + maxWaiting` as a sufficient queue bound would allow too much waiting during quota exhaustion.

The lock handles only short state transitions. Condition waits release it; network/provider calls and retry delays never hold it continuously. Backoff consumes a waiting slot and logical admission, but no provider slot. Under saturation, a retry can reject rather than adding an unbounded hidden retry queue.

## Why a local quota ledger and generation checks?

Fixed windows and total-token estimates are sufficient to demonstrate reservation, refunds and debt without writing a tokenizer or distributed rate limiter. Each retry charges independently. Unknown usage stays reserved conservatively. Usage from an expired dispatch window cannot refund the current window; epochs enforce that rule. This policy does not model every provider's exact billing or rate semantics.

A single half-open recovery probe is easy to bound. Generation checks prevent older in-flight successes and failures from overriding a newer breaker transition. Interrupted probes reopen rather than leaving the circuit permanently half-open.

## Alternatives and trade-offs

- Reusing the evidence worker pool would let slow providers consume unrelated evidence capacity. Provider-specific gateways are independent; callers must still avoid introducing a shared exhausted executor or transport underneath them.
- An asynchronous gateway would require ownership of scheduled work, cancellation handles and background executor shutdown. The synchronous boundary makes actual attempt lifetime match the calling thread's resource lifetime. It cannot forcibly return from uncooperative provider code; transport-enforced deadlines remain the provider's responsibility.
- A general resilience library could supply standard retry/breaker machinery. Here a narrow implementation makes token reservation, bounded waiting and stale-result behavior visible in one place. This is educational custom policy, not a production infrastructure replacement. A future production integration should reevaluate established components against the documented invariants rather than broadening this into a general framework.
- Continuous token buckets or sliding windows smooth bursts better but complicate estimated-token reconciliation. The fixed-window limitation is explicit and covered by deterministic tests.
- SDK retries plus gateway retries can multiply billed requests. There is currently no SDK; any future official Anthropic adapter must disable its internal retries and classify only eligible failures for the gateway.

## Deterministic verification

Monotonic time, waiting and jitter are package-visible constructor dependencies. Serial tests advance a fake clock rather than sleeping. Concurrency tests use the real condition protocol with latches and bounded waits. The production constructor always uses `System.nanoTime`, `Condition.awaitNanos` and `ThreadLocalRandom`. There is no configurable provider prompt logging.

Official Java 25 APIs were checked: [Semaphore](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/Semaphore.html), [ReentrantLock](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/locks/ReentrantLock.html), [Condition](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/locks/Condition.html), [Thread](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/Thread.html) and [Executors](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/Executors.html). Stable APIs only; no preview flags or dependency version changes are needed.
