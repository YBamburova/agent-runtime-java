# Milestone 2: gateway and overload

## Acceptance criteria (before implementation)

1. Default synthesis runs without credentials through a deterministic fake provider.
2. Provider calls never exceed `maxInFlight`; waiting requests never exceed `maxWaiting`; total retained callers are also bounded before acquiring the policy lock. Saturation rejects explicitly.
3. Each dispatched attempt consumes one request and reserves estimated total tokens in a fixed quota window. Known usage reconciles reservations; unknown usage stays charged; underestimated usage blocks subsequent admission until the debt clears.
4. Waiting, retry backoff and every new attempt respect a single caller deadline. Cancellation removes waiting work and releases logical admission; dispatched calls retain in-flight capacity until their bodies exit.
5. Only explicitly transient provider failures retry. Every retry reserves quota again and counts toward a maximum attempt budget. Backoff grows exponentially with injected full jitter and a cap. The gateway is the only retry owner.
6. Consecutive transient failures open a circuit. Open circuits fail fast; expiry admits at most one recovery probe. Stale completions cannot close a newer circuit generation. Interrupted probes cannot leave recovery stuck.
7. Separate gateway instances share no provider capacity, quotas or breaker state. A blocked/failed provider does not consume another provider's limits.
8. Tests use controllable providers, latches and injected monotonic time/jitter for policy assertions, with bounded waits. A reproducible CLI overload scenario exposes saturation and recovery.

## Intended scope

One small provider-specific gateway using JDK concurrency primitives; no generic resilience framework, provider registry, HTTP service or SDK dependency. Add the optional official Anthropic adapter only in a later explicit change, after verifying that SDK's current APIs and disabling its retries. Fake synthesis remains optional to evidence correctness and uses the investigation's remaining deadline.

## Implemented components

```text
CLI evidence investigation → immutable Report → optional fake synthesis
                                                │ remaining overall budget
                                                ▼
                                  provider-specific LlmGateway
                                    immediate logical admission
                                    policy lock + condition
                                    quota / in-flight / breaker checks
                                                │ no policy lock held
                                                ▼
                                  one blocking LlmProvider attempt
                                                │
                                  reconcile usage / release capacity
                                  return, or bounded retry backoff
```

- `LlmProvider`: one blocking attempt; immutable bounded prompt/response values, optional total-token usage, and explicit transient/permanent failures. Providers own transports and must close their resources. No internal retries are permitted.
- `FakeLlmProvider`: deterministic fixture synthesis, reports 64 synthetic usage tokens and never calls the network. It checks for successful required-source markers and suggests validation; it is not a tokenizer or model-quality simulation.
- `GatewayConfig`: immutable limits; `defaults()` supplies demo settings. All durations are positive and at most one day, attempt count is 1–10, and capacity arithmetic is validated.
- `LlmGateway`: admission, policy state and retry ownership. It creates no threads, executors or timers and needs no close method. Callers own execution and cancellation. Reuse one instance per provider/quota scope; creating one per request defeats shared limits.
- `GatewayDemo`: latch-controlled overload, executed by `java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar overload`.

The fake synthesis runs only for completed/partial investigations with time left. It cannot mutate the evidence report or turn failed evidence into success. Its deadline begins with the remaining CLI budget rather than a fresh 20 seconds. The fake is immediate; blocking-provider callers in tests and the overload demo use virtual threads. An eventual network adapter must define transport deadlines and cancellation before being connected to the CLI.

## State and resource ownership

A logical request follows `try admission → wait/dispatch → retry or terminal → release admission`. A semaphore permits at most `maxInFlight + maxWaiting` admitted calls, including those briefly contending for the policy lock. Saturated requests fail immediately. A lock protects in-flight count, waiting count, request/token ledger and circuit state. Provider I/O is never under this lock.

Every attempt checks capacity, quota, retry readiness and circuit state atomically under that lock. A blocked request increments the waiting counter at most once per acquisition and always decrements it in `finally`. `Condition.awaitNanos` releases the lock while waiting; signals and spurious wakeups recheck all predicates in a loop. The injected waiter used by serial policy tests advances virtual time; production always uses an actual condition wait.

Waiting slots cover quota delays, in-flight contention and retry backoff. They are not reserved for an entire logical request. A retry can return `OVERLOADED` after one or more attempts if waiting capacity is full; retries have no privilege over fresh work. With `maxWaiting=0`, calls that cannot dispatch immediately, including delayed retries, are rejected. Logical admission remains held throughout backoff, bounding the total retained request state.

Provider return, checked failure, unchecked failure and interruption all pass through an attempt `finally` that releases in-flight capacity. A future's cancellation is not used to release this capacity. Providers that ignore interruption keep their slot until they actually return. If they swallow the interrupt flag, the gateway cannot reconstruct that lost signal; an enclosing cancelled future still discards their result. Caller interruption is preserved when the gateway observes it.

`Snapshot` is diagnostic: policy fields are read together under the lock, while admission comes from the semaphore. During handoff, admitted need not equal in-flight plus waiting; it always stays within its configured bound. No prompt or provider exception message is logged or placed in a failure result.

## Quota policy

A gateway starts one fixed-duration local quota window at construction. On the first observation after expiry, it resets the ledger and starts the next window at that observed monotonic time. Request quota is charged per dispatched attempt, including retries. Token estimates must cover input plus anticipated output and must fit the configured window; impossible reservations return `INVALID_REQUEST` without waiting.

The attempt reserves estimated tokens before dispatch. A reservation abandoned before dispatch refunds both request and token charge in its original window. Once dispatch begins, request charge is never refunded. A response with usage replaces its estimate with actual usage in the same window; absent usage and failed attempts keep their estimate. Underestimates can exceed the configured token limit: subsequent requests then wait for reset. This is admission accounting, not a promise that actual provider usage can never exceed an estimate.

A reservation includes a quota epoch. When a response belongs to an older window, no current-window reconciliation is performed: it cannot subtract an old estimate from new requests. Historical charges are not persisted and delayed usage is not carried into a new window. Aligning quotas to an actual provider's billing/rate windows is deferred to the adapter. Fixed windows permit bursts across boundaries and are not a smooth sliding-window limit.

## Retry and deadline policy

Only `FailureKind.TRANSIENT` retries. Permanent typed failures, unchecked provider contract/programming failures and null responses return `PROVIDER_FAILED` without retry. Failures report only a typed status; arbitrary exception messages are not surfaced. Cancellation propagates `InterruptedException` with the flag restored.

After failed attempt `n`, the backoff cap is `min(maxBackoff, initialBackoff × 2^(n−1))`; full jitter chooses a delay in that range with a one-nanosecond minimum. The retry holds no provider in-flight slot. It must acquire a bounded waiting slot, wait until its not-before time, then reserve fresh request/token quota. Every loop checks the original absolute monotonic deadline. A delay that cannot fit returns `DEADLINE_EXCEEDED` immediately; it does not sleep until the deadline just to fail. A new attempt receives only the remaining budget.

Late responses return `DEADLINE_EXCEEDED` with no synthesis text. Known usage still reconciles if its dispatch window is current. A deadline does not preempt arbitrary synchronous provider code; it bounds admission/backoff and discards late responses after control returns. The provider must enforce its own transport timeout and cancellation behavior. This is the same external-work distinction demonstrated in Milestone 1.

## Circuit state

```text
CLOSED -- transient threshold --> OPEN
OPEN -- cooldown + actual dispatch capacity/quota --> HALF_OPEN
HALF_OPEN -- successful response --> CLOSED
HALF_OPEN -- failed/interrupted/abandoned probe --> OPEN
```

An open circuit fails fast until its cooldown elapses. It remains visibly `OPEN` after expiry until an eligible request actually reserves capacity and quota as the single probe. During `HALF_OPEN`, additional requests fail fast. The probe obeys normal quotas and in-flight bounds; recovery has no hidden bypass.

Successful current-generation calls reset the transient-failure count. Permanent failures in `CLOSED` are neutral: they do not open the circuit or reset the count. A failed permanent recovery probe reopens it conservatively. Cancellation of a normal call does not count as provider failure. A dispatched interrupted probe reopens with a fresh cooldown; an undispatched probe restores `OPEN` without delaying its next eligible probe.

Circuit transitions advance a generation. Each attempt records its generation at dispatch. Only matching completions can update breaker state; stale success cannot close a newer open circuit, and stale failure cannot reopen a recovered one. A successful response arriving after its request deadline may still demonstrate provider availability, while its synthesis text is discarded; observed interruption prevents probe success from closing the circuit.

## Verification mapping

| Acceptance area | Tests |
| --- | --- |
| Fake output and incomplete evidence | `fakeProviderIsDeterministicAndDoesNotInventMissingEvidence` |
| Active, waiting and total bounds; overload | Saturated virtual-thread calls; quota-only saturation; zero-wait configuration |
| Request and token quota | Fixed-window dispatch timestamps, refund, unknown usage, token debt |
| Old-window accounting race | `lateUsageCannotRefundTheNextQuotaWindow` |
| Retry ownership, budgets and jitter | Exact injected delays, capped attempts, transient-only failures, fresh retry quota |
| Deadline and cancellation | Waiting expiry, late response, backoff interruption, provider cleanup, uncooperative capacity retention |
| Predicate-loop correctness | Simulated spurious wakeup cannot bypass quota or deadline |
| Breaker recovery and races | Threshold, single probe, failed/interrupted probes, stale success and stale failure |
| Provider isolation | Blocked/open gateway alongside independently successful gateway |

See [ADR 002](../decisions/002-provider-gateway.md) and the [executed verification record](verification-milestone-2.md). Broader stress testing and measured performance remain Milestone 5 work.
