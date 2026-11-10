# Milestone 2 verification record

Date: 2026-11-10. Functional verification only; no benchmark results or performance claims.

## Implemented change

Added the deterministic fake LLM provider, synchronous provider-specific gateway, immutable configuration/results, optional CLI synthesis and latch-controlled CLI overload scenario. The gateway supplies bounded admission/waiting/in-flight calls, fixed-window request/token quotas, reconciliation, retries and a generation-protected circuit breaker. No dependency version, preview requirement or Maven module changed. Milestones 3–5 and the optional official Anthropic adapter remain unimplemented.

## Environment

Same local environment as [Milestone 1](verification.md): macOS 27.0.1 arm64, Homebrew OpenJDK 27 (build 27), Maven Wrapper selecting Maven 3.10.0, and compiler `--release 25`. Default system Java remains 24.0.2; the build explicitly selects JDK 27. The writable copy of the existing Maven cache at `/tmp/release-m2/repository` supports offline verification. No API credentials, database, SDK or network provider was used.

Exact JDK 25 remains unavailable locally. These results establish compilation against Java 25's stable API and execution on JDK 27, **not exact JDK 25 runtime verification**. No new JDK download succeeded or is claimed in this milestone. Official Java 25 concurrency API documentation was checked; existing pinned dependencies remain unchanged.

## Executed checks

From the repository root:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home \
  ./mvnw -o -Dmaven.repo.local=/tmp/release-m2/repository verify
```

Result: **BUILD SUCCESS; 47 tests; 0 failures, 0 errors, 0 skipped.**

| Suite | Executed tests | Coverage |
| --- | ---: | --- |
| `InvestigationTest` | 14 | Existing Milestone 1 behavior, including 100 completion/cancellation race iterations |
| `LlmGatewayTest` | 23 | Fake synthesis, capacity bounds, quotas, token reconciliation/debt, retries/jitter/budgets, deadline expiry, cancellation, uncooperative providers, validation and spurious wakeups |
| `GatewayCircuitTest` | 10 | Threshold/rejection, retry short-circuit, one half-open probe, failed/interrupted probes, stale completions, old-window usage and provider isolation |

Serial policy tests advance injected monotonic time and provide deterministic jitter. Concurrent tests use caller-owned virtual threads, controlled providers, latches and bounded waits. No correctness test uses `Thread.sleep`. This is regression evidence, not exhaustive exploration of every interleaving. Larger stress runs remain planned.

The packaged JAR was run with `/opt/homebrew/opt/openjdk/bin/java` for all five scenarios:

```sh
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar overload
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar healthy
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar partial
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar timeout 200
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar required-failure
```

Observed overload output:

```text
Gateway limits: in-flight=1, waiting=1
Saturated: in-flight=1, waiting=1
Third request: OVERLOADED, attempts=0
First request: SUCCESS
Second request: SUCCESS
After completion: admitted=0, in-flight=0, waiting=0
```

Healthy and partial evidence reports respectively remained `COMPLETED` and `PARTIAL`, each followed by `LLM synthesis [deterministic fake]: SUCCESS, attempts=1`. The 200ms timeout remained `TIMED_OUT`; required-source failure remained `FAILED`; both skipped synthesis. Already accepted evidence remained visible. Local Markdown links were checked to resolve to existing files.

## Important limitations

- Exact JDK 25 and Windows execution: not run. Repeat the build and scenarios on JDK 25 before claiming exact-runtime compatibility.
- Optional Anthropic adapter: deferred; no SDK retry configuration or real-provider behavior has been verified. Gateway providers must perform exactly one attempt and honor transport cancellation/deadlines.
- A synchronous provider that ignores interruption can exceed the request deadline. The gateway retains its actual in-flight capacity until the call exits and discards a response when it observes expiry. It does not force-stop arbitrary external code.
- Quotas are local fixed windows and estimates, permit boundary bursts and do not model distributed provider billing. Usage from an expired dispatch window does not modify the current ledger.
- Retries are bounded but can repeat external/billed effects. There is no exactly-once promise, retry idempotency mechanism, streaming support or `Retry-After` handling.
- No scheduled stress workflow, CI execution, JMH, load benchmark or PostgreSQL test was run. Those belong to later milestones.

## Interview questions

1. Why are total admission, waiting capacity and in-flight capacity separate bounds? How does quota exhaustion expose a mistake in using only an active-plus-queue semaphore?
2. How do token reservations behave when usage is lower, higher, unknown or reported after the original quota window expires?
3. Why must circuit completions carry a generation, and how do stale successes, stale failures and interrupted probes affect recovery?
