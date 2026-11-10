# Milestone 1: parallel investigation

## Acceptance criteria established before implementation

| Observable behavior | Verification |
| --- | --- |
| Independent calls overlap on virtual threads | Three-call barrier and virtual-thread assertion |
| Active calls and waiting work have fixed bounds | Saturated runtime with two active calls, one queued, next rejected |
| Rejection is explicit and does not execute the rejected call | Required rejected source produces `FAILED` / `REJECTED` |
| Optional failures retain useful evidence | Successful source plus failed optional source produces `PARTIAL` |
| Required failures cancel siblings | Latch-controlled failure and resource-finally latch |
| Investigation and per-tool budgets expire | Blocked source, typed timeout and retained fast result |
| Cancellation reclaims queued work and cooperatively closes resources | Queue-removal and interruption tests |
| Uncooperative work cannot invalidate bounds or alter a report | Stubborn tool retains slot, rejects excess work, eventually releases capacity |
| Completion and cancellation have one terminal outcome | 100 latch-released races and one-shot enforcement |
| Owner interruption is preserved | Explicit owner interruption, cancelled report and child cleanup |
| Shutdown is bounded and new work is rejected | Cooperative termination and uncooperative bounded termination check |

## Components and execution

```text
CLI / calling thread
  ├── synthetic EvidenceTool values (name, required flag, budget, blocking call)
  ├── Investigation (one-shot coordinator and immutable report publication)
  └── ToolRuntime (shared bounded executor)
        ├── virtual blocking workers: at most activeLimit
        └── ArrayBlockingQueue: at most waitingLimit tasks
```

`ReleaseInvestigator` loads immutable fixture text using resource streams closed by the tool. It constructs three tools and owns the runtime lifecycle. This describes the Milestone 1 evidence path. Milestone 2 adds optional fake-provider synthesis after evidence report publication; no scheduler, agent framework or HTTP service is involved.

`Investigation.run()` executes on its caller's thread. It submits at most 16 tool futures without blocking admission. Each accepted task posts one completion notification, and an accepted cancellation posts one notification. The event queue has capacity `tool count + 1`. Notifications only wake the coordinator; futures contain results. Rejected tasks have no completion event. Even cancelled futures post at most one event, so cancellation cannot overflow this queue.

The coordinator scans pending futures on each wakeup and polls only until the nearest tool or overall deadline. Scanning at most 16 entries is simpler than adding a timer thread per source. Tool deadlines begin at submission, so queue waiting consumes the budget. All elapsed-time comparisons use `System.nanoTime` differences; budgets are constrained to one day to avoid duration conversion and comparison overflow.

Only the coordinator modifies pending tasks, deadlines and accepted outcomes. These collections are thread-confined. Workers publish immutable results through `FutureTask`; they never modify reports. Completion timestamps reject evidence completed after a per-tool deadline. Overall deadline observation stops acceptance, even for results completed earlier but not yet observed; this makes the rule explicit rather than pretending the JVM provides hard real-time scheduling.

## State ownership and policy

The lifecycle is `NEW → RUNNING → terminal`; cancelling `NEW` records a request, and the eventual `run()` produces a cancelled report without starting tools. Repeated `run()` calls fail. Cancellation and final report publication synchronize on the investigation object. Network/tool calls never run while this lock is held.

| Condition | State / behavior |
| --- | --- |
| Cancellation accepted before publication | `CANCELLED`, regardless of concurrent completion |
| Caller interrupted | Cancel children, return `CANCELLED`, restore caller interrupt flag |
| Overall deadline observed | `TIMED_OUT`; unresolved sources get timeout outcomes |
| Required source has any non-success outcome | `FAILED`; cancel pending siblings |
| All sources observed successfully | `COMPLETED` |
| Required sources successful, optional source unsuccessful | `PARTIAL` |

If cancellation races final assembly, already accepted successes remain in the report and the state becomes `CANCELLED`. If a deadline and source failure compete, an overall deadline observed at the beginning of the coordinator iteration wins. A failure already observed in an earlier iteration can win first. These are permitted observable policies, not claims about the physical order of remote events.

At every terminal path the coordinator cancels unresolved futures. A source not accepted before termination is explicitly represented; the report contains every configured tool in declaration order. Only exception class names are reported, avoiding accidental disclosure through exception messages.

## Resource lifetime

A queued future is both cancelled and physically removed. A running future receives interruption, but its executor worker stays occupied until `FutureTask.run()` returns. `FutureTask.cancel(true)` can complete the future immediately while its callable continues; the executor's worker accounting is the resource invariant, not the future state.

`ToolRuntime.close()` interrupts workers, cancels drained queued futures and returns without joining arbitrary uncooperative code. `awaitTermination` is separately bounded. Tools must define their own transport aborts and resource ownership when real I/O is added. Virtual worker threads are daemon threads; JVM termination can abandon uncooperative work and does not guarantee its finally blocks ran.

An investigation deadline bounds willingness to await evidence. JVM scheduling, report assembly and cancellation bookkeeping can add latency. It does not forcibly terminate external work or provide a hard wall-clock upper bound.
