# jobs port (Phase 2 alpha test)

Kotlin port of [uwiger/jobs](https://github.com/uwiger/jobs) — Ulf Wiger's load regulator (rate + concurrency admission control) — built on kotlin-otp. Complements [poolboy](../poolboy/README.md): poolboy bounds **workers**; jobs bounds **work admitted**.

Erlang sources mirrored (v1 subset):

- [`jobs.erl`](https://github.com/uwiger/jobs/blob/master/src/jobs.erl) — public API
- [`jobs_server.erl`](https://github.com/uwiger/jobs/blob/master/src/jobs_server.erl) — coordinator `gen_server`
- [`jobs_queue.erl`](https://github.com/uwiger/jobs/blob/master/src/jobs_queue.erl) — fifo/lifo queue behaviour (in-memory here)

## Run it

```
./gradlew :samples:jobs:test
./gradlew :samples:jobs:run
```

## File layout

| File | Mirror of | Purpose |
|------|-----------|---------|
| [`Jobs.kt`](src/main/kotlin/org/otpstudy/jobs/Jobs.kt) | `jobs.erl` | `startLink`, `ask`, `done`, `run`, queue admin |
| [`JobsRef.kt`](src/main/kotlin/org/otpstudy/jobs/JobsRef.kt) | — | Typed handle around `GenServerRef` |
| [`JobsServer.kt`](src/main/kotlin/org/otpstudy/jobs/JobsServer.kt) | `jobs_server.erl` | Coordinator actor, regulators, timers |
| [`JobsQueueStorage.kt`](src/main/kotlin/org/otpstudy/jobs/JobsQueueStorage.kt) | `jobs_queue.erl` | Fifo/lifo/passive storage |
| [`JobsTypes.kt`](src/main/kotlin/org/otpstudy/jobs/JobsTypes.kt) | `jobs.hrl` | Options, regulators, `RegObj` |
| [`JobsTest.kt`](src/test/kotlin/org/otpstudy/jobs/JobsTest.kt) | `jobs_server_tests.erl` (subset) | 11-test matrix |

## Erlang → Kotlin mapping

| Erlang / OTP | Kotlin / kotlin-otp | Notes |
|---|---|---|
| `jobs_server` `gen_server` | [`JobsServer`](src/main/kotlin/org/otpstudy/jobs/JobsServer.kt) + [`GenServers.startLink`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt) | Single coordinator per node (v1) |
| `gen_server:reply` on backlog | [`ReplyResult.DeferReply`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt) + immediate `Reply` when slot available in same turn | Mirrors OTP `{noreply}` + later `approve/2` |
| `erlang:monitor(process, Pid)` on grant | [`ReplyHandle.callerJob`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt) + `invokeOnCompletion` → [`JobsInfo.BorrowerDown`](src/main/kotlin/org/otpstudy/jobs/JobsMessages.kt) | Restores counters when borrower dies without `done` |
| `{rate, [{limit, R}]}` | [`RegulatorSpec.Rate`](src/main/kotlin/org/otpstudy/jobs/JobsTypes.kt) | Interval ≈ `1000/R` ms between dispatches |
| `{counter, [{limit, C}]}` | [`RegulatorSpec.Counter`](src/main/kotlin/org/otpstudy/jobs/JobsTypes.kt) | `done/1` restores counter via cast |
| `{max_time, T}` | `QueueOptions.maxTime` | Waiters older than `T` get `{error, timeout}` |
| `{max_size, S}` | `QueueOptions.maxSize` | Rejects new `ask` when queue length ≥ `S` |
| `approve` / `reject` action queues | [`QueueType.Action`](src/main/kotlin/org/otpstudy/jobs/JobsTypes.kt) | Pass-through or hard reject |
| `passive` queue | [`QueueType.Passive`](src/main/kotlin/org/otpstudy/jobs/JobsTypes.kt) | `enqueue` / `dequeue` |
| `{link, Pid}` on queue | `QueueOptions.linkOwner` | [`Job.invokeOnCompletion`](src/main/kotlin/org/otpstudy/jobs/JobsServer.kt) → delete queue |
| `erlang:send_after` check | [`OtpTimers.sendAfter`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/OtpTimers.kt) → [`JobsInfo.CheckQueue`](src/main/kotlin/org/otpstudy/jobs/JobsMessages.kt) | Timers cancelled when queue idle |
| CPU/memory modifiers / distributed sampler | — | **Out of scope v1** (see plan) |

## Test matrix

| # | Test | Asserts |
|---|------|---------|
| 1 | `approve queue grants immediately` | Action queue returns `RegObj` without blocking |
| 2 | `reject queue returns rejected` | `JobsError.Rejected` |
| 3 | `counter limits concurrent grants` | Third `ask` blocks until first two `done` |
| 4 | `fifo passive dequeue order` | `enqueue` a,b → `dequeue` returns a,b |
| 5 | `lifo queue type` | Two concurrent jobs complete (ordering smoke) |
| 6 | `max_size rejects ask when full` | Queue length cap → `Rejected` |
| 7 | `max_time times out blocked ask` | Counter 0 + `max_time` → `Timeout` |
| 8 | `run executes and releases counter` | `done` restores counter value |
| 9 | `add and delete queue` | Dynamic `add_queue` / `delete_queue` |
| 10 | `rate queue serializes bursts` | 5 jobs at 5/s complete in ~1s |
| 11 | `linked queue removed when owner job completes` | `{link, Pid}` analogue via `linkOwner` |

## v1 non-goals

- Distributed sampler / cross-node CPU-memory modifiers
- `group_rate`, named counters, producers, `jobs_stateful` callbacks
- Replacing Resilience4j — this is an OTP-semantics conformance sample

## Follow-up samples (plan)

1. **[fuse](https://github.com/jlouis/fuse)** — circuit breaker + [`OtpTable`](../../otp-ets/src/main/kotlin/org/otpstudy/ets/OtpTable.kt) fast path
2. **[ranch](https://github.com/ninenines/ranch)** (scoped) — [`gen_statem`](../../otp-gen-statem) + TCP acceptor pool
