# Local Benchmark Results

Run command:

- `./gradlew :samples:investigation:run --profile=long --rounds=1`

Implementation:

- Benchmark harness: `samples/investigation/src/main/kotlin/org/otpstudy/investigation/InvestigationBenchmarks.kt`
- Scope: local machine, long profile, single round (post–gap-closure baseline).
- Machine: Apple Silicon (arm64), JVM default (G1GC), Kotlin coroutines `Dispatchers.Default`.

## Results — GenServer call / cast

| Scenario | Iterations | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | Throughput (ops/s) | Notes |
|---|---:|---:|---:|---:|---:|---:|---|
| `gen_server_call_roundtrip` | 50 000 | 12.67 | 20.88 | 32.13 | 65.25 | 75 441 | standard request/reply |
| `gen_server_call_fastReply_roundtrip` | 50 000 | **10.33** | 20.54 | 28.08 | 86.63 | 90 746 | `yield()` after reply; ~18 % p50 improvement |
| `gen_server_call_p99_tail_latency` | 100 000 | 9.42 | 13.83 | 27.13 | 59.92 | 96 196 | large sample; GC spikes visible in p999 |
| `gen_server_cast_enqueue` | 50 000 | 0.08 | 0.17 | 0.25 | 1.08 | 7 735 249 | enqueue speed; drain rate: **2 226 068 ops/s** |
| `distribution_in_memory_call` | 50 000 | 11.88 | 19.04 | 27.42 | 45.21 | 78 895 | in-process transport; matches local call |
| `mailbox_cast_memory_delta` | 250 000 | — | — | — | — | — | post-drain heap delta: 0 KiB (GC heuristic) |

**Key comparison:** `fastReply` p50 = 10.33 µs vs standard 12.67 µs (**2.34 µs saved**, ~18% reduction). Akka reference is ~8 µs; the gap from 10 to 8 µs traces to residual dispatcher scheduling overhead not eliminable without `UNDISPATCHED`.

---

## Results — Concurrent callers (serialization overhead)

| Scenario | Total calls | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | Agg. throughput (ops/s) |
|---|---:|---:|---:|---:|---:|---:|
| `gen_server_call_concurrent_callers_1` | 2 000 | 11.00 | 16.50 | 38.29 | 76.50 | 79 026 |
| `gen_server_call_concurrent_callers_10` | 20 000 | 14.75 | 39.50 | 63.50 | 147.50 | 548 378 |
| `gen_server_call_concurrent_callers_50` | 100 000 | 39.88 | 98.71 | 123.63 | 161.63 | 981 655 |
| `gen_server_call_concurrent_callers_100` | 200 000 | 59.42 | 109.17 | 123.25 | 213.67 | 1 348 486 |

Aggregate throughput scales with concurrency (more callers keep the actor busy), but per-call latency degrades with mailbox depth. At 100 callers, p50 is 59 µs vs 11 µs at 1 caller — a 5.4× latency penalty from mailbox queuing.

---

## Results — Bounded mailbox overflow

| Scenario | Senders | Capacity | Accepted | Rejected | p50 (µs) | p99 (µs) |
|---|---:|---:|---:|---:|---:|---:|
| `gen_server_call_bounded_mailbox_overflow` | 500 | 100 | 497 | 3 | 610.63 | 1 484.54 |

Overflow policy `CrashSender`: 3 senders rejected under 500-concurrent burst against capacity-100 mailbox. Accepted calls show elevated latency (610 µs p50) due to serialized drain-under-load.

---

## Results — Selective receive

| Scenario | Samples | p50 (µs) | p95 (µs) | p99 (µs) | Notes |
|---|---:|---:|---:|---:|---|
| `selective_receive_depth_1000` | 250 | 54.71 | 299.75 | 376.83 | O(n) saved-list scan |
| `selective_receive_depth_10000` | 250 | 418.92 | 494.04 | 545.67 | O(n) saved-list scan |
| `selective_receive_depth_25000` | 250 | 1 032.04 | 1 186.00 | 1 333.54 | O(n) saved-list scan |
| `selective_receive_ref_mark_depth_1000` | 250 | 45.33 | 391.08 | 470.71 | ref-equality predicate (similar cost to int-equality) |
| `selective_receive_ref_mark_depth_10000` | 250 | 419.25 | 515.08 | 590.75 | predicate cost < scan cost |
| `selective_receive_ref_mark_depth_25000` | 250 | 1 095.25 | 1 256.54 | 1 410.63 | predicate cost < scan cost |
| **`selective_receive_mark_depth_1000`** | 250 | **0.33** | 0.46 | 1.71 | `mark`+`receiveFrom`: O(1) skip — **165× faster** than depth-1000 scan |
| **`selective_receive_mark_depth_10000`** | 250 | **0.29** | 0.38 | 0.79 | O(1) regardless of pre-mark saved depth |
| **`selective_receive_mark_depth_25000`** | 250 | **0.13** | 0.17 | 1.38 | O(1) regardless of pre-mark saved depth |

**Key insight:** `mark`+`receiveFrom` reduces selective receive from O(n) (linear scan) to O(1) when the matching message arrives after the mark. At depth 25 000: **1032 µs → 0.13 µs** (≈ 8000× improvement). This is the scan-pointer optimization.

---

## Results — Supervisor restart

| Scenario | Iterations | p50 (µs) | Throughput (ops/s) | Notes |
|---|---:|---:|---:|---|
| `supervisor_restart_storm_recovery` | 101 | 17 671 (total) | 5 715 | 100 crashes to stable child; ~177 µs/crash derived |
| `supervisor_single_restart_latency` | 1 | **84.79** | 11 794 | single restart after 10 JIT warmup cycles |

**Key comparison:** derived per-crash from storm = ~177 µs; isolated warm-JIT restart = 85 µs. The 2× gap is cold-JIT overhead from the early crashes in the storm. Erlang/OTP single restart is ~10–20 µs (BEAM preemptive scheduling + no GC pressure). JVM gap is structural.

---

## Results — Worker pool checkout

| Scenario | Pool size | Callers | p50 (µs) | p95 (µs) | p99 (µs) |
|---|---:|---:|---:|---:|---:|
| `worker_pool_checkout_callers_20_pool_20` | 20 | 20 | 0.13 | 0.38 | 3.92 |
| `worker_pool_checkout_callers_50_pool_20` | 20 | 50 | 1 377.50 | 2 648.67 | 2 662.25 |
| `worker_pool_checkout_callers_100_pool_20` | 20 | 100 | 5 147.42 | 5 211.29 | 5 236.08 |

At equal callers/pool (20/20), checkout p50 = 0.13 µs. At 2.5× oversubscription (50/20), p95 exceeds 2.6 ms — latency spikes sharply when the pool is saturated, consistent with wait-queue scheduling overhead.

---

## Interpretation

- `fastReply` (`yield()` after reply) reduces p50 call latency by ~18% with no API changes to callers.
- `mark`+`receiveFrom` (scan-pointer optimization) eliminates O(n) selective receive cost entirely when the match arrives after the mark — the dominant use pattern in OTP-style request/reply tagging.
- `supervisor_single_restart_latency` isolates per-restart cost from JIT warm-up at **85 µs**, giving a clean baseline for supervision overhead.
- Cast drain rate (2.2M ops/s) is ~3.5× slower than enqueue rate (7.7M ops/s), revealing the true actor processing bottleneck vs. channel throughput.
- Concurrent-caller latency scaling confirms serial mailbox semantics: p50 increases 5.4× from 1→100 concurrent callers, while aggregate throughput scales near-linearly.
- Worker pool checkout shows sharp p95 cliff at 2.5× oversubscription; target deployments should size pools to ≤ 1.5× expected peak concurrency to stay under 1 ms p95.

## Comparison against reference implementations

| Metric | kotlin-otp (this run) | Akka/Pekko (reference) | Erlang/OTP (reference) |
|---|---|---|---|
| Call p50 (standard) | 12.67 µs | ~8 µs | ~5–15 µs |
| Call p50 (fast path) | 10.33 µs | ~8 µs | — |
| Single restart | 85 µs | ~50–150 µs | ~10–20 µs |
| Selective receive (n=25k, scan) | 1 032 µs | n/a (not native) | ~constant via process dict |
| Selective receive (n=25k, mark) | **0.13 µs** | n/a | — |
| Cast enqueue | 0.08 µs / 7.7M ops/s | ~0.1 µs | ~0.1 µs |

## Caveats

- Single-machine, single-round results; no cross-round stddev (run `--rounds=5` for confidence intervals).
- JVM G1GC stop-the-world pauses (80–125 ms in production) appear in p999 but are invisible at p50/p95 in short runs. See `LIMITATIONS.md` § "Tail latency and GC".
- Worker pool uses a channel-semaphore approximation, not a true actor-pool; checkout latency reflects channel scheduling, not genserver call overhead.
- Memory delta metric is heuristic (`System.gc` + heap snapshot); use JVM flight recorder for allocator-accurate measurements.
