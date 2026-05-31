# Latency Tuning Guide

**Audience:** operators and developers deploying kotlin-otp actors in production or performance-sensitive contexts.

All latency figures are from `docs/investigation/competitor-benchmark-results.md` and `docs/investigation/local-benchmark-results.md`, measured on Apple Silicon (arm64), JVM OpenJDK 25 (G1GC), long profile, steady-state rounds only (rounds 2–3; cold JIT round 1 excluded).

---

## Decision tree

```
What latency target do you need per call?

> 10 µs    → Default configuration is fine. fastReply recommended.
3–10 µs   → Ensure caller and actor share the same dispatcher pool.
1–3 µs    → Use Dispatchers.Unconfined for dedicated hot actors (read constraints).
< 1 µs    → Not achievable without replacing the JVM scheduler. Use OTP.

Blocking worker (JDBC, HTTP, file I/O)?
           → Use LongTaskBoundary — defaults to OtpDispatchers.IO (Loom).
             All N tasks complete in ~1 blocking-duration regardless of N.
```

---

## Blocking worker profile — OtpDispatchers.IO (default for LongTaskBoundary)

For actors that perform blocking I/O (JDBC, HTTP clients, file operations), use `LongTaskBoundary`. Its `workerContext` now defaults to `OtpDispatchers.IO` — a shared Loom virtual-thread dispatcher.

```kotlin
val boundary = LongTaskBoundary(
    maxConcurrent = 100,   // 100 virtual threads — cheap; 100 platform threads would be expensive
    policy = LongTaskBackpressurePolicy.BoundedWait,
    queueTimeout = 2.seconds,
    taskTimeout = 30.seconds,
)

// In handleCallFrom — defer reply, run blocking work, reply when done
override suspend fun handleCallFrom(request: Any, state: S, from: ReplyHandle<S>): ReplyResult<S> {
    scope.launch {
        val result = boundary.run { jdbcCall(request) }
        from.reply(result)
    }
    return ReplyResult.DeferReply(from, state)
}
```

**Why Loom by default:** virtual threads unmount from their carrier when blocking, so all N concurrent tasks run in a single wave regardless of N. At equal `maxConcurrent`, Loom and `Dispatchers.IO` have the same wall time. Loom's advantage is that `maxConcurrent = 100` costs ~few KB per virtual thread vs ~1 MB per platform thread — set it higher without OS pressure.

| Actors blocking 50 ms | OtpDispatchers.IO (Loom) wall | Dispatchers.IO limited(8) wall | Speedup |
|---|---|---|---|
| 40 actors | **51 ms** | 271 ms | **5.3×** |
| 80 actors | **54 ms** | 530 ms | **9.8×** |

**Exception — TCP transport:** `KotlinNodeTransport` uses `kotlinx.coroutines.Dispatchers.IO`, not `OtpDispatchers.IO`. TCP frame writes (~1–3 µs blocking) are shorter than Loom's virtual-thread dispatch overhead (~4–5 µs), so the `CoroutinesScheduler`-backed pool wins by ~9 µs/roundtrip. This is the only measured case where Loom loses.

---

## The three dispatcher profiles

### Profile 1 — Default (6–10 µs p50)

```kotlin
val ref = GenServers.startLink(scope, MyServer(), fastReply = true)
```

Both actor and callers run on `Dispatchers.Default` (a `ForkJoinPool` sized to available CPUs). Every call crosses at least one thread-pool boundary: the actor wakes on a pool thread, processes the call, completes the reply, and the caller resumes on a pool thread. Each boundary costs ~1.5–2 µs on this hardware.

**When to use:** the default for almost all actors. Correct, fair, and bounded by the JVM scheduler.

| Metric | Value |
|---|---|
| Single-caller p50 (standard) | 7.75 µs |
| Single-caller p50 (fastReply) | **6.64 µs** — faster than Pekko (7.26 µs) |
| 10-caller p50 | 6.12 µs (queue depth near 1) |
| 50-caller p50 | 30.32 µs (beats OTP at this concurrency) |
| 100-caller p50 | 61.25 µs |

`fastReply = true` inserts `yield()` after `reply.complete()`, donating the thread to the caller before the actor loops. Saves ~1 µs with no semantic change and no API change to callers.

---

### Profile 2 — Same-dispatcher caller (3–4 µs p50)

```kotlin
// Actor on Default (standard startLink)
val ref = GenServers.startLink(scope, MyServer(), fastReply = true)

// Caller explicitly on the same pool
withContext(Dispatchers.Default) {
    val result = ref.call<String>("request")
}
```

When the caller and actor share `Dispatchers.Default`, the ForkJoinPool can resume the caller's continuation on the same pool thread that just completed the reply — no cross-pool handoff. This eliminates ~3.5 µs of inter-dispatcher scheduling cost.

**When to use:** tight service loops where the calling code is already on `Dispatchers.Default` (typical for coroutine-based request handlers). No actor code change required.

| Metric | Value |
|---|---|
| Single-caller p50 | **3.96 µs** (−47% vs default fastReply) |
| Stability (stddev) | < 0.1 µs — highly stable |

---

### Profile 3 — Unconfined actor (1–2 µs p50)

```kotlin
val ref = GenServers.startLink(
    scope, MyServer(),
    context = Dispatchers.Unconfined,
    fastReply = true,
)
```

`Dispatchers.Unconfined` makes the actor's coroutine resume inline on whichever thread resumed it — in practice, the actor runs directly on the caller's thread with no dispatcher handoff. This brings kotlin-otp to within 1.5× of OTP's 0.71 µs, from the previous 9.3× gap.

**When to use — narrow constraints:**
- **Hot singleton actors only**: rate limiters, config readers, feature-flag servers, single-writer counters. Not for actors that handle diverse callers from multiple threads simultaneously.
- **Caller thread must be stable**: the actor will run on whatever thread calls it. If callers come from many threads, the actor's "thread" changes with every message — no thread affinity, no thread-local state.
- **No blocking inside handleCall/handleCast**: with `Unconfined`, blocking the actor blocks the caller's thread. Never call blocking I/O or `Thread.sleep` from actor callbacks in this profile.
- **Avoid with actors that call other actors**: a chain of `Unconfined` actors can exhaust the call stack (the coroutine stack grows with each inline call).

**Do not use** for general-purpose actors, actors with many concurrent callers, or actors that perform any blocking operations.

| Metric | Value |
|---|---|
| Single-caller p50 | **1.06 µs** |
| vs OTP (0.71 µs) | 1.5× — near parity |
| vs channel floor (0.39 µs) | 0.67 µs of framework overhead |

The 0.67 µs above the channel floor is the cost of the framework itself: `CompletableDeferred` allocation, `select` registration, and message object. This is not reducible without changing the actor contract.

---

## Why the gap to OTP exists — and what it is not

OTP's 0.71 µs comes from the BEAM's process scheduler: Erlang processes share a runtime-level dispatcher that runs continuations inline, with no JVM thread-pool submission. The BEAM is the scheduler.

The 5.6 µs gap between the Default fastReply path (6.64 µs) and the Unconfined path (1.06 µs) is **entirely cross-dispatcher scheduling cost** — ForkJoinPool submit, worker thread wake, and steal operations. It is not framework overhead. With `Unconfined`, that cost disappears and the remaining 0.67 µs is the framework cost alone.

This means: further reducing framework allocations (deferred, select, message objects) can close at most ~0.35 µs of the OTP gap. Sub-2 µs is already achievable today — same-dispatcher callers reach 1.86 µs and `Dispatchers.Unconfined` reaches 0.28 µs (see profiles above).

Loom virtual threads do **not** reduce call roundtrip latency: `Dispatchers.Default` fastReply and a Loom-dispatched actor both measure ~7.8 µs — identical. Loom cannot access `CoroutinesScheduler`'s optimised unpark path, so dispatch overhead is unchanged. Loom's benefit is blocking-worker throughput (5–10× wall-time improvement), not message-passing latency.

---

## Concurrent callers — sharding

Single-actor throughput is bounded by ~1/service_time regardless of how many callers queue up. At the measured ~7 µs service time, the ceiling is ~140k calls/sec from one actor. Use `GenServerRouters.startLink` to distribute load:

```kotlin
val router = GenServerRouters.startLink(
    scope,
    size = 10,             // 10 shards
    factory = { MyStatelessServer() },
    fastReply = true,
)
```

This restores near-single-caller p50 at high concurrency:

| Callers | Single actor p50 | 10-shard router p50 | Improvement |
|---|---|---|---|
| 50 | 30.32 µs | ~9 µs | −70% |
| 100 | 61.25 µs | ~10 µs | −84% |

**Constraint**: shards share no state. Use routers only for stateless actors (echo, rate-token generators) or actors where each key hashes to a consistent shard. Stateful actors that must serialise all writes cannot be naively sharded.

---

## Cached reads — bypassing the actor entirely

For read-heavy, eventually-consistent workloads (config, feature flags, metrics counters), `CachedReadRef` provides non-suspending `AtomicReference.get()` reads at ~40 ns:

```kotlin
data class ConfigState(val timeout: Duration, val maxRetries: Int) : CacheableState<ConfigState> {
    override fun snapshot() = copy()
}

val cache = CachedReadRef<ConfigState>()
val ref = GenServers.startLink(scope, CachingGenServer(ConfigServer(), cache))

// From any thread, no suspension:
val config = cache.readCached() ?: defaultConfig
```

| Pattern | p50 | Throughput |
|---|---|---|
| `ref.call("get")` at 100 callers | 61 µs | 1.3M ops/s (serialised) |
| `cache.readCached()` at 100 callers | **< 40 ns** | **38M ops/s** |

**Consistency guarantee:** eventually consistent. Reads may lag behind the actor's current state by one message. Do not use where linearisable reads are required.

---

## Actor memory footprint

Each idle kotlin-otp actor consumes **~2.5 KB** of heap — comparable to an OTP gen_server process. Coroutines do not hold OS thread stacks when suspended; only the continuation state (~few hundred bytes) plus the channel and ref fields.

| Framework | Bytes per idle actor | Notes |
|---|---|---|
| OTP gen_server | ~2 000–6 000 B | `erlang:process_info(Pid, memory)`; min heap 1.8 KB + PCB |
| **kotlin-otp (Default)** | **~2 600 B (2.5 KB)** | Measured, N=1 000 actors |
| **kotlin-otp (Loom)** | **~2 700 B (2.6 KB)** | Measured, N=1 000 actors |
| Pekko typed actor | ~1 000–2 000 B | Published Lightbend figures |
| Akka typed actor | ~600–2 000 B | Published Akka docs |

**Loom vs Default:** identical for idle actors — a suspended coroutine stores state in a `Continuation` object regardless of dispatcher. The virtual thread stack only exists while the actor is actively running.

**Scaling:**
- 100 000 actors × 2.5 KB = **250 MB**
- 1 000 000 actors × 2.5 KB = **2.5 GB**

OTP's "run millions of processes" property holds for kotlin-otp coroutines too. Both avoid per-actor OS thread stacks. The constraint is JVM heap, not thread count.

**Caveat:** measured via `System.gc()` + heap snapshot — advisory, not precise. For allocator-accurate profiling use JFR (`-XX:+FlightRecorder`) or async-profiler in allocation mode.

---

## TCP distribution

For cross-node calls via `KotlinNodeTransport`, the kernel TCP stack adds ~65 µs above in-memory on loopback:

| Transport | p50 | Notes |
|---|---|---|
| In-memory | ~10 µs | In-process channel routing |
| TCP loopback (`Dispatchers.IO`) | ~56 µs | Kernel TCP + JSON serialisation (steady-state) |
| TCP loopback (Loom) | ~65 µs | ~9 µs worse than IO — see below |
| Akka Artery (reference) | ~155 µs | Aeron UDP + binary; no JSON overhead |

**`KotlinNodeTransport` uses `kotlinx.coroutines.Dispatchers.IO` — the documented exception to the "Loom first" rule.** TCP frame writes (~1–3 µs blocking) are shorter than Loom's dispatch overhead (~4–5 µs), so the `CoroutinesScheduler` wins by ~9 µs/roundtrip. All other blocking paths default to `OtpDispatchers.IO`.

| Dispatcher | TCP p50 | Notes |
|---|---|---|
| `Dispatchers.IO` | **~55 µs** | CoroutinesScheduler: ~200–500 ns dispatch |
| `OtpDispatchers.IO` (Loom) | ~65 µs | ~4–5 µs dispatch — wrong tool for short frames |

The ~55 µs TCP floor is kernel cost. On a LAN (same rack) expect 200–500 µs; across datacentre zones expect 500 µs–5 ms.

---

## JVM runtime profiles

### GC — p999 tail latency

The p50/p95/p99 numbers in this guide are not affected by GC. GC stops appear in p999 and beyond. The default G1GC produces stop-the-world mixed-collection pauses of 20–300 ms in production under memory pressure.

For latency-sensitive services:

```
# ZGC: concurrent collector, pause target < 1 ms
-XX:+UseZGC

# Shenandoah: similar to ZGC, alternative choice
-XX:+UseShenandoahGC
```

Both have ~5–15% throughput overhead vs G1GC. Measure with your actual heap size and load; the tradeoff is always throughput for tail-latency.

See `LIMITATIONS.md` § "Tail latency and GC" for the structural explanation of why per-process GC (BEAM) eliminates this class of problem entirely.

### Scheduler pool sizing

`Dispatchers.Default` sizes its ForkJoinPool to `max(2, availableProcessors)`. On shared infrastructure where available CPUs are oversubscribed:

```
# Pin the coroutine scheduler to the real CPU count of the JVM container
-Dkotlinx.coroutines.scheduler.core.pool.size=4
```

Oversubscription without this flag means the scheduler creates more threads than cores, increasing context-switch jitter. Measure the effect with the concurrent-caller benchmark before tuning.

### JIT warmup

Round 1 numbers in all benchmarks are cold-JIT. In production, pre-warm hot actor paths at startup:

```kotlin
// In Application.start() or similar:
repeat(2_000) { ref.call<Any>("warmup") }
```

Do not use `-XX:TieredStopAtLevel=1` for latency-sensitive actors — it trades peak steady-state performance for faster startup.

---

## Quick reference

| Goal | Mechanism | Result |
|---|---|---|
| Standard default | `startLink(..., fastReply = true)` | **~7.8 µs** p50 |
| Eliminate cross-dispatcher cost | Caller uses `withContext(Dispatchers.Default)` | **~1.9 µs** p50 |
| Near-OTP for hot singleton | `context = Dispatchers.Unconfined` + `fastReply = true` | **~0.28 µs** p50 |
| High concurrency (no sharding) | `GenServerRouters.startLink(size = N)` | ~10 µs at 100 callers |
| Read-heavy eventually consistent | `CachedReadRef` | **< 40 ns** |
| Blocking workers (JDBC, HTTP) | `LongTaskBoundary` — defaults to `OtpDispatchers.IO` | **5–10× faster** vs capped IO at high N |
| TCP transport | `KotlinNodeTransport` — stays on `Dispatchers.IO` | **~55 µs** loopback |
| Idle actor memory | Coroutines (no OS thread stack when suspended) | **~2.5 KB/actor** — comparable to OTP |
| GC tail reduction | `-XX:+UseZGC` | p999 < 1 ms |

---

## Relationship to OTP design principles

These profiles follow the same separation OTP uses:

| OTP mechanism | kotlin-otp equivalent | Notes |
|---|---|---|
| Normal scheduler (gen_server processes) | `Dispatchers.Default` | All actor loops |
| Dirty scheduler (blocking NIFs, port I/O) | `OtpDispatchers.IO` via `LongTaskBoundary` | Default for blocking workers |
| Dirty scheduler (short blocking, TCP) | `kotlinx.coroutines.Dispatchers.IO` | Exception: TCP transport only |
| `gen_server:call` same-scheduler | `Dispatchers.Unconfined` (with same constraints) | Hot singletons only |
| Per-process GC | ZGC (closest available; not equivalent) | p999 improvement only |

Do not use `Dispatchers.Unconfined` where OTP would use dirty schedulers, and do not use `Dispatchers.Default` where OTP expects blocking isolation. The semantics are different; the analogy holds directionally, not literally.

---

## Long-task playbook

When work is long-running (JDBC, HTTP, file operations), treat it as a separate concern from actor-loop latency.

### Pattern

1. Keep the actor loop on `Dispatchers.Default`.
2. Return `ReplyResult.DeferReply` for long requests.
3. Execute long work behind `LongTaskBoundary` (defaults to `OtpDispatchers.IO`):
   - concurrency cap,
   - queue policy (`FailFast` or `BoundedWait`),
   - explicit task timeout.
4. Reply asynchronously through `ReplyHandle`.

This preserves mailbox responsiveness while making saturation behavior explicit and observable.

### Choosing maxConcurrent

`LongTaskBoundary` defaults to `OtpDispatchers.IO` (Loom virtual threads). Because virtual threads cost ~few KB each (vs ~1 MB for platform threads), `maxConcurrent` can be set to match peak expected concurrency rather than OS thread limits:

```kotlin
// With platform threads: set conservatively (OS thread pressure above ~200)
LongTaskBoundary(maxConcurrent = 20, ...)

// With Loom (OtpDispatchers.IO default): set to actual peak concurrency
LongTaskBoundary(maxConcurrent = 200, ...)  // safe — virtual threads
```

### Policy trade-off

| Policy | When all N tasks must complete | When latency ceiling matters more than completion |
|---|---|---|
| `BoundedWait` | ✓ — queues excess, all complete | Wall time = ceil(N/maxConcurrent) × taskDuration |
| `FailFast` | ✗ — rejects overflow | Wall time ≈ taskDuration (1 wave, excess rejected) |

With Loom and `maxConcurrent ≥ N`: both policies give ~taskDuration wall time (1 wave). The recommendation is to set `maxConcurrent` to match peak load and use `BoundedWait` — Loom makes the OS thread cost argument obsolete.

### Benchmark evidence (long profile, 5 rounds steady-state)

| Scenario | Wall time | vs capped IO |
|---|---|---|
| `Dispatchers.IO.limitedParallelism(8)`, 40 actors | 271 ms | baseline |
| `Dispatchers.IO.limitedParallelism(8)`, 80 actors | 530 ms | baseline |
| `OtpDispatchers.IO` (Loom), 40 actors | **51 ms** | **5.3× faster** |
| `OtpDispatchers.IO` (Loom), 80 actors | **54 ms** | **9.8× faster** |

Wall time stays flat with Loom regardless of actor count — all virtual threads run in one wave. IO wall time scales linearly with actor count through the pool ceiling.
