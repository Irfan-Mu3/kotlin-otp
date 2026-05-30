# Build environment

- **Run Gradle on JDK 21–25** (Gradle **9.5+** for JDK 25; Gradle 8.11 cannot *run* on JDK 25).
- **Bytecode level:** the build pins **JVM 22** for Kotlin and Java (`compileKotlin` / `compileJava`) so the toolchain stays consistent even when Gradle runs on a newer JDK (e.g. 25).

# What this is not (honest limits)

This Kotlin layout **does not** reproduce the BEAM. It ports **OTP-shaped structure** (supervision flags, child specs, `gen_server`-style callbacks, application lifecycle) onto **coroutines and JVM threads**.

## Runtime semantics

- **No per-actor heaps or independent GC** like Erlang processes; all actors share the JVM heap.
- **No BEAM-style preemptive scheduling** of arbitrary Kotlin code; fairness and isolation differ from OTP.
- **Links and monitors** are not VM-level; library helpers ([`ProcessMonitor`](otp-core/src/main/kotlin/org/otpstudy/core/ProcessMonitor.kt), [`linkJobs`](otp-core/src/main/kotlin/org/otpstudy/core/ProcessMonitor.kt)) surface `DOWN`-style messages via channels; this is not the BEAM emulator.
- **Selective receive** is not implemented; mailboxes are FIFO `Channel`s unless you build prioritization yourself.
- **Hot code upgrade**, **distributed Erlang**, **ETS/Mnesia**, and the **SASL release handler** are out of scope for v0.
- **[THE_HADAL_ZONE.md](THE_HADAL_ZONE.md)** (next roadmap: TCP clustering, JVM post-mortem, and related tooling) is **Kotlin-native only**; **Erlang/Elixir nodes, ETF, BEAM distribution wire, and jinterface-style bridges** are **not** goals of that roadmap.

## Tail latency and GC

The JVM's shared heap garbage collector introduces a structural tail-latency ceiling that this library cannot eliminate.

**G1GC stop-the-world pauses:** JVM G1GC (the default collector) produces stop-the-world mixed-collection pauses ranging from **20 ms** (well-tuned, small heap) to **300 ms** (large heap under memory pressure). Production Akka and Pekko clusters have measured pauses of **80–125 ms** under normal operating conditions.

**Cluster consequences:** During a GC pause, all actors on the affected node stop processing. In a clustered deployment:
- Cluster heartbeat timeouts (typically 3–10 s) are usually not triggered by a single pause, but repeated pauses or pauses under memory pressure can approach the threshold.
- A node that pauses while holding a distributed lock or acting as a singleton coordinator (global registry, shard coordinator) will block dependent nodes for the duration of the pause.
- This is why BEAM-based systems (Discord, WhatsApp, Klarna) choose Erlang for their highest-reliability components — per-process GC means one process's collection does not stop any other.

**BEAM contrast:** Erlang/OTP garbage-collects each process independently, bounded by per-process heap limits and reduction counts. Other processes continue executing through a neighbour's GC cycle. Tail latency on BEAM stays near the median with no spike — this property cannot be replicated on the JVM without per-thread allocation isolation, which the JVM does not provide.

**Available mitigations (JVM-level, not library-level):**
- **ZGC** (`-XX:+UseZGC`): concurrent collector with pause targets under 1 ms, but not zero; throughput overhead ~5–15%.
- **Shenandoah** (`-XX:+UseShenandoahGC`): similar pause profile to ZGC; available on OpenJDK.
- **Smaller heap**: reduces G1GC pause duration at the cost of more frequent collections.
- **GC logging and tuning**: `-Xlog:gc*` to measure actual pause times in your deployment before assuming the worst.

**Recommendation:** For latency-sensitive production deployments using kotlin-otp in a cluster, run ZGC or Shenandoah and measure p99 tail latency under your actual load. Do not rely on p50/p95 numbers alone — GC spikes appear in the tail. See `docs/investigation/benchmark-algorithm-analysis.md` section 2 (`gen_server_call_p99_tail_latency`) for the benchmark that makes this visible.

## Supervision (library semantics)

- **`one_for_one`**, **`one_for_all`**, and **`rest_for_one`** are implemented in [`Supervisor.startLink`](otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/Supervisor.kt) with a single coordinator coroutine; behaviour follows OTP intent but is not BEAM-identical.
- **`ChildSpec.shutdown`** is honoured when stopping children: [`Shutdown.BrutalKill`](otp-core/src/main/kotlin/org/otpstudy/core/Shutdown.kt) cancels without waiting; **timeout** / **infinity** use cooperative `cancel` + `join` (JVM, not `kill`).
- **`SupervisorRef.shutdown()`** stops children in **reverse start order** with each child’s shutdown policy.
- Restart intensity uses **`System.nanoTime()`** sliding windows per failing child; optional **[`RestartBackoff`](otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/RestartBackoff.kt)** delays the next restart after intensity allows it.

## `gen_server` gaps (remaining)

- Callback modules, `-behaviour`, and compiler checks do not exist; use Kotlin types or [`TypedGenServerRef`](otp-gen-server/src/main/kotlin/org/otpstudy/genserver/TypedGenServer.kt) for request/reply typing.
- **`handle_info`** exists on the interface with a default; mailbox is typed to internal messages plus your protocol on `Any`.
- **`code_change`**, **hibernate**, **debug/sys**, and the full OTP proc_lib stack are not ported.

## Other notes

- **JUnit 5 `@Test` return type:** a test method whose body is an expression (for example `fun x() = runBlocking { … }` or `fun x(): Unit = runBlocking { assertNotNull(foo) }`) must be declared to return **`Unit`**. If the last expression is not `Unit` (for example `assertNotNull` returns the asserted value), the JVM signature is **`T method()`** with `T ≠ void`, and **JUnit Jupiter silently ignores** the method — it never appears as skipped; it simply does not run. Prefer `fun x(): Unit = runBlocking { …; Unit }` or a block body ending with `Unit` on its own line.
- **Dynamic supervisors:** [`DynamicSupervisor`](otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/DynamicSupervisor.kt) implements `simple_one_for_one`-style dynamic children only under [`SupervisorStrategy.OneForOne`](otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/SupervisorFlags.kt).
- **Links/monitors** in [`ProcessMonitor`](otp-core/src/main/kotlin/org/otpstudy/core/ProcessMonitor.kt) are not VM-level; install an [`OtpLogger`](otp-core/src/main/kotlin/org/otpstudy/core/OtpLog.kt) via `OtpLogging.setLogger` for correlation in logs.

## Isolation ladder (JVM vs BEAM)

OTP provides a spectrum of isolation from lightweight processes (shared BEAM heap, preemptive
scheduling) to distributed nodes. On the JVM, the equivalent ladder looks like this:

| Level | JVM mechanism | Library support |
|-------|---------------|-----------------|
| Coroutine (default) | `Dispatchers.Default` shared thread pool | Default `startLink` context |
| Virtual thread (Loom, Java 21+) | `Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()` | Pass as `context` to `startLink` |
| Platform thread per actor | `newFixedThreadPoolContext(n, "pool")` | Pass as `context` to `startLink` |
| OS process | `ProcessBuilder` + IPC | Out of scope for this library |
| JVM process / distributed nodes | Multiple JVM instances + gRPC/MQ | Out of scope for this library |

### Loom (virtual threads) example

For actors that perform blocking I/O, JVM virtual threads give BEAM-like lightweight
blocking without blocking a platform thread:

```kotlin
import java.util.concurrent.Executors
import kotlinx.coroutines.asCoroutineDispatcher

val loomDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()

val ref = GenServers.startLink(scope, MyBlockingServer(), context = loomDispatcher)
```

Each actor gets its own virtual thread. Because virtual threads are cheap (comparable to
goroutines or Erlang processes in memory cost), this approximates BEAM's per-process model
for blocking workloads.

**Limitations vs BEAM:**
- No preemptive reduction counting; a tight CPU loop blocks the virtual thread indefinitely.
- GC is still shared across all actors.
- No per-actor heap isolation; one actor's memory leak affects all.

When you need BEAM guarantees, run Erlang/OTP or embed the BEAM—not this library.
