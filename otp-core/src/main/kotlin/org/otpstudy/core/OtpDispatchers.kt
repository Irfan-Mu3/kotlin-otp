package org.otpstudy.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

/**
 * Pre-built dispatcher profiles for common actor deployment patterns.
 *
 * OTP analogy: normal schedulers run gen_server message loops; dirty schedulers
 * run blocking NIFs and port drivers. These dispatchers follow the same separation:
 * use [Default][kotlinx.coroutines.Dispatchers.Default] for CPU-bound actor loops,
 * and [IO] (or [loom] for per-actor lifecycle control) for blocking work.
 *
 * ## Dispatcher selection guide
 *
 * | Use case | Recommended |
 * |---|---|
 * | Actor message loops, CPU-bound work | `Dispatchers.Default` |
 * | DB workers, HTTP clients, file I/O (blocking > ~1 ms) | **[OtpDispatchers.IO]** |
 * | TCP transport frame writes (~1–3 µs blocking) | `Dispatchers.IO` (kotlinx) |
 * | Per-actor lifecycle-managed Loom dispatcher | [loom] |
 *
 * **Why is TCP the exception?** `kotlinx.coroutines.Dispatchers.IO` uses the
 * `CoroutinesScheduler` (~200–500 ns dispatch per hop). [IO] uses virtual threads
 * (~4–5 µs dispatch per hop). For TCP frame writes that complete in ~1–3 µs the
 * dispatch overhead dominates — IO wins by ~9 µs/roundtrip. For all other blocking
 * work (JDBC, HTTP, file) the 4 µs dispatch is negligible vs 1–100 ms blocking.
 * Benchmarks: `docs/deployment/latency-tuning.md`.
 *
 * OTP analogy: [IO] is the dirty scheduler. [Default][kotlinx.coroutines.Dispatchers.Default]
 * is the normal scheduler.
 */
object OtpDispatchers {

    /**
     * Shared application-lifetime Loom dispatcher for blocking I/O work.
     *
     * Backed by [Executors.newVirtualThreadPerTaskExecutor]: one virtual thread
     * per submitted task, unbounded concurrency, virtual threads unmount from
     * their carrier when blocking so carriers are free for other work.
     *
     * **Default for [LongTaskBoundary] and blocking actor workers.** Use this
     * wherever you would previously have used `Dispatchers.IO` for long-blocking
     * tasks (JDBC, HTTP clients, file I/O). At equal concurrency, wall-time is
     * identical to `Dispatchers.IO`. Loom's advantage: virtual threads cost ~few KB
     * vs ~1 MB for platform threads, so `maxConcurrent` can be set much higher
     * without OS thread pressure.
     *
     * Benchmark evidence (40 actors × 50 ms blocking, pool size 8):
     * - `Dispatchers.IO.limitedParallelism(8)`: 268 ms wall (3 waves)
     * - `OtpDispatchers.IO` (uncapped):         54 ms wall (1 wave, 5× faster)
     *
     * **Lifecycle:** shared and application-scoped — do not close. For per-actor
     * lifecycle control use [loom] instead.
     *
     * **Exception — TCP transport:** `KotlinNodeTransport` uses
     * `kotlinx.coroutines.Dispatchers.IO` not this dispatcher, because TCP frame
     * writes (~1–3 µs) are shorter than virtual-thread dispatch overhead (~4–5 µs).
     * That is the only measured case where virtual threads lose to the
     * `CoroutinesScheduler`. Everything else blocking for >5 µs should use this.
     *
     * **Requires JDK 21+.**
     *
     * OTP analogy: dirty scheduler pool — handles blocking work so normal
     * schedulers stay free for actor message processing.
     */
    val IO: CoroutineDispatcher by lazy {
        Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
    }

    /**
     * Creates a **per-use** Loom dispatcher for actors that need individual
     * lifecycle control (create on actor start, close on actor stop).
     *
     * Use [IO] instead unless the actor needs its own isolated executor.
     *
     * **Lifecycle:** the caller owns the returned dispatcher and must close it:
     * ```kotlin
     * val dispatcher = OtpDispatchers.loom()
     * val ref = GenServers.startLink(scope, MyServer(), context = dispatcher)
     * ref.job.invokeOnCompletion { dispatcher.close() }
     * ```
     *
     * **Requires JDK 21+.**
     */
    fun loom(): CoroutineDispatcher =
        Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
}
