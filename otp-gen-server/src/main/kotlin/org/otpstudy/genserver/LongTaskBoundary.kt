package org.otpstudy.genserver

import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.otpstudy.core.OtpDispatchers

enum class LongTaskBackpressurePolicy {
    FailFast,
    BoundedWait,
}

class LongTaskRejectedException(message: String) : RuntimeException(message)

/**
 * Coroutine-native boundary for long-running or blocking tasks.
 *
 * It provides:
 * - bounded concurrency via [maxConcurrent]
 * - queue admission behavior via [policy] and [queueTimeout]
 * - task deadline via [taskTimeout]
 *
 * Intended usage in GenServer handlers:
 * - keep actor loop on Dispatchers.Default
 * - run long/blocking work via [run]
 * - return [ReplyResult.DeferReply] and complete later
 *
 * **[workerContext] default:** [OtpDispatchers.IO] — a shared Loom virtual-thread
 * dispatcher. Virtual threads unmount from their carrier when blocking, so
 * [maxConcurrent] can be set much higher than a platform-thread pool allows without
 * OS thread pressure. For genuinely CPU-bound tasks (no blocking) pass
 * `Dispatchers.Default` explicitly.
 *
 * **Exception:** if [run] wraps TCP socket operations, pass
 * `kotlinx.coroutines.Dispatchers.IO` — the CoroutinesScheduler is ~9 µs faster
 * per roundtrip for sub-millisecond blocking.
 *
 * OTP analogy: a dirty-scheduler fence — offloads blocking NIFs so normal
 * schedulers stay free for actor message processing.
 */
class LongTaskBoundary(
    private val workerContext: CoroutineContext = OtpDispatchers.IO,
    private val maxConcurrent: Int,
    private val policy: LongTaskBackpressurePolicy = LongTaskBackpressurePolicy.BoundedWait,
    private val queueTimeout: Duration,
    private val taskTimeout: Duration,
) {
    private val permits = Semaphore(maxConcurrent)
    private val inFlight = AtomicInteger(0)
    private val rejected = AtomicInteger(0)
    private val queued = AtomicInteger(0)
    private val maxInFlightObserved = AtomicInteger(0)
    private val maxQueuedObserved = AtomicInteger(0)

    data class Snapshot(
        val inFlight: Int,
        val queued: Int,
        val rejected: Int,
        val maxConcurrent: Int,
        val maxInFlightObserved: Int,
        val maxQueuedObserved: Int,
    )

    fun snapshot(): Snapshot =
        Snapshot(
            inFlight = inFlight.get(),
            queued = queued.get(),
            rejected = rejected.get(),
            maxConcurrent = maxConcurrent,
            maxInFlightObserved = maxInFlightObserved.get(),
            maxQueuedObserved = maxQueuedObserved.get(),
        )

    suspend fun <T> run(task: suspend () -> T): T {
        acquirePermit()
        return try {
            inFlight.incrementAndGet()
            maxInFlightObserved.accumulateAndGet(inFlight.get()) { a, b -> if (a > b) a else b }
            withContext(workerContext) {
                withTimeout(taskTimeout) { task() }
            }
        } finally {
            inFlight.decrementAndGet()
            permits.release()
        }
    }

    private suspend fun acquirePermit() {
        if (policy == LongTaskBackpressurePolicy.FailFast) {
            if (!permits.tryAcquire()) {
                rejected.incrementAndGet()
                throw LongTaskRejectedException("long-task boundary saturated (maxConcurrent=$maxConcurrent)")
            }
            return
        }

        queued.incrementAndGet()
        maxQueuedObserved.accumulateAndGet(queued.get()) { a, b -> if (a > b) a else b }
        try {
            val acquired = withTimeoutOrNull(queueTimeout) { permits.acquire(); true } ?: false
            if (!acquired) {
                rejected.incrementAndGet()
                throw LongTaskRejectedException("long-task queue timeout after $queueTimeout (maxConcurrent=$maxConcurrent)")
            }
        } finally {
            queued.decrementAndGet()
        }
    }
}
