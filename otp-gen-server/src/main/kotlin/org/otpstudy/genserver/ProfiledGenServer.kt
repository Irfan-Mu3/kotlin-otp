package org.otpstudy.genserver

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Wraps any [GenServer] and records per-callback wall time (OTP `eprof`-style, coarse).
 */
class ProfiledGenServer<S>(private val inner: GenServer<S>) : GenServer<S> {

    data class CallStats(
        val callCount: AtomicLong = AtomicLong(0),
        val totalNs: AtomicLong = AtomicLong(0),
        val minNs: AtomicLong = AtomicLong(Long.MAX_VALUE),
        val maxNs: AtomicLong = AtomicLong(0),
    ) {
        val avgNs: Long get() = if (callCount.get() == 0L) 0L else totalNs.get() / callCount.get()
    }

    private val stats = ConcurrentHashMap<String, CallStats>()

    private inline fun <R> timed(name: String, block: () -> R): R {
        val t0 = System.nanoTime()
        return try {
            block()
        } finally {
            val elapsed = System.nanoTime() - t0
            val s = stats.getOrPut(name) { CallStats() }
            s.callCount.incrementAndGet()
            s.totalNs.addAndGet(elapsed)
            s.minNs.updateAndGet { minOf(it, elapsed) }
            s.maxNs.updateAndGet { maxOf(it, elapsed) }
        }
    }

    override suspend fun init(): InitResult<S> = timed("init") { inner.init() }

    override suspend fun handleCallFrom(request: Any, state: S, from: ReplyHandle<S>): ReplyResult<S> =
        timed("handleCallFrom") { inner.handleCallFrom(request, state, from) }

    override suspend fun handleCall(request: Any, state: S): ReplyResult<S> =
        timed("handleCall") { inner.handleCall(request, state) }

    override suspend fun handleCast(request: Any, state: S): NoreplyResult<S> =
        timed("handleCast") { inner.handleCast(request, state) }

    override suspend fun handleInfo(msg: InfoMsg, state: S): NoreplyResult<S> =
        timed("handleInfo") { inner.handleInfo(msg, state) }

    override suspend fun terminate(reason: TerminateReason, state: S) =
        timed("terminate") { inner.terminate(reason, state) }

    override suspend fun codeChange(oldV: String, newV: String, state: S): S =
        timed("codeChange") { inner.codeChange(oldV, newV, state) }

    override val trapExit: Boolean get() = inner.trapExit

    data class ProfileReport(
        val actorClass: String,
        val callbacks: Map<String, CallStats>,
    ) {
        fun formatted(): String = buildString {
            appendLine("Profile: $actorClass")
            appendLine("${"callback".padEnd(16)} ${"calls".padEnd(10)} ${"total_ms".padEnd(12)} ${"avg_us".padEnd(10)} ${"min_us".padEnd(10)} max_us")
            for ((name, s) in callbacks.entries.sortedByDescending { it.value.totalNs.get() }) {
                appendLine(
                    "${name.padEnd(16)} ${s.callCount.get().toString().padEnd(10)} " +
                        "${"%.2f".format(s.totalNs.get() / 1_000_000.0).padEnd(12)} " +
                        "${"%.1f".format(s.avgNs / 1_000.0).padEnd(10)} " +
                        "${"%.1f".format((if (s.minNs.get() == Long.MAX_VALUE) 0L else s.minNs.get()) / 1_000.0).padEnd(10)} " +
                        "%.1f".format(s.maxNs.get() / 1_000.0),
                )
            }
        }
    }

    fun report(): ProfileReport = ProfileReport(
        actorClass = inner::class.qualifiedName ?: inner::class.simpleName ?: "unknown",
        callbacks = stats.toMap(),
    )

    fun reset() = stats.clear()
}

fun <S> GenServer<S>.profiled(): ProfiledGenServer<S> = ProfiledGenServer(this)
