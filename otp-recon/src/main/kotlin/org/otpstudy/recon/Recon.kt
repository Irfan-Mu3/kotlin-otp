package org.otpstudy.recon

import kotlinx.coroutines.delay
import org.otpstudy.observer.ProcessInfo
import org.otpstudy.observer.ProcessTable

/**
 * Production introspection for kotlin-otp — Fred Hébert's `recon` library in Kotlin.
 *
 * All queries are non-blocking, read-only, and safe to call in production.
 * Built on [ProcessTable] and [ProcessInfo], exactly as `recon` is built on
 * `erlang:processes()` and `erlang:process_info/2`.
 *
 * OTP source: https://github.com/ferd/recon — recon.erl
 */
object Recon {
    /**
     * Top [n] processes by [attribute], sorted descending.
     *
     * Analogous to `recon:proc_count/2`.
     */
    fun procCount(attribute: ProcessAttribute, n: Int): List<ProcessInfoEntry> =
        ProcessTable.all()
            .sortedByDescending { it.attributeValue(attribute) }
            .take(n)
            .map { ProcessInfoEntry(it, it.attributeValue(attribute)) }

    /**
     * Sample [attribute] twice over [windowMs] ms; return top [n] by delta.
     *
     * Analogous to `recon:proc_window/3`. The delta is per-window (not per-second).
     */
    suspend fun procWindow(
        attribute: ProcessAttribute,
        n: Int,
        windowMs: Long,
    ): List<ProcessWindowEntry> {
        val before = ProcessTable.all().associateBy { it.id }
        delay(windowMs)
        val after = ProcessTable.all().associateBy { it.id }
        return after.values
            .mapNotNull { p ->
                val b = before[p.id] ?: return@mapNotNull null
                val delta = p.attributeValue(attribute) - b.attributeValue(attribute)
                ProcessWindowEntry(p, delta)
            }
            .sortedByDescending { it.delta }
            .take(n)
    }

    /** Sorted snapshot of all live actors by [attribute], no limit. */
    fun procList(attribute: ProcessAttribute): List<ProcessInfoEntry> =
        ProcessTable.all()
            .sortedByDescending { it.attributeValue(attribute) }
            .map { ProcessInfoEntry(it, it.attributeValue(attribute)) }

    /**
     * Top [n] processes by approximate object-graph size via [ErtsDump.size].
     *
     * Sizes the [ProcessInfo] snapshot itself, which includes state refs, name strings, etc.
     * Analogous to combining recon:proc_count with erts_debug:size.
     */
    fun processSize(n: Int): List<Pair<ProcessInfo, Long>> =
        ProcessTable.all()
            .map { it to ErtsDump.size(it) }
            .sortedByDescending { it.second }
            .take(n)

    enum class ProcessAttribute { Reductions, MessageQueueLen, Memory }

    data class ProcessInfoEntry(val info: ProcessInfo, val value: Long)
    data class ProcessWindowEntry(val info: ProcessInfo, val delta: Long)

    private fun ProcessInfo.attributeValue(attr: ProcessAttribute): Long = when (attr) {
        ProcessAttribute.Reductions      -> reductions
        ProcessAttribute.MessageQueueLen -> messageQueueLen.toLong()
        ProcessAttribute.Memory          -> memoryBytes
    }
}
