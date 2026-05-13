package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration

/**
 * Centralized timer service — OTP `timer` module style.
 *
 * Unlike [OtpTimers], timers here are **not** tied to the creating actor's [CoroutineScope];
 * they run on an internal supervisor scope and must be [cancel]led explicitly.
 */
object OtpTimer {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val pending = ConcurrentHashMap<Ref, Job>()

    @JvmInline
    value class Ref internal constructor(internal val id: UUID = UUID.randomUUID())

    fun sendAfter(delay: Duration, dest: GenServerRef<*>, message: InfoMsg): Ref {
        val ref = Ref()
        pending[ref] = scope.launch {
            kotlinx.coroutines.delay(delay)
            pending.remove(ref)
            if (dest.job.isActive) dest.sendInfo(message)
        }
        return ref
    }

    fun sendInterval(interval: Duration, dest: GenServerRef<*>, message: InfoMsg): Ref {
        val ref = Ref()
        pending[ref] = scope.launch {
            while (isActive) {
                kotlinx.coroutines.delay(interval)
                if (!dest.job.isActive) break
                dest.sendInfo(message)
            }
            pending.remove(ref)
        }
        return ref
    }

    fun applyAfter(delay: Duration, block: () -> Unit): Ref {
        val ref = Ref()
        pending[ref] = scope.launch {
            kotlinx.coroutines.delay(delay)
            pending.remove(ref)
            block()
        }
        return ref
    }

    fun cancel(ref: Ref): Boolean =
        pending.remove(ref)?.also { it.cancel() } != null

    /** Cancel all pending timers (e.g. test teardown). */
    fun reset() {
        pending.values.forEach { it.cancel() }
        pending.clear()
    }
}
