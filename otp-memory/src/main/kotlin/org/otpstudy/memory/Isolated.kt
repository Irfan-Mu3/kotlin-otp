package org.otpstudy.memory

import kotlinx.coroutines.Job
import kotlin.coroutines.coroutineContext

/**
 * A value that may only be read by the coroutine (actor) that owns it.
 *
 * Enforces the OTP discipline of per-process state: state is private to the actor that
 * holds it. Pass immutable data-class copies in messages; never share mutable state.
 *
 * Erlang enforces this at the language level (immutable terms, process-local heap).
 * Here it is a runtime check — same intent, library-level enforcement.
 *
 * OTP source: concept mirrors OTP process dictionary and per-process heap isolation.
 */
class Isolated<T>(
    private val value: T,
    private val ownerJob: Job,
) {
    /**
     * Read the value. Throws [IllegalStateException] if called from a different actor's coroutine.
     *
     * Pass an immutable copy (data class) in a message instead of sharing this reference.
     */
    suspend fun get(): T {
        val currentJob = coroutineContext[Job]
        check(currentJob == ownerJob) {
            "Isolated<T> read from wrong actor — pass an immutable copy in the message instead"
        }
        return value
    }

    /** Non-suspend read for use inside the owning actor's synchronous code paths. */
    fun getUnchecked(): T = value
}

/**
 * Create an [Isolated] value bound to the current coroutine's [Job].
 * Must be called from within a coroutine; throws if not.
 */
suspend fun <T> isolated(value: T): Isolated<T> {
    val job = coroutineContext[Job] ?: error("isolated() must be called from a coroutine with a Job")
    return Isolated(value, job)
}
