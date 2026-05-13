package org.otpstudy.genserver

import org.otpstudy.core.OtpProcessId

/**
 * Point-in-time snapshot of a running actor for [erlang:process_info/2]-style queries.
 *
 * Populated lazily by a probe lambda registered via [GenServerHooks.onActorStart].
 * The probe is cheap to call (no blocking, no message passing).
 */
data class ActorSnapshot(
    val id: OtpProcessId,
    val name: String?,
    val isActive: Boolean,
    val messageQueueLen: Int,
    val reductions: Long,
    val trapExit: Boolean,
    /** Off-heap bytes allocated by [org.otpstudy.memory.ActorArena]; 0 if no arena. */
    val memoryBytes: Long = 0L,
)

/**
 * Optional lifecycle hooks for [GenServers.startLink].
 *
 * Set [onActorStart] before starting actors; it will be called for each new actor with a
 * probe lambda. The returned [AutoCloseable] is invoked when the actor's [kotlinx.coroutines.Job]
 * completes (normal, cancel, or crash).
 *
 * This is the extension point for [org.otpstudy.observer.ProcessTable] auto-registration.
 * The observer module sets this hook via [org.otpstudy.observer.OtpObserver.install].
 */
object GenServerHooks {
    var onActorStart: ((probe: () -> ActorSnapshot) -> AutoCloseable)? = null
}
