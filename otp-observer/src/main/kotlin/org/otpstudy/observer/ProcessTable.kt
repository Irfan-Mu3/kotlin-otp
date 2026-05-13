package org.otpstudy.observer

import org.otpstudy.core.OtpProcessId
import org.otpstudy.genserver.ActorSnapshot
import java.util.concurrent.ConcurrentHashMap

enum class ProcessStatus { Running, Waiting, Dead }

/**
 * Structured record of the actor's initial call — mirrors OTP `process_info(Pid, initial_call)`.
 *
 * For GenServer actors: module = class qualified name, function = "init", arity = 0.
 *
 * OTP source: erts/emulator/beam/erl_bif_info.c — process_info, initial_call
 */
data class InitialCall(
    val module: String,
    val function: String,
    val arity: Int,
)

/**
 * Extended per-actor snapshot for `erlang:process_info/2`-style queries.
 *
 * Fields map directly to OTP process_info keys:
 * - [messageQueueLen]  → message_queue_len
 * - [reductions]       → reductions
 * - [trapExit]         → trap_exit
 * - [status]           → status (running/waiting)
 * - [memoryBytes]      → memory (off-heap bytes; non-zero only when ActorArena is wired)
 * - [initialCall]      → initial_call ({Module, Function, Arity})
 *
 * OTP source: `erts/emulator/beam/erl_bif_info.c` process_info/2
 */
data class ProcessInfo(
    val id: OtpProcessId,
    val name: String?,
    val status: ProcessStatus,
    val module: String,
    val messageQueueLen: Int = 0,
    val reductions: Long = 0L,
    val trapExit: Boolean = false,
    /** Off-heap bytes allocated by ActorArena; 0 for actors without an arena. */
    val memoryBytes: Long = 0L,
    /** Structured initial-call record (proc_lib semantics). */
    val initialCall: InitialCall? = null,
)

/**
 * Global registry of live actor probes.
 *
 * Actors call [register] at start and receive an [AutoCloseable] that unregisters them on stop.
 * Each probe is a lambda invoked lazily when introspection is requested — O(1) per actor.
 *
 * Analogous to the per-process metadata storage the BEAM maintains for `erlang:process_info/2`.
 *
 * Wire up auto-registration from gen-server actors by calling [OtpObserver.install] at startup.
 */
object ProcessTable {
    private val probes = ConcurrentHashMap<OtpProcessId, () -> ProcessInfo>()

    fun register(id: OtpProcessId, probe: () -> ProcessInfo): AutoCloseable {
        probes[id] = probe
        return AutoCloseable { probes.remove(id) }
    }

    /** Register using a live [ActorSnapshot] probe from [org.otpstudy.genserver.GenServerHooks]. */
    internal fun registerFromSnapshot(
        module: String,
        probe: () -> ActorSnapshot,
    ): AutoCloseable {
        val snapshot = probe()
        val id = snapshot.id
        val initialCall = InitialCall(module, "init", 0)
        probes[id] = {
            val s = probe()
            ProcessInfo(
                id = s.id,
                name = s.name,
                status = if (s.isActive) ProcessStatus.Running else ProcessStatus.Dead,
                module = module,
                messageQueueLen = s.messageQueueLen,
                reductions = s.reductions,
                trapExit = s.trapExit,
                memoryBytes = s.memoryBytes,
                initialCall = initialCall,
            )
        }
        return AutoCloseable { probes.remove(id) }
    }

    fun info(id: OtpProcessId): ProcessInfo? = probes[id]?.invoke()

    fun all(): List<ProcessInfo> = probes.values.map { it.invoke() }

    fun clear() { probes.clear() }
}
