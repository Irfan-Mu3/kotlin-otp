package org.otpstudy.observer

import org.otpstudy.core.OtpProcessId
import org.otpstudy.ets.OtpTableRegistry
import org.otpstudy.ets.TableStats
import org.otpstudy.ets.stats
import org.otpstudy.genserver.GenServerHooks
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.SysStatus
import org.otpstudy.supervisor.SupervisorStrategy

data class ProcessSnapshot(
    val id: OtpProcessId,
    val name: String?,
    val module: String,
    val state: Any?,
    val messageQueueLength: Int,
)

data class SupervisorSnapshot(
    val id: String,
    val strategy: SupervisorStrategy,
    val childIds: List<String>,
)

/**
 * Machine-readable data layer analogous to OTP's `observer` GUI.
 *
 * Data sources:
 * - Per-process state/status: [GenServerRef.sysGetStatus] (§3 sys module)
 * - Process enumeration: [ProcessTable] (voluntary registration)
 * - ETS table stats: [OtpTableRegistry]
 *
 * This is a read-only data layer. State mutation goes through [GenServerRef.sysReplaceState].
 *
 * Next step: expose [OtpObserver] as an HTTP endpoint (Ktor / embedded Javalin) for a minimal
 * browser-based view. That is the synthesis step described in docs/archive/roadmaps/DEEPER_STILL.md §13.
 *
 * OTP source: `lib/observer/src/observer_backend.erl` — reads from sys, process_info, ets
 */
object OtpObserver {
    /**
     * Wire auto-registration of gen-server actors into [ProcessTable].
     *
     * Call once at application startup. After this, every actor started via
     * [org.otpstudy.genserver.GenServers.startLink] automatically appears in [listProcesses],
     * and is removed when the actor's job completes.
     *
     * Analogous to the BEAM maintaining a global process table without user intervention.
     */
    fun install() {
        GenServerHooks.onActorStart = { probe ->
            ProcessTable.registerFromSnapshot("GenServer", probe)
        }
    }

    /** Inspect a running gen_server via its sys channel. */
    suspend fun inspectProcess(ref: GenServerRef<*>): ProcessSnapshot {
        val status: SysStatus = ref.sysGetStatus()
        return ProcessSnapshot(
            id = status.id,
            name = status.name,
            module = status.module,
            state = status.state,
            messageQueueLength = status.messageQueueLength,
        )
    }

    /** List all processes registered in [ProcessTable]. */
    fun listProcesses(): List<ProcessInfo> = ProcessTable.all()

    /** Stats for all named tables in [OtpTableRegistry]. */
    fun tableStats(): List<TableStats> = OtpTableRegistry.allTables().map { it.stats() }
}
