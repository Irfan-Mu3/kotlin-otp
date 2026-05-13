package org.otpstudy.application

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.supervisor.ChildSpec
import org.otpstudy.supervisor.Supervisor
import org.otpstudy.supervisor.SupervisorFlags
import org.otpstudy.supervisor.SupervisorRef

/**
 * A named unit of work that must complete before the supervisor root is started.
 *
 * ```
 * flowchart LR
 *   subgraph boot [PhasedBoot]
 *     P1[start_phase A] --> P2[start_phase B] --> P3[SupervisorRoot]
 *   end
 * ```
 *
 * Phases run in list order inside the [parent] scope. A phase failure aborts startup
 * with [ApplicationStartResult.PhaseError] and subsequent phases are skipped.
 */
data class StartPhase(
    val name: String,
    val run: suspend CoroutineScope.(env: ApplicationEnv) -> Unit,
)

/**
 * [OtpApplication] that runs [phases] sequentially before starting a supervision root.
 *
 * Phases receive the [ApplicationEnv] and can mutate it via their own side effects or
 * populate external systems (e.g. register config, start optional sub-systems).
 *
 * If all phases succeed, [Supervisor.startLink] is called with [flags] and [children].
 */
class PhasedSupervisorApplication(
    override val name: String,
    private val flags: SupervisorFlags,
    private val children: List<ChildSpec>,
    private val phases: List<StartPhase> = emptyList(),
    private val env: ApplicationEnv = ApplicationEnv.EMPTY,
) : OtpApplication {
    private var root: SupervisorRef? = null

    override suspend fun start(parent: CoroutineScope): ApplicationStartResult {
        for (phase in phases) {
            try {
                phase.run(parent, env)
            } catch (t: Throwable) {
                return ApplicationStartResult.PhaseError(phase.name, t)
            }
        }
        return try {
            val r = Supervisor.startLink(parent, flags, children)
            root = r
            ApplicationStartResult.Ok(r)
        } catch (t: Throwable) {
            ApplicationStartResult.Error(t)
        }
    }

    override suspend fun stop() {
        root?.shutdown()
        root = null
    }
}
