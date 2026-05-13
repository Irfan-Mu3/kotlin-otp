package org.otpstudy.application

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.supervisor.ChildSpec
import org.otpstudy.supervisor.Supervisor
import org.otpstudy.supervisor.SupervisorFlags
import org.otpstudy.supervisor.SupervisorRef

class SupervisorApplication(
    override val name: String,
    private val flags: SupervisorFlags,
    private val children: List<ChildSpec>,
) : OtpApplication {
    private var root: SupervisorRef? = null

    override suspend fun start(parent: CoroutineScope): ApplicationStartResult =
        try {
            val r = Supervisor.startLink(parent, flags, children)
            root = r
            ApplicationStartResult.Ok(r)
        } catch (t: Throwable) {
            ApplicationStartResult.Error(t)
        }

    override suspend fun stop() {
        root?.shutdown()
        root = null
    }
}
