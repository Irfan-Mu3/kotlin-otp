package org.otpstudy.supervisor

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.core.ChildType
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.time.Duration.Companion.seconds

/**
 * Kotlin analogue of OTP `child_spec` (id, start, restart, shutdown, type).
 * [start] runs until the child exits (normally or with failure); the supervisor
 * observes completion and applies [restart] policy.
 */
data class ChildSpec(
    val id: String,
    val restart: Restart,
    val shutdown: Shutdown = Shutdown.Timeout(5.seconds),
    val type: ChildType = ChildType.Worker,
    val start: suspend CoroutineScope.() -> Unit,
)
