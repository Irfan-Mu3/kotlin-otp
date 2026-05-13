package org.otpstudy.supervisor

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

enum class SupervisorStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

/**
 * Governs whether the restart intensity window is tracked per-failing-child or
 * across all children under the supervisor.
 *
 * - [PerChild]: OTP default. Each child has its own sliding window; a child that
 *   crashes too frequently kills only the supervisor, but fast-crashing children
 *   don't "use up" restarts for stable siblings.
 * - [SupervisorWide]: A single shared window counts every restart across all children.
 *   Mirrors the OTP supervisor's original global restart frequency check when all
 *   children contribute to the same `intensity`/`period` budget.
 */
enum class RestartIntensityScope {
    PerChild,
    SupervisorWide,
}

/**
 * Mirrors OTP supervisor flags (`strategy`, `intensity`, `period`) plus optional
 * [restartBackoff] and [intensityScope].
 */
data class SupervisorFlags(
    val strategy: SupervisorStrategy = SupervisorStrategy.OneForOne,
    val intensity: Int = 1,
    val period: Duration = 5.seconds,
    val restartBackoff: RestartBackoff = RestartBackoff.None,
    val intensityScope: RestartIntensityScope = RestartIntensityScope.PerChild,
) {
    init {
        require(intensity >= 1) { "intensity must be >= 1" }
        require(period > Duration.ZERO) { "period must be positive" }
    }
}
