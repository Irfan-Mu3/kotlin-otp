package org.otpstudy.application

import org.otpstudy.supervisor.SupervisorRef

sealed class ApplicationStartResult {
    data class Ok(
        val root: SupervisorRef,
    ) : ApplicationStartResult()

    /** A [StartPhase] failed before the supervisor was started. */
    data class PhaseError(
        val phase: String,
        val cause: Throwable,
    ) : ApplicationStartResult()

    data class Error(
        val cause: Throwable,
    ) : ApplicationStartResult()
}
