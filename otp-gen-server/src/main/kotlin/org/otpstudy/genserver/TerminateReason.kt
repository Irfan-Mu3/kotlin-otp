package org.otpstudy.genserver

sealed class TerminateReason {
    data object Normal : TerminateReason()

    data object Shutdown : TerminateReason()

    /** Cooperative cancellation without graceful stop (OTP `brutal_kill` analogue). */
    data object BrutalKill : TerminateReason()

    data class Failure(val cause: Throwable) : TerminateReason()
}
