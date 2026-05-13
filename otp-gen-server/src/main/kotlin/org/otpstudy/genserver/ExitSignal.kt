package org.otpstudy.genserver

import org.otpstudy.core.OtpProcessId
import kotlinx.coroutines.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

sealed class ExitSignal : InfoMsg {
    data class Exit(
        val from: OtpProcessId,
        val reason: TerminateReason,
    ) : ExitSignal()
}

data class OtpLink(
    val a: OtpProcessId,
    val b: OtpProcessId,
) {
    private val active = AtomicBoolean(true)
    fun unlink() { active.set(false) }
    internal fun isActive() = active.get()
}

fun link(refA: GenServerRef<*>, refB: GenServerRef<*>): OtpLink {
    val otpLink = OtpLink(refA.id, refB.id)
    refA.job.invokeOnCompletion { cause ->
        if (!otpLink.isActive()) return@invokeOnCompletion
        if (cause == null) return@invokeOnCompletion
        if (refB.isTrapExit) {
            val reason = mapCause(cause)
            refB.sendInfo(ExitSignal.Exit(refA.id, reason))
        } else {
            refB.job.cancel(CancellationException("linked process ${refA.id} exited", cause))
        }
    }
    refB.job.invokeOnCompletion { cause ->
        if (!otpLink.isActive()) return@invokeOnCompletion
        if (cause == null) return@invokeOnCompletion
        if (refA.isTrapExit) {
            val reason = mapCause(cause)
            refA.sendInfo(ExitSignal.Exit(refB.id, reason))
        } else {
            refA.job.cancel(CancellationException("linked process ${refB.id} exited", cause))
        }
    }
    return otpLink
}

private fun mapCause(cause: Throwable): TerminateReason =
    if (cause.message?.contains("brutal_kill", ignoreCase = true) == true)
        TerminateReason.BrutalKill
    else
        TerminateReason.Failure(cause)
