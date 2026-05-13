package org.otpstudy.genserver

import org.otpstudy.core.OtpProcessId
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

data class CrashReport(
    val id: OtpProcessId,
    val name: String?,
    val supervisorId: String?,
    val reason: TerminateReason,
    val lastState: Any?,
    val messageQueueLength: Int,
    val stackTrace: List<StackTraceElement>,
    val linkedProcesses: List<OtpProcessId>,
    val timestamp: Instant,
)

fun interface CrashReporter {
    fun report(crash: CrashReport)
}

object CrashReporting {
    private val reporter = AtomicReference<CrashReporter?>(null)

    fun setCrashReporter(r: CrashReporter) { reporter.set(r) }
    fun reportCrash(crash: CrashReport) { reporter.get()?.report(crash) }
}
