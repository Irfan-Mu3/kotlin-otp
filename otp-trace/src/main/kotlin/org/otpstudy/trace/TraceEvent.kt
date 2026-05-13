package org.otpstudy.trace

import org.otpstudy.core.OtpProcessId
import java.time.Instant

sealed class TraceEvent {
    abstract val timestamp: Instant

    data class Send(
        val from: OtpProcessId,
        val to: OtpProcessId?,
        val message: Any,
        override val timestamp: Instant = Instant.now(),
    ) : TraceEvent()

    data class Receive(
        val pid: OtpProcessId,
        val message: Any,
        override val timestamp: Instant = Instant.now(),
    ) : TraceEvent()

    data class Call(
        val pid: OtpProcessId,
        val function: String,
        val args: List<Any?>,
        override val timestamp: Instant = Instant.now(),
    ) : TraceEvent()

    data class Return(
        val pid: OtpProcessId,
        val function: String,
        val result: Any?,
        override val timestamp: Instant = Instant.now(),
    ) : TraceEvent()

    data class Procs(
        val pid: OtpProcessId,
        val event: ProcsEvent,
        override val timestamp: Instant = Instant.now(),
    ) : TraceEvent()

    enum class ProcsEvent {
        Spawned, ExitNormal, ExitShutdown, ExitCrash, NameRegistered, NameUnregistered
    }
}
