package org.otpstudy.core

import java.util.concurrent.atomic.AtomicReference

data class OtpLogContext(
    val component: String,
    val processId: OtpProcessId? = null,
    /** Optional short label (e.g. gen_server name, supervisor child id). */
    val tag: String? = null,
    /** ID of the supervising actor, for cross-referencing restart decisions. */
    val supervisorId: String? = null,
    /** ID of the child within its supervisor. */
    val childId: String? = null,
    /** Number of restarts recorded for this child so far. */
    val restartCount: Int? = null,
    /** Current state label for gen_statem actors. */
    val stateLabel: String? = null,
)

fun interface OtpLogger {
    fun log(
        level: OtpLogLevel,
        ctx: OtpLogContext,
        message: String,
        throwable: Throwable?,
    )
}

enum class OtpLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

object OtpLogging {
    private val impl = AtomicReference<OtpLogger>(NoOpLogger)
    private val tracerRef = AtomicReference<OtpTracer>(OtpTracer.NOOP)

    fun setLogger(logger: OtpLogger) {
        impl.set(logger)
    }

    fun setTracer(tracer: OtpTracer) {
        tracerRef.set(tracer)
    }

    fun tracer(): OtpTracer = tracerRef.get()

    fun log(
        level: OtpLogLevel,
        ctx: OtpLogContext,
        message: String,
        throwable: Throwable? = null,
    ) {
        impl.get().log(level, ctx, message, throwable)
    }

    private object NoOpLogger : OtpLogger {
        override fun log(
            level: OtpLogLevel,
            ctx: OtpLogContext,
            message: String,
            throwable: Throwable?,
        ) = Unit
    }
}
