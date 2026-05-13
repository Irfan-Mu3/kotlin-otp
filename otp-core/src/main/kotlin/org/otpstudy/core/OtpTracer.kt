package org.otpstudy.core

/**
 * Minimal tracing interface. The library ships a no-op default; swap in an OpenTelemetry-backed
 * implementation from an `otp-observability` module (or your own) via [OtpLogging.setTracer].
 *
 * The interface is intentionally minimal: span creation helpers only. Metric hooks (restart counts,
 * mailbox depth) can be wired by wrapping the logging path or extending this interface.
 */
interface OtpTracer {
    fun startSpan(name: String, attributes: Map<String, String> = emptyMap()): OtpSpan

    companion object {
        /** No-op tracer — all spans are discarded immediately. */
        val NOOP: OtpTracer = NoOpTracer
    }
}

/** A live span handle. Use [end] (or [close]) to finish it. */
interface OtpSpan : AutoCloseable {
    fun setAttribute(key: String, value: String): OtpSpan
    fun end()
    override fun close() = end()
}

private object NoOpTracer : OtpTracer {
    override fun startSpan(name: String, attributes: Map<String, String>): OtpSpan = NoOpSpan
}

private object NoOpSpan : OtpSpan {
    override fun setAttribute(key: String, value: String): OtpSpan = this
    override fun end() = Unit
}
