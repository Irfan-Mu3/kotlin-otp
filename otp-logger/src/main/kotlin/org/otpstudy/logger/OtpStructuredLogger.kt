package org.otpstudy.logger

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Log level — mirrors OTP logger levels (emergency/alert/critical/error/warning/notice/info/debug).
 * We expose the four most commonly used ones.
 */
enum class LogLevel { Debug, Info, Warning, Error }

/**
 * Structured log event — the kotlin-otp analogue of OTP logger's log event map.
 *
 * OTP source: lib/kernel/src/logger.erl — log/2, the "log event" map
 */
data class LogEvent(
    val level: LogLevel,
    val message: String,
    val metadata: Map<String, Any?> = emptyMap(),
    val timestamp: Instant = Instant.now(),
)

/**
 * Primary or per-handler filter.
 *
 * Return [LogFilterResult.Pass] to short-circuit remaining filters and emit the event.
 * Return [LogFilterResult.Stop] to discard the event entirely.
 * Return [LogFilterResult.Ignore] to let the next filter decide.
 *
 * OTP source: lib/kernel/src/logger_filters.erl
 */
fun interface LogFilter {
    fun filter(event: LogEvent): LogFilterResult
}

sealed class LogFilterResult {
    data object Pass : LogFilterResult()
    data object Stop : LogFilterResult()
    data object Ignore : LogFilterResult()
}

/**
 * Formats a [LogEvent] into a string for a specific handler backend.
 *
 * OTP source: lib/kernel/src/logger_formatter.erl
 */
fun interface LogFormatter {
    fun format(event: LogEvent): String
}

/**
 * The sink that receives formatted log strings.
 *
 * Built-in options: [StdoutBackend], [ListBackend].
 */
fun interface LogHandlerBackend {
    fun emit(formatted: String)
}

data class LogHandler(
    val id: String,
    val filters: List<LogFilter> = emptyList(),
    val formatter: LogFormatter = OtpStructuredLogger.defaultFormatter,
    val backend: LogHandlerBackend,
    val minLevel: LogLevel = LogLevel.Debug,
)

/**
 * Structured logging pipeline — OTP 21+ `logger` in kotlin-otp.
 *
 * Pipeline:
 * ```
 * log() → primary filters → [handler filters → formatter → backend] × N handlers
 * ```
 *
 * Process-local metadata is stored in a [ThreadLocal] (approximates OTP's process metadata dict).
 * Call [setProcessMetadata] from inside an actor to tag all log events with actor-specific fields.
 *
 * OTP source: lib/kernel/src/logger.erl
 */
object OtpStructuredLogger {
    private val primaryFilters = CopyOnWriteArrayList<LogFilter>()
    private val handlers = ConcurrentHashMap<String, LogHandler>()
    private val processMetadata = ThreadLocal<Map<String, Any?>>()

    val defaultFormatter = LogFormatter { event ->
        val meta = (processMetadata.get() ?: emptyMap()) + event.metadata
        "${event.timestamp} [${event.level.name.padEnd(7)}] ${event.message}" +
            if (meta.isEmpty()) "" else " | $meta"
    }

    fun addPrimaryFilter(filter: LogFilter) { primaryFilters.add(filter) }
    fun removePrimaryFilter(filter: LogFilter) { primaryFilters.remove(filter) }

    fun addHandler(handler: LogHandler) { handlers[handler.id] = handler }
    fun removeHandler(id: String) { handlers.remove(id) }
    fun getHandler(id: String): LogHandler? = handlers[id]
    fun handlerIds(): Set<String> = handlers.keys.toSet()

    /** Attach per-actor metadata; all log events emitted on this thread carry these fields. */
    fun setProcessMetadata(metadata: Map<String, Any?>) { processMetadata.set(metadata) }
    fun getProcessMetadata(): Map<String, Any?> = processMetadata.get() ?: emptyMap()
    fun clearProcessMetadata() { processMetadata.remove() }

    fun log(level: LogLevel, message: String, metadata: Map<String, Any?> = emptyMap()) {
        val procMeta = processMetadata.get() ?: emptyMap()
        val event = LogEvent(level, message, procMeta + metadata)

        // Primary filter pass
        for (filter in primaryFilters) {
            when (filter.filter(event)) {
                LogFilterResult.Stop   -> return
                LogFilterResult.Pass   -> break
                LogFilterResult.Ignore -> continue
            }
        }

        // Fan out to handlers
        for (handler in handlers.values) {
            if (event.level.ordinal < handler.minLevel.ordinal) continue
            if (!passesHandlerFilters(handler, event)) continue
            handler.backend.emit(handler.formatter.format(event))
        }
    }

    fun debug(message: String, metadata: Map<String, Any?> = emptyMap()) =
        log(LogLevel.Debug, message, metadata)
    fun info(message: String, metadata: Map<String, Any?> = emptyMap()) =
        log(LogLevel.Info, message, metadata)
    fun warning(message: String, metadata: Map<String, Any?> = emptyMap()) =
        log(LogLevel.Warning, message, metadata)
    fun error(message: String, metadata: Map<String, Any?> = emptyMap()) =
        log(LogLevel.Error, message, metadata)

    fun reset() {
        primaryFilters.clear()
        handlers.clear()
        processMetadata.remove()
    }

    private fun passesHandlerFilters(handler: LogHandler, event: LogEvent): Boolean {
        if (handler.filters.isEmpty()) return true
        for (filter in handler.filters) {
            when (filter.filter(event)) {
                LogFilterResult.Stop   -> return false
                LogFilterResult.Pass   -> return true
                LogFilterResult.Ignore -> continue
            }
        }
        return true
    }
}

/** Standard handler backend that writes to stdout. */
object StdoutBackend : LogHandlerBackend {
    override fun emit(formatted: String) = println(formatted)
}

/** In-memory handler backend for testing — captures all emitted lines. */
class ListBackend : LogHandlerBackend {
    val lines = mutableListOf<String>()
    override fun emit(formatted: String) { lines.add(formatted) }
    fun clear() { lines.clear() }
}
