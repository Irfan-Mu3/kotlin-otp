package org.otpstudy.trace

import org.otpstudy.core.OtpProcessId
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Global trace registry — attach handlers to specific actors or globally.
 *
 * Analogous to erlang:trace/3 and dbg:tracer/0.
 *
 * OTP source: erts/emulator/beam/erl_bif_trace.c, lib/runtime_tools/src/dbg.erl
 */
object Tracer {
    private val handlers = ConcurrentHashMap<OtpProcessId, CopyOnWriteArrayList<Pair<Set<TraceFlag>, TraceHandler>>>()
    private val globalHandlers = CopyOnWriteArrayList<Pair<Set<TraceFlag>, TraceHandler>>()

    /**
     * Attach [handler] to [pid] for the given [flags].
     * Returns an [AutoCloseable] that removes the handler.
     * Analogous to erlang:trace(Pid, true, Flags).
     */
    fun trace(pid: OtpProcessId, flags: Set<TraceFlag>, handler: TraceHandler): AutoCloseable {
        val entry = flags to handler
        handlers.getOrPut(pid) { CopyOnWriteArrayList() }.add(entry)
        return AutoCloseable { handlers[pid]?.remove(entry) }
    }

    /**
     * Attach [handler] globally — receives events from all traced actors.
     * Analogous to dbg:tracer/0 with a global match spec.
     */
    fun traceAll(flags: Set<TraceFlag>, handler: TraceHandler): AutoCloseable {
        val entry = flags to handler
        globalHandlers.add(entry)
        return AutoCloseable { globalHandlers.remove(entry) }
    }

    /** Returns true if any handler is registered for [pid] or globally. */
    fun isTracing(pid: OtpProcessId): Boolean =
        handlers[pid]?.isNotEmpty() == true || globalHandlers.isNotEmpty()

    /**
     * Emit a trace event. Called by the GenServer run loop at message boundaries.
     * Handlers run synchronously on the calling coroutine; keep them fast.
     */
    fun emit(event: TraceEvent) {
        val pid = pidOf(event)
        val flag = flagOf(event)
        handlers[pid]?.forEach { (flags, h) ->
            if (flag in flags) runCatching { h.onEvent(event) }
        }
        globalHandlers.forEach { (flags, h) ->
            if (flag in flags) runCatching { h.onEvent(event) }
        }
    }

    fun clearAll() {
        handlers.clear()
        globalHandlers.clear()
    }

    private fun pidOf(event: TraceEvent): OtpProcessId = when (event) {
        is TraceEvent.Send    -> event.from
        is TraceEvent.Receive -> event.pid
        is TraceEvent.Call    -> event.pid
        is TraceEvent.Return  -> event.pid
        is TraceEvent.Procs   -> event.pid
    }

    private fun flagOf(event: TraceEvent): TraceFlag = when (event) {
        is TraceEvent.Send    -> TraceFlag.Send
        is TraceEvent.Receive -> TraceFlag.Receive
        is TraceEvent.Call    -> TraceFlag.Call
        is TraceEvent.Return  -> TraceFlag.Call
        is TraceEvent.Procs   -> TraceFlag.Procs
    }
}

fun interface TraceHandler {
    fun onEvent(event: TraceEvent)
}
