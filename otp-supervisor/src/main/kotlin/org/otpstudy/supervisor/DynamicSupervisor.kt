package org.otpstudy.supervisor

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.otpstudy.core.OtpLogContext
import org.otpstudy.core.OtpLogLevel
import org.otpstudy.core.OtpLogging
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Template for dynamic children: all children share the same restart/shutdown policy
 * and start lambda; each instance gets a distinct [childId].
 *
 * The [start] lambda receives [ready]: invoke it **exactly once** when the supervised
 * resource is usable (e.g. worker linked and running). [DynamicSupervisorRef.startChild]
 * does not wait for [ready]; [DynamicSupervisorRef.startChildSync] awaits the first
 * [ready] value (with a timeout) and returns it to the caller while the child coroutine
 * continues (e.g. `join()` on a worker job).
 *
 * **Misuse:** calling [ready] zero times hangs [startChildSync] until timeout; calling it
 * more than once fails that sync start; throwing before [ready] fails [startChildSync]
 * with that exception (the dynamic child job does not rethrow the same failure once the
 * sync handshake has completed the ready deferred exceptionally).
 *
 * Analogous to OTP `simple_one_for_one` but extended to support all three strategies
 * ([SupervisorStrategy.OneForOne], [SupervisorStrategy.OneForAll], [SupervisorStrategy.RestForOne]).
 *
 * **Strategy semantics for dynamic children:**
 * - `one_for_one`: restart only the failed child (OTP default for dynamic supervisors).
 * - `one_for_all`: on abnormal exit, stop all active children in reverse insertion order,
 *   then restart all from template (re-using existing child IDs).
 * - `rest_for_one`: on abnormal exit, stop the failed child and all children inserted after it
 *   (reverse order), then restart them in insertion order.
 *
 * JVM/OTP difference: OTP's `simple_one_for_one` only supports `one_for_one`. The
 * `one_for_all` and `rest_for_one` semantics here are educational extensions.
 */
data class SimpleOneForOneTemplate<T>(
    val restart: Restart,
    val shutdown: Shutdown,
    /** [scope] is the dynamic child job's scope; invoke [ready] exactly once when the resource is usable. */
    val start: suspend (scope: CoroutineScope, childId: String, ready: (T) -> Unit) -> Unit,
)

data class DynamicChildInfo(
    val id: String,
    val restart: Restart,
    val shutdown: Shutdown,
    val active: Boolean,
)

class DynamicSupervisorRef internal constructor(
    val job: Job,
    val scope: CoroutineScope,
    private val coordinatorJob: Job,
    private val events: Channel<DynamicSupervisorEvent>,
) {
    suspend fun startChild(): String {
        val reply = CompletableDeferred<String>()
        events.send(DynamicSupervisorEvent.StartChild(reply))
        return reply.await()
    }

    /**
     * Starts a child and waits until [SimpleOneForOneTemplate.start] invokes [ready] once
     * with the resource value, then returns [Pair] of [childId] and that value while the
     * child job keeps running.
     *
     * The ready handshake is awaited on a **separate** coroutine so the supervisor coordinator
     * keeps processing other events ([startChild], [whichChildren], [ChildExited], …) while
     * a sync start is in flight.
     *
     * If [shutdown] runs while this call is still waiting, the deferred completes exceptionally
     * with [IllegalStateException] (`"shutting down"`), matching the reject-on-entry behaviour
     * for calls made after shutdown has begun.
     *
     * @param timeout bounds how long the coordinator waits for the first [ready] call.
     * @throws kotlinx.coroutines.TimeoutCancellationException if [ready] is never invoked in time.
     * @throws IllegalStateException if [ready] is invoked more than once for this start.
     */
    suspend fun <T> startChildSync(timeout: Duration = 60.seconds): Pair<String, T> {
        val reply = CompletableDeferred<Pair<String, Any?>>()
        events.send(DynamicSupervisorEvent.StartChildSync(timeout, reply))
        @Suppress("UNCHECKED_CAST")
        return reply.await() as Pair<String, T>
    }

    suspend fun terminateChild(
        id: String,
        shutdownOverride: Shutdown? = null,
    ) {
        val done = CompletableDeferred<Unit>()
        events.send(DynamicSupervisorEvent.TerminateChild(id, shutdownOverride, done))
        done.await()
    }

    suspend fun whichChildren(): List<DynamicChildInfo> {
        val reply = CompletableDeferred<List<DynamicChildInfo>>()
        events.send(DynamicSupervisorEvent.QueryWhich(reply))
        return reply.await()
    }

    suspend fun countChildren(): Int {
        val reply = CompletableDeferred<Int>()
        events.send(DynamicSupervisorEvent.QueryCount(reply))
        return reply.await()
    }

    suspend fun shutdown() {
        val done = CompletableDeferred<Unit>()
        val sent = events.trySend(DynamicSupervisorEvent.RequestShutdown(done))
        if (sent.isSuccess) {
            withTimeoutOrNull(30.seconds) { done.await() }
        }
        scope.cancel()
        job.join()
        coordinatorJob.join()
    }
}

internal sealed class DynamicSupervisorEvent {
    data class StartChild(
        val reply: CompletableDeferred<String>,
    ) : DynamicSupervisorEvent()

    data class StartChildSync(
        val timeout: Duration,
        val reply: CompletableDeferred<Pair<String, Any?>>,
    ) : DynamicSupervisorEvent()

    data class TerminateChild(
        val id: String,
        val shutdownOverride: Shutdown?,
        val done: CompletableDeferred<Unit>,
    ) : DynamicSupervisorEvent()

    data class ChildExited(
        val id: String,
        val startEpoch: Long,
        val kind: ExitKind,
    ) : DynamicSupervisorEvent()

    data class QueryWhich(
        val reply: CompletableDeferred<List<DynamicChildInfo>>,
    ) : DynamicSupervisorEvent()

    data class QueryCount(
        val reply: CompletableDeferred<Int>,
    ) : DynamicSupervisorEvent()

    data class RequestShutdown(
        val done: CompletableDeferred<Unit>,
    ) : DynamicSupervisorEvent()

    /**
     * Internal: sync-waiter coroutine finished (success, timeout, or await failure).
     * [startEpoch] matches the slot incarnation from [startDynamicWorker] for failure cleanup.
     */
    data class StartChildSyncAwaitResult(
        val reply: CompletableDeferred<Pair<String, Any?>>,
        val childId: String,
        val startEpoch: Long,
        val result: Result<Any?>,
    ) : DynamicSupervisorEvent()
}

private sealed class ReadyMode {
    data object Async : ReadyMode()

    data class Sync(
        val deferred: CompletableDeferred<Any?>,
    ) : ReadyMode()
}

private class DynamicChildSlot(
    val id: String,
    val template: SimpleOneForOneTemplate<*>,
) {
    val restartTimestampsNanos: MutableList<Long> = mutableListOf()
    var job: Job? = null
    var startEpoch: Long = 0L

    /** Set for [DynamicSupervisorEvent.StartChildSync] until the handshake completes (success or failure). */
    var syncHandshakeActive: Boolean = false

    /** [StartChildSync] caller reply while the sync waiter is in flight (cleared when handshake completes). */
    var pendingSyncReply: CompletableDeferred<Pair<String, Any?>>? = null

    /**
     * When a sync handshake fails, matches [startEpoch] so [ChildExited] can skip [Restart.Permanent] restart
     * when racing the waiter's failure delivery.
     */
    var lastSyncStartFailedEpoch: Long? = null
}

object DynamicSupervisor {
    data class MetricsSnapshot(
        val syncStarts: Long,
        val syncStartSuccess: Long,
        val syncStartFailure: Long,
        val pendingSyncHighWatermark: Long,
        val reconciliationRetries: Long,
        val restartEvents: Long,
    )

    private val epochSeq = AtomicLong(1L)
    private val idSeq = AtomicLong(0L)
    private val syncStarts = AtomicLong(0L)
    private val syncStartSuccess = AtomicLong(0L)
    private val syncStartFailure = AtomicLong(0L)
    private val pendingSyncHighWatermark = AtomicLong(0L)
    private val reconciliationRetries = AtomicLong(0L)
    private val restartEvents = AtomicLong(0L)

    fun resetMetrics() {
        syncStarts.set(0L)
        syncStartSuccess.set(0L)
        syncStartFailure.set(0L)
        pendingSyncHighWatermark.set(0L)
        reconciliationRetries.set(0L)
        restartEvents.set(0L)
    }

    fun metricsSnapshot(): MetricsSnapshot =
        MetricsSnapshot(
            syncStarts = syncStarts.get(),
            syncStartSuccess = syncStartSuccess.get(),
            syncStartFailure = syncStartFailure.get(),
            pendingSyncHighWatermark = pendingSyncHighWatermark.get(),
            reconciliationRetries = reconciliationRetries.get(),
            restartEvents = restartEvents.get(),
        )

    fun startLink(
        parent: CoroutineScope,
        flags: SupervisorFlags,
        template: SimpleOneForOneTemplate<*>,
        context: CoroutineContext = Dispatchers.Default,
    ): DynamicSupervisorRef {
        val supervisorJob = SupervisorJob(parent.coroutineContext[Job])
        val supervisorScope =
            CoroutineScope(
                parent.coroutineContext + context + supervisorJob + CoroutineName("dynamic-supervisor"),
            )
        val events = Channel<DynamicSupervisorEvent>(Channel.UNLIMITED)
        val shuttingDown = AtomicBoolean(false)
        val children = LinkedHashMap<String, DynamicChildSlot>()

        // Supervisor-wide restart timestamps (M5b)
        val supervisorRestartTimestamps: MutableList<Long> = mutableListOf()

        val coordinator =
            supervisorScope.launch(CoroutineName("dynamic-supervisor-coordinator")) {
                val pendingSyncReplies = mutableSetOf<CompletableDeferred<Pair<String, Any?>>>()
                for (event in events) {
                    when (event) {
                        is DynamicSupervisorEvent.RequestShutdown -> {
                            shuttingDown.set(true)
                            for (reply in pendingSyncReplies.toList()) {
                                if (!reply.isCompleted) {
                                    reply.completeExceptionally(IllegalStateException("shutting down"))
                                }
                            }
                            pendingSyncReplies.clear()
                            for (slot in children.values.toList().asReversed()) {
                                val j = slot.job ?: continue
                                bumpEpoch(slot)
                                runCatching { stopChildJob(j, slot.template.shutdown) }
                                slot.job = null
                            }
                            children.clear()
                            event.done.complete(Unit)
                            return@launch
                        }
                        is DynamicSupervisorEvent.QueryWhich -> {
                            val list =
                                children.values.map { s ->
                                    DynamicChildInfo(
                                        id = s.id,
                                        restart = s.template.restart,
                                        shutdown = s.template.shutdown,
                                        active = s.job?.isActive == true,
                                    )
                                }
                            event.reply.complete(list)
                        }
                        is DynamicSupervisorEvent.QueryCount -> {
                            event.reply.complete(children.size)
                        }
                        is DynamicSupervisorEvent.StartChild -> {
                            if (shuttingDown.get()) {
                                event.reply.completeExceptionally(IllegalStateException("shutting down"))
                                continue
                            }
                            val id = "dyn-${idSeq.incrementAndGet()}"
                            val slot = DynamicChildSlot(id, template)
                            children[id] = slot
                            startDynamicWorker(
                                slot,
                                supervisorScope,
                                events,
                                shuttingDown,
                                readyMode = ReadyMode.Async,
                            )
                            event.reply.complete(id)
                        }
                        is DynamicSupervisorEvent.StartChildSync -> {
                            if (shuttingDown.get()) {
                                event.reply.completeExceptionally(IllegalStateException("shutting down"))
                                continue
                            }
                            val id = "dyn-${idSeq.incrementAndGet()}"
                            val slot = DynamicChildSlot(id, template)
                            children[id] = slot
                            val readyDeferred = CompletableDeferred<Any?>()
                            slot.syncHandshakeActive = true
                            slot.pendingSyncReply = event.reply
                            pendingSyncReplies += event.reply
                            syncStarts.incrementAndGet()
                            pendingSyncHighWatermark.accumulateAndGet(pendingSyncReplies.size.toLong()) { a, b ->
                                if (a > b) a else b
                            }
                            startDynamicWorker(
                                slot,
                                supervisorScope,
                                events,
                                shuttingDown,
                                readyMode = ReadyMode.Sync(readyDeferred),
                            )
                            val job = slot.job!!
                            val epoch = slot.startEpoch
                            supervisorScope.launch(CoroutineName("dynamic-supervisor-sync-wait:$id")) {
                                try {
                                    val value =
                                        withTimeout(event.timeout) {
                                            readyDeferred.await()
                                        }
                                    if (!event.reply.isCompleted) {
                                        events.trySend(
                                            DynamicSupervisorEvent.StartChildSyncAwaitResult(
                                                event.reply,
                                                id,
                                                epoch,
                                                Result.success(value),
                                            ),
                                        )
                                    }
                                } catch (e: TimeoutCancellationException) {
                                    job.cancel(e)
                                    if (!event.reply.isCompleted) {
                                        events.trySend(
                                            DynamicSupervisorEvent.StartChildSyncAwaitResult(
                                                event.reply,
                                                id,
                                                epoch,
                                                Result.failure(e),
                                            ),
                                        )
                                    }
                                } catch (t: Throwable) {
                                    if (t is CancellationException) {
                                        if (!event.reply.isCompleted) {
                                            events.trySend(
                                                DynamicSupervisorEvent.StartChildSyncAwaitResult(
                                                    event.reply,
                                                    id,
                                                    epoch,
                                                    Result.failure(t),
                                                ),
                                            )
                                        }
                                        throw t
                                    }
                                    job.cancel(CancellationException("startChildSync failed", t))
                                    if (!event.reply.isCompleted) {
                                        events.trySend(
                                            DynamicSupervisorEvent.StartChildSyncAwaitResult(
                                                event.reply,
                                                id,
                                                epoch,
                                                Result.failure(t),
                                            ),
                                        )
                                    }
                                }
                            }
                        }
                        is DynamicSupervisorEvent.StartChildSyncAwaitResult -> {
                            pendingSyncReplies -= event.reply
                            val slot = children[event.childId]
                            event.result.fold(
                                onSuccess = { value ->
                                    syncStartSuccess.incrementAndGet()
                                    if (slot != null && slot.startEpoch == event.startEpoch) {
                                        slot.pendingSyncReply = null
                                        slot.syncHandshakeActive = false
                                    }
                                    if (!event.reply.isCompleted) {
                                        event.reply.complete(Pair(event.childId, value))
                                    }
                                },
                                onFailure = { ex ->
                                    syncStartFailure.incrementAndGet()
                                    if (slot != null && slot.startEpoch == event.startEpoch) {
                                        slot.lastSyncStartFailedEpoch = event.startEpoch
                                        children.remove(event.childId)
                                        slot.pendingSyncReply = null
                                        slot.syncHandshakeActive = false
                                    }
                                    if (!event.reply.isCompleted) {
                                        event.reply.completeExceptionally(ex)
                                    }
                                },
                            )
                        }
                        is DynamicSupervisorEvent.TerminateChild -> {
                            val slot = children.remove(event.id)
                            if (slot != null) {
                                val j = slot.job
                                if (j != null) {
                                    bumpEpoch(slot)
                                    val sd = event.shutdownOverride ?: slot.template.shutdown
                                    runCatching { stopChildJob(j, sd) }
                                }
                                slot.job = null
                            }
                            event.done.complete(Unit)
                        }
                        is DynamicSupervisorEvent.ChildExited -> {
                            if (shuttingDown.get()) continue
                            val slot = children[event.id] ?: continue
                            if (event.startEpoch != slot.startEpoch) continue

                            if (event.kind == ExitKind.Normal && slot.syncHandshakeActive) {
                                var bail = false
                                repeat(64) {
                                    reconciliationRetries.incrementAndGet()
                                    yield()
                                    when (val s = children[event.id]) {
                                        null -> {
                                            bail = true
                                            return@repeat
                                        }
                                        else -> {
                                            if (s.startEpoch != event.startEpoch) return@repeat
                                            if (s.lastSyncStartFailedEpoch == event.startEpoch) {
                                                children.remove(event.id)
                                                bail = true
                                                return@repeat
                                            }
                                        }
                                    }
                                }
                                if (bail) continue
                                slot.syncHandshakeActive = false
                            }

                            val abnormal =
                                when (event.kind) {
                                    is ExitKind.Cancelled -> false
                                    ExitKind.Normal -> false
                                    is ExitKind.Failure -> true
                                }
                            if (!RestartPolicy.shouldRestart(slot.template.restart, abnormal)) {
                                children.remove(event.id)
                                continue
                            }

                            val now = System.nanoTime()
                            val canRestart = when (flags.intensityScope) {
                                RestartIntensityScope.PerChild ->
                                    RestartPolicy.canRestart(slot.restartTimestampsNanos, now, flags.intensity, flags.period)
                                RestartIntensityScope.SupervisorWide ->
                                    RestartPolicy.canRestart(supervisorRestartTimestamps, now, flags.intensity, flags.period)
                            }

                            if (!canRestart) {
                                OtpLogging.log(
                                    OtpLogLevel.Error,
                                    OtpLogContext(
                                        "dynamic-supervisor",
                                        tag = slot.id,
                                        childId = slot.id,
                                        restartCount = slot.restartTimestampsNanos.size,
                                    ),
                                    "restart intensity exceeded (scope=${flags.intensityScope})",
                                )
                                supervisorScope.coroutineContext[Job]?.cancel(
                                    CancellationException("restart intensity exceeded for child ${slot.id}"),
                                )
                                return@launch
                            }

                            val backoff =
                                RestartPolicy.backoffDuration(flags.restartBackoff, slot.restartTimestampsNanos.size)
                            delayDuration(backoff)

                            val restartNow = System.nanoTime()
                            slot.restartTimestampsNanos.add(restartNow)
                            restartEvents.incrementAndGet()
                            if (flags.intensityScope == RestartIntensityScope.SupervisorWide) {
                                supervisorRestartTimestamps.add(restartNow)
                            }

                            // M5c: apply strategy
                            when (flags.strategy) {
                                SupervisorStrategy.OneForOne -> {
                                    startDynamicWorker(slot, supervisorScope, events, shuttingDown, ReadyMode.Async)
                                }
                                SupervisorStrategy.OneForAll -> {
                                    // Stop all active children in reverse insertion order
                                    val allActive = children.values.filter { it.job?.isActive == true }.toList().asReversed()
                                    for (s in allActive) {
                                        bumpEpoch(s)
                                        runCatching { stopChildJob(s.job!!, s.template.shutdown) }
                                        s.job = null
                                    }
                                    // Restart all children (preserve childIds, re-use template)
                                    for (s in children.values.toList()) {
                                        startDynamicWorker(s, supervisorScope, events, shuttingDown, ReadyMode.Async)
                                    }
                                }
                                SupervisorStrategy.RestForOne -> {
                                    // Insertion-ordered list; find failed child's position
                                    val allChildren = children.values.toList()
                                    val failedIdx = allChildren.indexOfFirst { it.id == event.id }
                                    if (failedIdx < 0) continue
                                    val suffix = allChildren.subList(failedIdx, allChildren.size)
                                    // Stop suffix in reverse order
                                    for (s in suffix.asReversed()) {
                                        if (s.job?.isActive == true) {
                                            bumpEpoch(s)
                                            runCatching { stopChildJob(s.job!!, s.template.shutdown) }
                                            s.job = null
                                        } else {
                                            bumpEpoch(s)
                                        }
                                    }
                                    // Restart suffix in insertion order
                                    for (s in suffix) {
                                        startDynamicWorker(s, supervisorScope, events, shuttingDown, ReadyMode.Async)
                                    }
                                }
                            }
                        }
                    }
                }
            }

        return DynamicSupervisorRef(supervisorJob, supervisorScope, coordinator, events)
    }

    private fun bumpEpoch(slot: DynamicChildSlot) {
        slot.startEpoch = epochSeq.incrementAndGet()
    }

    private fun startDynamicWorker(
        slot: DynamicChildSlot,
        scope: CoroutineScope,
        events: Channel<DynamicSupervisorEvent>,
        shuttingDown: AtomicBoolean,
        readyMode: ReadyMode,
    ) {
        val epoch = epochSeq.incrementAndGet()
        slot.startEpoch = epoch
        val id = slot.id
        @Suppress("UNCHECKED_CAST")
        val tmpl = slot.template as SimpleOneForOneTemplate<Any?>
        val job =
            scope.launch(CoroutineName("dynamic-child:$id")) {
                try {
                    tmpl.start(this, id) { value ->
                        when (readyMode) {
                            ReadyMode.Async -> Unit
                            is ReadyMode.Sync -> {
                                if (!readyMode.deferred.complete(value)) {
                                    error("ready invoked more than once for dynamic child $id")
                                }
                            }
                        }
                    }
                } catch (t: Throwable) {
                    if (t is CancellationException) throw t
                    if (readyMode is ReadyMode.Sync && !readyMode.deferred.isCompleted) {
                        if (readyMode.deferred.completeExceptionally(t)) {
                            val pr = slot.pendingSyncReply
                            if (pr != null) {
                                events.trySend(
                                    DynamicSupervisorEvent.StartChildSyncAwaitResult(
                                        pr,
                                        id,
                                        slot.startEpoch,
                                        Result.failure(t),
                                    ),
                                )
                            }
                            return@launch
                        }
                    }
                    throw t
                }
            }
        slot.job = job
        job.invokeOnCompletion { cause ->
            if (shuttingDown.get()) return@invokeOnCompletion
            val kind =
                when {
                    cause == null -> ExitKind.Normal
                    cause is CancellationException -> ExitKind.Cancelled(cause)
                    else -> ExitKind.Failure(cause)
                }
            events.trySend(DynamicSupervisorEvent.ChildExited(id, epoch, kind))
        }
    }
}
