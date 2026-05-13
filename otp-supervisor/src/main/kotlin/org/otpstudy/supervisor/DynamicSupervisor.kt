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
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.otpstudy.core.OtpLogContext
import org.otpstudy.core.OtpLogLevel
import org.otpstudy.core.OtpLogging
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.time.Duration.Companion.seconds

/**
 * Template for dynamic children: all children share the same restart/shutdown policy
 * and start lambda; each instance gets a distinct [childId].
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
data class SimpleOneForOneTemplate(
    val restart: Restart,
    val shutdown: Shutdown,
    val start: suspend CoroutineScope.(childId: String) -> Unit,
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
}

private class DynamicChildSlot(
    val id: String,
    val template: SimpleOneForOneTemplate,
) {
    val restartTimestampsNanos: MutableList<Long> = mutableListOf()
    var job: Job? = null
    var startEpoch: Long = 0L
}

object DynamicSupervisor {
    private val epochSeq = AtomicLong(1L)
    private val idSeq = AtomicLong(0L)

    fun startLink(
        parent: CoroutineScope,
        flags: SupervisorFlags,
        template: SimpleOneForOneTemplate,
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
                for (event in events) {
                    when (event) {
                        is DynamicSupervisorEvent.RequestShutdown -> {
                            shuttingDown.set(true)
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
                            startDynamicWorker(slot, supervisorScope, events, shuttingDown)
                            event.reply.complete(id)
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
                            if (flags.intensityScope == RestartIntensityScope.SupervisorWide) {
                                supervisorRestartTimestamps.add(restartNow)
                            }

                            // M5c: apply strategy
                            when (flags.strategy) {
                                SupervisorStrategy.OneForOne -> {
                                    startDynamicWorker(slot, supervisorScope, events, shuttingDown)
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
                                        startDynamicWorker(s, supervisorScope, events, shuttingDown)
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
                                        startDynamicWorker(s, supervisorScope, events, shuttingDown)
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
    ) {
        val epoch = epochSeq.incrementAndGet()
        slot.startEpoch = epoch
        val id = slot.id
        val job =
            scope.launch(CoroutineName("dynamic-child:$id")) {
                try {
                    slot.template.start(this, id)
                } catch (t: Throwable) {
                    if (t is CancellationException) throw t
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
