@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

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
import org.otpstudy.core.ChildType
import org.otpstudy.core.OtpLogContext
import org.otpstudy.core.OtpLogLevel
import org.otpstudy.core.OtpLogging
import kotlin.time.Duration.Companion.seconds

/** Point-in-time snapshot of one static supervisor child (see [SupervisorRef.whichChildren]). */
data class SupervisorChildInfo(
    val id: String,
    val childType: ChildType,
    val isActive: Boolean,
    val restartCount: Int,
)

class SupervisorRef internal constructor(
    val job: Job,
    val scope: CoroutineScope,
    private val coordinatorJob: Job,
    private val events: Channel<SupervisorEvent>,
    /** Live view of children; keeps [ChildSlot] file-private (no visibility leak on ctor). */
    private val childSnapshot: () -> List<SupervisorChildInfo>,
) {
    /** Current view of declared children; safe to call from any thread. */
    fun whichChildren(): List<SupervisorChildInfo> = childSnapshot()

    /**
     * Stops children in **reverse start order**, honouring each [ChildSpec.shutdown], then cancels the supervisor scope.
     */
    suspend fun shutdown() {
        val done = CompletableDeferred<Unit>()
        val sent = events.trySend(SupervisorEvent.RequestShutdown(done))
        if (sent.isSuccess) {
            withTimeoutOrNull(30.seconds) { done.await() }
        }
        scope.cancel()
        job.join()
        coordinatorJob.join()
    }
}

internal sealed class SupervisorEvent {
    data class ChildExited(
        val index: Int,
        val startEpoch: Long,
        val kind: ExitKind,
    ) : SupervisorEvent()

    data class RequestShutdown(
        val done: CompletableDeferred<Unit>,
    ) : SupervisorEvent()
}

internal sealed class ExitKind {
    data object Normal : ExitKind()

    data class Failure(
        val cause: Throwable,
    ) : ExitKind()

    data class Cancelled(
        val cause: CancellationException,
    ) : ExitKind()
}

private class ChildSlot(
    val index: Int,
    val spec: ChildSpec,
) {
    val restartTimestampsNanos: MutableList<Long> = mutableListOf()
    var job: Job? = null
    var startEpoch: Long = 0L
}

object Supervisor {
    private val epochSeq = AtomicLong(1L)

    /**
     * Starts a supervisor tree under [parent]. Children are started in declaration order.
     * [SupervisorRef.shutdown] stops children in reverse order with per-child [ChildSpec.shutdown].
     */
    fun startLink(
        parent: CoroutineScope,
        flags: SupervisorFlags,
        children: List<ChildSpec>,
        context: CoroutineContext = Dispatchers.Default,
    ): SupervisorRef {
        val supervisorJob = SupervisorJob(parent.coroutineContext[Job])
        val supervisorScope =
            CoroutineScope(
                parent.coroutineContext + context + supervisorJob + CoroutineName("supervisor"),
            )
        val events = Channel<SupervisorEvent>(Channel.UNLIMITED)
        val shuttingDown = AtomicBoolean(false)

        val slots = children.mapIndexed { i, spec -> ChildSlot(i, spec) }

        val coordinator =
            supervisorScope.launch(CoroutineName("supervisor-coordinator")) {
                runCoordinatorLoop(slots, flags, events, supervisorScope, shuttingDown)
            }

        for (slot in slots) {
            startWorker(slot, supervisorScope, events, shuttingDown)
        }

        return SupervisorRef(
            supervisorJob,
            supervisorScope,
            coordinator,
            events,
            childSnapshot = {
                slots.map { slot ->
                    SupervisorChildInfo(
                        id = slot.spec.id,
                        childType = slot.spec.type,
                        isActive = slot.job?.isActive == true,
                        restartCount = slot.restartTimestampsNanos.size,
                    )
                }
            },
        )
    }

    private suspend fun runCoordinatorLoop(
        slots: List<ChildSlot>,
        flags: SupervisorFlags,
        events: Channel<SupervisorEvent>,
        supervisorScope: CoroutineScope,
        shuttingDown: AtomicBoolean,
    ) {
        // Supervisor-wide intensity window (M5b)
        val supervisorRestartTimestamps: MutableList<Long> = mutableListOf()

        try {
            for (event in events) {
                when (event) {
                    is SupervisorEvent.RequestShutdown -> {
                        shuttingDown.set(true)
                        stopAllChildrenReverseOrder(slots)
                        event.done.complete(Unit)
                        return
                    }
                    is SupervisorEvent.ChildExited -> {
                        if (shuttingDown.get()) continue
                        val slot = slots.getOrNull(event.index) ?: continue
                        if (event.startEpoch != slot.startEpoch) continue

                        val abnormal =
                            when (event.kind) {
                                is ExitKind.Cancelled -> false
                                ExitKind.Normal -> false
                                is ExitKind.Failure -> true
                            }

                        if (!RestartPolicy.shouldRestart(slot.spec.restart, abnormal)) continue

                        val now = System.nanoTime()

                        // M5b: check intensity against per-child or supervisor-wide window
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
                                    "supervisor",
                                    tag = slot.spec.id,
                                    childId = slot.spec.id,
                                    restartCount = slot.restartTimestampsNanos.size,
                                ),
                                "restart intensity exceeded (scope=${flags.intensityScope})",
                            )
                            supervisorScope.coroutineContext[Job]?.cancel(
                                CancellationException("restart intensity exceeded for child ${slot.spec.id}"),
                            )
                            return
                        }

                        val backoff =
                            RestartPolicy.backoffDuration(flags.restartBackoff, slot.restartTimestampsNanos.size)
                        delayDuration(backoff)

                        val restartNow = System.nanoTime()
                        slot.restartTimestampsNanos.add(restartNow)
                        if (flags.intensityScope == RestartIntensityScope.SupervisorWide) {
                            supervisorRestartTimestamps.add(restartNow)
                        }

                        OtpLogging.log(
                            OtpLogLevel.Info,
                            OtpLogContext(
                                "supervisor",
                                tag = slot.spec.id,
                                childId = slot.spec.id,
                                restartCount = slot.restartTimestampsNanos.size,
                            ),
                            "restarting child (strategy=${flags.strategy})",
                        )

                        when (flags.strategy) {
                            SupervisorStrategy.OneForOne -> {
                                startWorker(slot, supervisorScope, events, shuttingDown)
                            }
                            SupervisorStrategy.OneForAll -> {
                                val toStop =
                                    slots.filter { it.job?.isActive == true }
                                        .sortedByDescending { it.index }
                                stopSlotsReverseOrder(toStop)
                                for (s in slots) {
                                    bumpEpochInvalidate(s)
                                    s.job = null
                                }
                                for (s in slots) {
                                    startWorker(s, supervisorScope, events, shuttingDown)
                                }
                            }
                            SupervisorStrategy.RestForOne -> {
                                val younger =
                                    slots.filter { it.index > slot.index && it.job?.isActive == true }
                                        .sortedByDescending { it.index }
                                stopSlotsReverseOrder(younger)
                                val suffix = slots.filter { it.index >= slot.index }
                                for (s in suffix) {
                                    if (s.job?.isActive == true) {
                                        bumpEpochInvalidate(s)
                                        runCatching { stopChildJob(checkNotNull(s.job), s.spec.shutdown) }
                                    } else {
                                        bumpEpochInvalidate(s)
                                    }
                                    s.job = null
                                }
                                for (s in suffix) {
                                    startWorker(s, supervisorScope, events, shuttingDown)
                                }
                            }
                        }
                    }
                }
            }
        } catch (_: CancellationException) {
            throw CancellationException()
        }
    }

    private suspend fun stopAllChildrenReverseOrder(slots: List<ChildSlot>) {
        for (slot in slots.asReversed()) {
            val j = slot.job ?: continue
            bumpEpochInvalidate(slot)
            runCatching { stopChildJob(j, slot.spec.shutdown) }
            slot.job = null
        }
    }

    private suspend fun stopSlotsReverseOrder(slots: List<ChildSlot>) {
        for (slot in slots.sortedByDescending { it.index }) {
            val j = slot.job ?: continue
            bumpEpochInvalidate(slot)
            runCatching { stopChildJob(j, slot.spec.shutdown) }
            slot.job = null
        }
    }

    private fun bumpEpochInvalidate(slot: ChildSlot) {
        slot.startEpoch = epochSeq.incrementAndGet()
    }

    private fun startWorker(
        slot: ChildSlot,
        scope: CoroutineScope,
        events: Channel<SupervisorEvent>,
        shuttingDown: AtomicBoolean,
    ) {
        val epoch = epochSeq.incrementAndGet()
        slot.startEpoch = epoch
        val job =
            scope.launch(CoroutineName("supervisor-child:${slot.spec.id}")) {
                try {
                    slot.spec.start(this)
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
            events.trySend(SupervisorEvent.ChildExited(slot.index, epoch, kind))
        }
    }
}
