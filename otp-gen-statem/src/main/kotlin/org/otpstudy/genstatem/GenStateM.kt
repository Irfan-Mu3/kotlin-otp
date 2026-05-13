package org.otpstudy.genstatem

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.otpstudy.core.OtpProcessId
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// ---------------------------------------------------------------------------
// Timeout types (OTP-style: state_timeout, event_timeout, generic_timeout)
// ---------------------------------------------------------------------------

/**
 * Identifies which kind of OTP-style timeout to set or that fired.
 *
 * - [StateTimeout]: cancelled when the FSM transitions to a different state.
 * - [EventTimeout]: cancelled when any external user event arrives.
 * - [GenericTimeout]: named; not auto-cancelled by events or state changes—only
 *   replaced when a new [GenericTimeout] with the same [name] is set.
 *
 * JVM/OTP difference: OTP's `gen_statem` timeouts run in the BEAM scheduler;
 * here they are `kotlinx.coroutines.delay`-based coroutine jobs under the
 * same scope as the mailbox loop.
 */
sealed class StateMTimeoutKind {
    data object StateTimeout : StateMTimeoutKind()
    data object EventTimeout : StateMTimeoutKind()
    data class GenericTimeout(val name: String) : StateMTimeoutKind()
}

/** Specifies a timeout to arm on a transition or init result. */
data class StateMTimeoutSpec(val kind: StateMTimeoutKind, val delay: Duration)

// ---------------------------------------------------------------------------
// Internal mailbox protocol
// ---------------------------------------------------------------------------

/** Internal mailbox messages. [epoch] on [TimeoutFired] guards stale firings after cancellation. */
internal sealed class GenStateMMsg {
    class Event<E>(
        val event: E,
        val reply: CompletableDeferred<StateMTransition<*, *, *>>?,
    ) : GenStateMMsg()

    /** Synthetic message injected by a timer coroutine when a timeout fires. */
    data class TimeoutFired(val kind: StateMTimeoutKind, val epoch: Long) : GenStateMMsg()

    data object Stop : GenStateMMsg()
}

// ---------------------------------------------------------------------------
// Transition result types
// ---------------------------------------------------------------------------

/**
 * Result of handling an event or timeout.
 *
 * [Stay] and [Next] carry optional [timeout] and [postpone] flag.
 * When [postpone] is true, the current event is re-queued at the front of the next state's
 * event processing (replayed on next state change). Analogous to OTP `{postpone, true}`.
 */
sealed class StateMTransition<out S : Any, out D, out E> {
    data class Stay<S : Any, D, E>(
        val newData: D,
        val timeout: StateMTimeoutSpec? = null,
        val postpone: Boolean = false,
    ) : StateMTransition<S, D, E>()

    data class Next<S : Any, D, E>(
        val newState: S,
        val newData: D,
        val timeout: StateMTimeoutSpec? = null,
        val postpone: Boolean = false,
    ) : StateMTransition<S, D, E>()

    data class Stop(
        val reason: Throwable? = null,
    ) : StateMTransition<Nothing, Nothing, Nothing>()
}

// ---------------------------------------------------------------------------
// Callback interfaces
// ---------------------------------------------------------------------------

/**
 * OTP [`gen_statem`](https://www.erlang.org/doc/design_principles/statem.html)-shaped callback module.
 *
 * Override [handleTimeout] to react to timers armed via [StateMTimeoutSpec] on transitions or init.
 * Override [onEnterState] to react on state entry (analogous to OTP state-enter callback mode).
 */
interface GenStateM<S : Any, D, E> {
    suspend fun init(): InitStateMResult<S, D>

    suspend fun handleEvent(
        state: S,
        data: D,
        event: E,
    ): StateMTransition<S, D, E>

    /** Called when a timeout fires. Default: stay in current state, no new timeout. */
    suspend fun handleTimeout(
        state: S,
        data: D,
        kind: StateMTimeoutKind,
    ): StateMTransition<S, D, E> = StateMTransition.Stay(data)

    /**
     * Called when entering [newState] (analogous to OTP state-enter callback).
     * [prevState] is null on initial entry from init.
     * Default: stay in [newState] with unchanged data.
     *
     * Risk: returning [StateMTransition.Next] with a different state from here triggers
     * another state-enter. Returning [StateMTransition.Next] with the SAME state is
     * treated as [StateMTransition.Stay] to prevent infinite loops.
     */
    suspend fun onEnterState(
        newState: S,
        prevState: S?,
        data: D,
    ): StateMTransition<S, D, E> = StateMTransition.Stay(data)
}

sealed class InitStateMResult<out S : Any, out D> {
    data class Ok<S : Any, D>(
        val state: S,
        val data: D,
        /** Optional timeout to arm immediately after init. */
        val timeout: StateMTimeoutSpec? = null,
    ) : InitStateMResult<S, D>()

    data class Stop(
        val reason: Throwable? = null,
    ) : InitStateMResult<Nothing, Nothing>()
}

// ---------------------------------------------------------------------------
// M2: handle_event_function-style input wrapper
// ---------------------------------------------------------------------------

/**
 * Unified input type for [GenStateMHandleEvent].
 *
 * Wraps either a user event or a fired timeout, mirroring OTP `gen_statem`
 * `handle_event_function` mode where a single callback handles all inputs.
 */
sealed class StateMInput<out E> {
    data class User<E>(val event: E) : StateMInput<E>()
    data class Timeout(val kind: StateMTimeoutKind) : StateMInput<Nothing>()
}

/**
 * Alternative callback shape (OTP `handle_event_function` analogue): a single [handleEvent]
 * receives both user events and timeout firings as [StateMInput].
 *
 * Use [GenStateMs.startLinkHandleEvent] to start a machine with this interface.
 */
interface GenStateMHandleEvent<S : Any, D, E> {
    suspend fun init(): InitStateMResult<S, D>

    suspend fun handleEvent(
        state: S,
        data: D,
        input: StateMInput<E>,
    ): StateMTransition<S, D, E>
}

// ---------------------------------------------------------------------------
// Ref and factory
// ---------------------------------------------------------------------------

class GenStateMRef<S : Any, D, E> internal constructor(
    val id: OtpProcessId,
    val job: Job,
    private val mailbox: Channel<GenStateMMsg>,
) {
    suspend fun sendEvent(
        event: E,
        timeout: Duration = 5.seconds,
    ): StateMTransition<S, D, E> =
        withTimeout(timeout) {
            val reply = CompletableDeferred<StateMTransition<*, *, *>>()
            mailbox.send(GenStateMMsg.Event(event, reply))
            @Suppress("UNCHECKED_CAST")
            reply.await() as StateMTransition<S, D, E>
        }

    suspend fun castEvent(event: E) {
        mailbox.send(GenStateMMsg.Event(event, null))
    }

    suspend fun stop() {
        mailbox.send(GenStateMMsg.Stop)
        job.join()
    }
}

object GenStateMs {
    fun <S : Any, D, E> startLink(
        parent: CoroutineScope,
        machine: GenStateM<S, D, E>,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
    ): GenStateMRef<S, D, E> {
        val id = OtpProcessId.allocate()
        val mailbox = Channel<GenStateMMsg>(Channel.UNLIMITED)
        val jobName = name?.let { CoroutineName("gen_statem:$it") } ?: CoroutineName("gen_statem:$id")
        val job =
            parent.launch(context + jobName) {
                runLoop(this, machine, mailbox)
            }
        return GenStateMRef(id, job, mailbox)
    }

    /**
     * Starts a machine using the [GenStateMHandleEvent] callback interface
     * (OTP `handle_event_function` analogue). Events and timeouts both arrive
     * through the single [GenStateMHandleEvent.handleEvent] callback wrapped in [StateMInput].
     */
    fun <S : Any, D, E> startLinkHandleEvent(
        parent: CoroutineScope,
        machine: GenStateMHandleEvent<S, D, E>,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
    ): GenStateMRef<S, D, E> =
        startLink(parent, HandleEventAdapter(machine), context, name)

    private class HandleEventAdapter<S : Any, D, E>(
        private val inner: GenStateMHandleEvent<S, D, E>,
    ) : GenStateM<S, D, E> {
        override suspend fun init(): InitStateMResult<S, D> = inner.init()

        override suspend fun handleEvent(state: S, data: D, event: E): StateMTransition<S, D, E> =
            inner.handleEvent(state, data, StateMInput.User(event))

        override suspend fun handleTimeout(state: S, data: D, kind: StateMTimeoutKind): StateMTransition<S, D, E> =
            inner.handleEvent(state, data, StateMInput.Timeout(kind))
    }

    // ------------------------------------------------------------------
    // Run loop
    // ------------------------------------------------------------------

    private suspend fun <S : Any, D, E> runLoop(
        scope: CoroutineScope,
        machine: GenStateM<S, D, E>,
        mailbox: Channel<GenStateMMsg>,
    ) {
        val initResult = machine.init()
        if (initResult is InitStateMResult.Stop) return

        val init = initResult as InitStateMResult.Ok<S, D>
        var state: S = init.state
        var data: D = init.data

        // Per-kind timeout tracking: job + epoch (epoch invalidates stale TimeoutFired messages).
        var stateTimeoutJob: Job? = null
        var stateTimeoutEpoch = 0L
        var eventTimeoutJob: Job? = null
        var eventTimeoutEpoch = 0L
        val genericTimeoutJobs = mutableMapOf<String, Job>()
        val genericTimeoutEpochs = mutableMapOf<String, Long>()
        var epochSeq = 0L

        // Postponed events (drained before mailbox on each iteration after state change)
        val postponedQueue = ArrayDeque<GenStateMMsg.Event<E>>()
        var postponeActive = false  // true = flush postponed on next state change

        fun nextEpoch() = ++epochSeq

        fun armTimeout(spec: StateMTimeoutSpec) {
            when (val kind = spec.kind) {
                StateMTimeoutKind.StateTimeout -> {
                    stateTimeoutJob?.cancel()
                    val epoch = nextEpoch(); stateTimeoutEpoch = epoch
                    stateTimeoutJob = scope.launch {
                        delay(spec.delay)
                        mailbox.trySend(GenStateMMsg.TimeoutFired(kind, epoch))
                    }
                }
                StateMTimeoutKind.EventTimeout -> {
                    eventTimeoutJob?.cancel()
                    val epoch = nextEpoch(); eventTimeoutEpoch = epoch
                    eventTimeoutJob = scope.launch {
                        delay(spec.delay)
                        mailbox.trySend(GenStateMMsg.TimeoutFired(kind, epoch))
                    }
                }
                is StateMTimeoutKind.GenericTimeout -> {
                    genericTimeoutJobs[kind.name]?.cancel()
                    val epoch = nextEpoch(); genericTimeoutEpochs[kind.name] = epoch
                    genericTimeoutJobs[kind.name] = scope.launch {
                        delay(spec.delay)
                        mailbox.trySend(GenStateMMsg.TimeoutFired(kind, epoch))
                    }
                }
            }
        }

        fun cancelStateTimeout() {
            stateTimeoutJob?.cancel(); stateTimeoutJob = null
            stateTimeoutEpoch = nextEpoch()
        }

        fun cancelEventTimeout() {
            eventTimeoutJob?.cancel(); eventTimeoutJob = null
            eventTimeoutEpoch = nextEpoch()
        }

        // Returns true if should stop. Also handles postpone flag on the current event.
        @Suppress("UNCHECKED_CAST")
        fun applyTransition(
            t: StateMTransition<*, *, *>,
            prevState: S,
            currentEvent: GenStateMMsg.Event<E>?,
        ): Boolean {
            val postpone = when (t) {
                is StateMTransition.Stay<*, *, *> -> t.postpone
                is StateMTransition.Next<*, *, *> -> t.postpone
                is StateMTransition.Stop -> false
            }
            if (postpone && currentEvent != null) {
                postponedQueue.addLast(currentEvent)
            }
            return when (t) {
                is StateMTransition.Stay<*, *, *> -> {
                    data = t.newData as D
                    t.timeout?.let { armTimeout(it) }
                    false
                }
                is StateMTransition.Next<*, *, *> -> {
                    val newSt = t.newState as S
                    val stateChanged = newSt != prevState
                    state = newSt
                    data = t.newData as D
                    if (stateChanged) {
                        cancelStateTimeout()
                        postponeActive = true  // unlock postponed on next iteration
                    }
                    t.timeout?.let { armTimeout(it) }
                    false
                }
                is StateMTransition.Stop -> true
            }
        }

        init.timeout?.let { armTimeout(it) }

        // Fire onEnterState for initial state (prevState = null)
        try {
            val enterT = machine.onEnterState(state, null, data)
            if (applyTransition(enterT, state, null)) return
        } catch (_: CancellationException) { throw CancellationException() }

        try {
            while (scope.isActive) {
                // Drain postponed before reading from mailbox (only after state change)
                val nextMsg: GenStateMMsg = if (postponeActive && postponedQueue.isNotEmpty()) {
                    postponeActive = false
                    postponedQueue.removeFirst()
                } else {
                    mailbox.receiveCatching().getOrNull() ?: break
                }

                when (nextMsg) {
                    is GenStateMMsg.Event<*> -> {
                        cancelEventTimeout()
                        @Suppress("UNCHECKED_CAST")
                        val ev = nextMsg as GenStateMMsg.Event<E>
                        try {
                            val t = machine.handleEvent(state, data, ev.event)
                            val prevState = state
                            val stop = applyTransition(t, prevState, ev)
                            ev.reply?.complete(t)
                            if (stop) break

                            // Fire onEnterState if state changed
                            if (state != prevState) {
                                val enterT = machine.onEnterState(state, prevState, data)
                                // Protect: if onEnterState returns Next to same state, treat as Stay
                                val safeEnterT = if (enterT is StateMTransition.Next<*, *, *> &&
                                    enterT.newState == state) {
                                    @Suppress("UNCHECKED_CAST")
                                    StateMTransition.Stay<S, D, E>(enterT.newData as D, enterT.timeout)
                                } else enterT
                                if (applyTransition(safeEnterT, state, null)) break
                            }
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            ev.reply?.completeExceptionally(t)
                            throw t
                        }
                    }
                    is GenStateMMsg.TimeoutFired -> {
                        // Discard stale firings that arrived after cancel
                        val stale = when (val kind = nextMsg.kind) {
                            StateMTimeoutKind.StateTimeout -> nextMsg.epoch != stateTimeoutEpoch
                            StateMTimeoutKind.EventTimeout -> nextMsg.epoch != eventTimeoutEpoch
                            is StateMTimeoutKind.GenericTimeout ->
                                nextMsg.epoch != (genericTimeoutEpochs[kind.name] ?: -1L)
                        }
                        if (stale) continue
                        when (val kind = nextMsg.kind) {
                            StateMTimeoutKind.StateTimeout -> stateTimeoutJob = null
                            StateMTimeoutKind.EventTimeout -> eventTimeoutJob = null
                            is StateMTimeoutKind.GenericTimeout -> genericTimeoutJobs.remove(kind.name)
                        }
                        try {
                            val t = machine.handleTimeout(state, data, nextMsg.kind)
                            if (applyTransition(t, state, null)) break
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            throw t
                        }
                    }
                    GenStateMMsg.Stop -> break
                }
            }
        } catch (_: CancellationException) {
            throw CancellationException()
        } finally {
            stateTimeoutJob?.cancel()
            eventTimeoutJob?.cancel()
            genericTimeoutJobs.values.forEach { it.cancel() }
        }
    }
}
