package org.otpstudy.genevent

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext

// ---------------------------------------------------------------------------
// Back-pressure policy
// ---------------------------------------------------------------------------

/**
 * Controls what happens when the event manager's mailbox is full (only relevant
 * for bounded-capacity policies).
 *
 * - [Block]: [GenEventManagerRef.notify] suspends until the coordinator processes the event.
 *   All handlers are notified synchronously before `notify` returns. The mailbox is
 *   effectively unbounded.
 * - [DropOldest]: bounded mailbox of [capacity]; when full, the oldest pending event
 *   is dropped and the new one is accepted.
 * - [DropNew]: bounded mailbox of [capacity]; when full, the new event is silently dropped.
 *
 * JVM/OTP difference: OTP `gen_event` has no built-in back-pressure; this is a library extension.
 * Handler ordering is always preserved (insertion order). There is no per-handler buffer.
 */
sealed class BackPressurePolicy {
    /** Synchronous delivery: `notify` suspends until all handlers have been called. */
    data object Block : BackPressurePolicy()

    /** Drop the oldest pending event when the internal buffer is full. */
    data class DropOldest(val capacity: Int) : BackPressurePolicy()

    /** Drop the incoming event when the internal buffer is full. */
    data class DropNew(val capacity: Int) : BackPressurePolicy()
}

// ---------------------------------------------------------------------------
// Handler interface
// ---------------------------------------------------------------------------

/**
 * OTP [`gen_event`](https://www.erlang.org/doc/design_principles/events.html) handler analogue.
 *
 * Handlers are called in insertion order for each event. [terminate] is called
 * when the handler is deleted or the manager shuts down.
 */
interface GenEventHandler<E> {
    suspend fun handleEvent(event: E)
    suspend fun terminate() {}
}

// ---------------------------------------------------------------------------
// Internal protocol
// ---------------------------------------------------------------------------

internal sealed class GenEventMsg<out E> {
    data class Notify<E>(val event: E, val done: CompletableDeferred<Unit>?) : GenEventMsg<E>()
    data class AddHandler<E>(val handler: GenEventHandler<E>, val done: CompletableDeferred<Unit>) : GenEventMsg<E>()
    data class RemoveHandler<E>(val handler: GenEventHandler<E>, val done: CompletableDeferred<Unit>) : GenEventMsg<E>()
    data class Shutdown(val done: CompletableDeferred<Unit>) : GenEventMsg<Nothing>()
}

// ---------------------------------------------------------------------------
// Manager ref
// ---------------------------------------------------------------------------

/**
 * Reference to a running [GenEventManager] process.
 */
class GenEventManagerRef<E> internal constructor(
    val job: Job,
    private val mailbox: Channel<GenEventMsg<E>>,
    private val policy: BackPressurePolicy,
) {
    /** Add a handler. Returns once the manager has registered it. */
    suspend fun addHandler(handler: GenEventHandler<E>) {
        val done = CompletableDeferred<Unit>()
        mailbox.send(GenEventMsg.AddHandler(handler, done))
        done.await()
    }

    /** Remove a handler (calls [GenEventHandler.terminate]). Returns once removed. */
    suspend fun deleteHandler(handler: GenEventHandler<E>) {
        val done = CompletableDeferred<Unit>()
        mailbox.send(GenEventMsg.RemoveHandler(handler, done))
        done.await()
    }

    /**
     * Notify all registered handlers of [event].
     *
     * For [BackPressurePolicy.Block]: suspends until all handlers have processed the event.
     * For [BackPressurePolicy.DropOldest] / [BackPressurePolicy.DropNew]: fire-and-forget
     * (the event may be silently dropped if the buffer is full).
     */
    suspend fun notify(event: E) {
        when (policy) {
            BackPressurePolicy.Block -> {
                val done = CompletableDeferred<Unit>()
                mailbox.send(GenEventMsg.Notify(event, done))
                done.await()
            }
            is BackPressurePolicy.DropOldest, is BackPressurePolicy.DropNew -> {
                mailbox.trySend(GenEventMsg.Notify(event, null))
            }
        }
    }

    /** Shut down the event manager, calling [GenEventHandler.terminate] on each handler. */
    suspend fun shutdown() {
        val done = CompletableDeferred<Unit>()
        val sent = mailbox.trySend(GenEventMsg.Shutdown(done))
        if (sent.isSuccess) done.await()
        job.join()
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

object GenEventManagers {
    /**
     * Starts an event manager under [parent].
     *
     * Handlers are notified in **insertion order** (synchronous per-event delivery
     * within the coordinator coroutine). For isolated per-handler concurrency,
     * dispatch from within [GenEventHandler.handleEvent] to a separate channel.
     */
    fun <E> startLink(
        parent: CoroutineScope,
        policy: BackPressurePolicy = BackPressurePolicy.Block,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
    ): GenEventManagerRef<E> {
        val mailbox: Channel<GenEventMsg<E>> = when (policy) {
            BackPressurePolicy.Block ->
                Channel(Channel.UNLIMITED)
            is BackPressurePolicy.DropOldest ->
                Channel(policy.capacity, BufferOverflow.DROP_OLDEST)
            is BackPressurePolicy.DropNew ->
                // DROP_LATEST = drop the new item when full (closest to DROP_NEW semantics)
                Channel(policy.capacity, BufferOverflow.DROP_LATEST)
        }

        val jobName = name?.let { CoroutineName("gen_event:$it") } ?: CoroutineName("gen_event")
        val job = parent.launch(context + jobName) {
            runLoop(mailbox)
        }
        return GenEventManagerRef(job, mailbox, policy)
    }

    private suspend fun <E> runLoop(mailbox: Channel<GenEventMsg<E>>) {
        val handlers = mutableListOf<GenEventHandler<E>>()

        try {
            for (msg in mailbox) {
                when (msg) {
                    is GenEventMsg.Shutdown -> {
                        for (h in handlers) runCatching { h.terminate() }
                        handlers.clear()
                        msg.done.complete(Unit)
                        return
                    }
                    is GenEventMsg.AddHandler -> {
                        handlers.add(msg.handler)
                        msg.done.complete(Unit)
                    }
                    is GenEventMsg.RemoveHandler -> {
                        val removed = handlers.remove(msg.handler)
                        if (removed) runCatching { msg.handler.terminate() }
                        msg.done.complete(Unit)
                    }
                    is GenEventMsg.Notify -> {
                        for (h in handlers.toList()) {
                            runCatching { h.handleEvent(msg.event) }
                        }
                        msg.done?.complete(Unit)
                    }
                }
            }
        } catch (_: CancellationException) {
            throw CancellationException()
        }
    }
}
