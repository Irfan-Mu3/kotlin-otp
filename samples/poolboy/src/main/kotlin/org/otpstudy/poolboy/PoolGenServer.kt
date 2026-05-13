package org.otpstudy.poolboy

import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import org.otpstudy.core.OtpLogContext
import org.otpstudy.core.OtpLogLevel
import org.otpstudy.core.OtpLogging
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.InfoMsg
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyHandle
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.TerminateReason
import org.otpstudy.supervisor.DynamicSupervisorRef

/**
 * Internal contract: how the pool gets new workers and dismisses overflow ones.
 * Implemented by [Poolboy] which owns the [DynamicSupervisorRef].
 *
 * The handler also owns hook installation on the worker's [Job]: every spawn
 * registers a [Job.invokeOnCompletion] that posts [WorkerDown] via [deliver] (wired in
 * [init] to [GenServerRef.sendInfo]).
 */
internal interface WorkerHandler<W> {
    /** Spawn a fresh worker via the dynamic supervisor and wait for its [GenServerRef]. */
    suspend fun spawn(): SpawnedWorker<W>

    /** Dismiss an overflow worker (`supervisor:terminate_child`). */
    suspend fun dismiss(spawned: SpawnedWorker<W>)
}

/** Worker freshly produced by the dynamic supervisor template. */
internal data class SpawnedWorker<W>(val childId: String, val ref: GenServerRef<W>)

/** Per-borrower bookkeeping for a checked-out worker (poolboy `monitors` ETS row). */
internal data class MonitorEntry<W>(
    val cref: CheckoutRef,
    val borrower: Job,
    val borrowerHook: DisposableHandle,
    val spawned: SpawnedWorker<W>,
)

/** Per-blocked-checkout bookkeeping (poolboy `waiting` queue entry). */
internal data class WaitingItem<W>(
    val cref: CheckoutRef,
    val borrower: Job,
    val borrowerHook: DisposableHandle,
    val from: ReplyHandle<PoolState<W>>,
)

/**
 * Mutable pool state. Mirrors the `#state{}` record in
 * [poolboy.erl](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L42-L51).
 */
internal class PoolState<W>(val config: PoolConfig) {
    /** Idle workers (poolboy `workers :: pid_queue()`). FIFO/LIFO is applied at checkout. */
    val available: ArrayDeque<SpawnedWorker<W>> = ArrayDeque()

    /** Blocked checkouts (poolboy `waiting :: pid_queue()`). */
    val waiting: ArrayDeque<WaitingItem<W>> = ArrayDeque()

    /** Checked-out workers, keyed by ref (poolboy ETS `monitors` table). */
    val monitors: MutableMap<GenServerRef<W>, MonitorEntry<W>> = LinkedHashMap()

    /**
     * Secondary index from [CheckoutRef] to the borrowed worker, kept in sync with
     * [monitors]. Lets [BorrowerDown] / cancel_waiting paths do O(1) lookups instead
     * of scanning [monitors] for a matching cref. Mirrors poolboy's ETS `monitors`
     * table key (which is the cref ref).
     */
    val crefIndex: MutableMap<CheckoutRef, GenServerRef<W>> = HashMap()

    /** Currently active overflow workers (poolboy `overflow :: non_neg_integer()`). */
    var overflow: Int = 0

    /** Spawned workers we've asked the supervisor to dismiss; their pending [WorkerDown] is ignored. */
    val dismissing: MutableSet<GenServerRef<W>> = HashSet()

    /** Returns the pool's `state_name` per poolboy `state_name/1`. */
    fun stateName(): PoolStateName =
        if (overflow < 1) {
            when {
                available.isEmpty() && config.maxOverflow < 1 -> PoolStateName.Full
                available.isEmpty() -> PoolStateName.Overflow
                else -> PoolStateName.Ready
            }
        } else if (overflow == config.maxOverflow) {
            PoolStateName.Full
        } else {
            PoolStateName.Overflow
        }

    fun status(): PoolStatus =
        PoolStatus(
            state = stateName(),
            available = available.size,
            overflow = overflow,
            monitors = monitors.size,
        )
}

/**
 * Faithful Kotlin port of [`poolboy.erl`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl).
 *
 * Mapping summary (see also `samples/poolboy/README.md`):
 * - `process_flag(trap_exit, true)` -> [trapExit] = true. We additionally watch worker exits
 *   via [Job.invokeOnCompletion] hooks (installed by [Poolboy] inside [WorkerHandler.spawn]),
 *   producing [WorkerDown] messages for the actor.
 * - ETS `monitors` table -> in-actor [PoolState.monitors] map (single-threaded, no need for
 *   [org.otpstudy.ets.OtpTable]).
 * - `gen_server:reply(From, Pid)` -> [ReplyResult.DeferReply] + [ReplyHandle.reply].
 * - `erlang:monitor(process, FromPid)` -> [Job.invokeOnCompletion] hook on the borrower's job
 *   that posts [BorrowerDown] (scoped by checkout [CheckoutRef]) back to the pool.
 * - Hooks post [BorrowerDown] / [WorkerDown] via [deliver], set in [init] to [GenServerRef.sendInfo].
 */
internal class PoolGenServer<W>(
    private val config: PoolConfig,
    private val workerHandler: WorkerHandler<W>,
    private val deliver: AtomicReference<((InfoMsg) -> Unit)?>,
) : GenServer<PoolState<W>> {
    override val trapExit: Boolean get() = true

    override suspend fun init(self: GenServerRef<PoolState<W>>): InitResult<PoolState<W>> {
        deliver.set(self::sendInfo)
        val state = PoolState<W>(config)
        repeat(config.size) {
            val spawned = workerHandler.spawn()
            state.available.addLast(spawned)
        }
        return InitResult.Ok(state)
    }

    override suspend fun handleCall(
        request: Any,
        state: PoolState<W>,
    ): ReplyResult<PoolState<W>> = error("use handleCallFrom")

    override suspend fun handleCallFrom(
        request: Any,
        state: PoolState<W>,
        from: ReplyHandle<PoolState<W>>,
    ): ReplyResult<PoolState<W>> =
        when (request) {
            is PoolRequest.Checkout -> handleCheckout(request, state, from)
            PoolRequest.Status -> ReplyResult.Reply(state.status(), state)
            PoolRequest.Stop -> ReplyResult.Stop(Unit, TerminateReason.Normal, state)
            else -> {
                // Don't crash the pool over a stray message (matches OTP's general
                // gen_server discipline of {reply, {error, invalid_message}, State}).
                // Reply with null so the caller's `call` returns rather than timing out.
                logUnknown("call", request)
                ReplyResult.Reply(null, state)
            }
        }

    override suspend fun handleCast(
        request: Any,
        state: PoolState<W>,
    ): NoreplyResult<PoolState<W>> {
        when (request) {
            is PoolCast.Checkin -> {
                @Suppress("UNCHECKED_CAST")
                handleReturnedWorker(request.worker as GenServerRef<W>, state)
            }
            is PoolCast.CancelWaiting -> handleCancelWaiting(request.cref, state)
            else -> logUnknown("cast", request)
        }
        return NoreplyResult.Noreply(state)
    }

    override suspend fun handleInfo(
        msg: InfoMsg,
        state: PoolState<W>,
    ): NoreplyResult<PoolState<W>> {
        when (msg) {
            is BorrowerDown -> handleBorrowerDown(msg.cref, state)
            is WorkerDown -> {
                @Suppress("UNCHECKED_CAST")
                handleWorkerDown(msg.worker as GenServerRef<W>, state)
            }
            else -> { }
        }
        return NoreplyResult.Noreply(state)
    }

    override suspend fun terminate(reason: TerminateReason, state: PoolState<W>) {
        for (item in state.waiting) {
            item.borrowerHook.dispose()
            item.from.reply(null)
        }
        state.waiting.clear()
        for (entry in state.monitors.values) {
            entry.borrowerHook.dispose()
        }
        state.monitors.clear()
        state.crefIndex.clear()
        state.available.clear()
    }

    private suspend fun handleCheckout(
        req: PoolRequest.Checkout,
        state: PoolState<W>,
        from: ReplyHandle<PoolState<W>>,
    ): ReplyResult<PoolState<W>> {
        val borrowerJob = req.borrower ?: from.callerJob
            ?: error("checkout requires a borrower Job (implicit from GenServerRef.call or explicit)")
        val taken = takeAvailable(state)
        if (taken != null) {
            installMonitor(state, taken, req.cref, borrowerJob)
            return ReplyResult.Reply(taken.ref, state)
        }
        if (state.overflow < config.maxOverflow) {
            val spawned = workerHandler.spawn()
            state.overflow += 1
            installMonitor(state, spawned, req.cref, borrowerJob)
            return ReplyResult.Reply(spawned.ref, state)
        }
        if (!req.block) {
            return ReplyResult.Reply(null, state)
        }
        val hook = installBorrowerHook(req.cref, borrowerJob)
        state.waiting.addLast(WaitingItem(req.cref, borrowerJob, hook, from))
        return ReplyResult.DeferReply(from, state)
    }

    private suspend fun handleReturnedWorker(worker: GenServerRef<W>, state: PoolState<W>) {
        val entry = state.monitors.remove(worker)
        if (entry == null) {
            // Foreign / duplicate / late checkin. Two real causes:
            //   (1) caller checked in a worker from a *different* pool — buggy app code;
            //   (2) caller checked in the same worker twice.
            // In both cases the right response is "do nothing" — touching `available`
            // would silently corrupt this pool's accounting. Log and move on.
            logForeignCheckin(worker)
            return
        }
        state.crefIndex.remove(entry.cref)
        entry.borrowerHook.dispose()
        handleAvailableSlot(entry.spawned, state)
    }

    private suspend fun handleBorrowerDown(cref: CheckoutRef, state: PoolState<W>) {
        val worker = state.crefIndex.remove(cref)
        if (worker != null) {
            val entry = state.monitors.remove(worker) ?: return
            entry.borrowerHook.dispose()
            handleAvailableSlot(entry.spawned, state)
            return
        }
        val toRemove = state.waiting.filter { it.cref == cref }
        if (toRemove.isEmpty()) return
        state.waiting.removeAll(toRemove.toSet())
        for (item in toRemove) item.borrowerHook.dispose()
    }

    private suspend fun handleWorkerDown(worker: GenServerRef<W>, state: PoolState<W>) {
        if (state.dismissing.remove(worker)) return
        val checkedOut = state.monitors.remove(worker)
        if (checkedOut != null) {
            state.crefIndex.remove(checkedOut.cref)
            checkedOut.borrowerHook.dispose()
            replaceWithNewWorkerOrShrink(state)
            return
        }
        val wasIdleIdx = state.available.indexOfFirst { it.ref === worker }
        if (wasIdleIdx >= 0) {
            state.available.removeAt(wasIdleIdx)
            val replacement = workerHandler.spawn()
            state.available.addLast(replacement)
        }
    }

    private suspend fun replaceWithNewWorkerOrShrink(state: PoolState<W>) {
        val pending = state.waiting.removeFirstOrNull()
        if (pending != null) {
            val replacement = workerHandler.spawn()
            state.monitors[replacement.ref] =
                MonitorEntry(pending.cref, pending.borrower, pending.borrowerHook, replacement)
            state.crefIndex[pending.cref] = replacement.ref
            pending.from.reply(replacement.ref)
            return
        }
        if (state.overflow > 0) {
            state.overflow -= 1
            return
        }
        val replacement = workerHandler.spawn()
        state.available.addLast(replacement)
    }

    private suspend fun handleCancelWaiting(cref: CheckoutRef, state: PoolState<W>) {
        val worker = state.crefIndex.remove(cref)
        if (worker != null) {
            val entry = state.monitors.remove(worker) ?: return
            entry.borrowerHook.dispose()
            handleAvailableSlot(entry.spawned, state)
            return
        }
        val toRemove = state.waiting.filter { it.cref == cref }
        if (toRemove.isEmpty()) return
        state.waiting.removeAll(toRemove.toSet())
        for (item in toRemove) item.borrowerHook.dispose()
    }

    /**
     * Mirror of poolboy `handle_checkin/2`: route a freshly-available worker either to
     * the next waiting caller, or dismiss it if we're over capacity, or queue it for re-use.
     */
    private suspend fun handleAvailableSlot(spawned: SpawnedWorker<W>, state: PoolState<W>) {
        check(spawned.ref !in state.monitors) {
            "internal bug: returning checked-out worker ${spawned.ref.id} to available (monitors=${state.monitors.keys.map { it.id }})"
        }
        val pending = state.waiting.removeFirstOrNull()
        if (pending != null) {
            state.monitors[spawned.ref] =
                MonitorEntry(pending.cref, pending.borrower, pending.borrowerHook, spawned)
            state.crefIndex[pending.cref] = spawned.ref
            pending.from.reply(spawned.ref)
            return
        }
        if (state.overflow > 0) {
            state.dismissing.add(spawned.ref)
            workerHandler.dismiss(spawned)
            state.overflow -= 1
            return
        }
        state.available.addLast(spawned)
    }

    private fun takeAvailable(state: PoolState<W>): SpawnedWorker<W>? =
        when (config.strategy) {
            PoolConfig.Strategy.Lifo -> state.available.removeLastOrNull()
            PoolConfig.Strategy.Fifo -> state.available.removeFirstOrNull()
        }

    private fun installMonitor(
        state: PoolState<W>,
        spawned: SpawnedWorker<W>,
        cref: CheckoutRef,
        borrower: Job,
    ) {
        val hook = installBorrowerHook(cref, borrower)
        state.monitors[spawned.ref] = MonitorEntry(cref, borrower, hook, spawned)
        state.crefIndex[cref] = spawned.ref
    }

    /**
     * Install a one-shot hook that posts [BorrowerDown] (scoped by [cref]) when the
     * borrower's [Job] completes. Each checkout uses a unique [cref] so multiple workers
     * borrowed under the same [Job] remain independent.
     * Returns the [DisposableHandle] so we can cancel it on a clean checkin.
     */
    private fun installBorrowerHook(cref: CheckoutRef, borrower: Job): DisposableHandle =
        borrower.invokeOnCompletion {
            deliver.get()?.invoke(BorrowerDown(cref))
        }

    private fun logUnknown(kind: String, request: Any) {
        OtpLogging.log(
            level = OtpLogLevel.Warn,
            ctx = OtpLogContext(component = "poolboy", tag = config.name),
            message = "ignoring unknown $kind: ${request::class.qualifiedName ?: request::class.simpleName} ($request)",
        )
    }

    private fun logForeignCheckin(worker: GenServerRef<W>) {
        OtpLogging.log(
            level = OtpLogLevel.Warn,
            ctx = OtpLogContext(component = "poolboy", tag = config.name),
            message = "ignoring checkin of unknown worker ${worker.id} (not borrowed from this pool, or already checked in)",
        )
    }
}
