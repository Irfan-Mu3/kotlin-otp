package org.otpstudy.poolboy

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InfoMsg
import org.otpstudy.registry.GlobalProcessRegistry
import org.otpstudy.supervisor.DynamicSupervisor
import org.otpstudy.supervisor.DynamicSupervisorRef
import org.otpstudy.supervisor.SimpleOneForOneTemplate
import org.otpstudy.supervisor.SupervisorFlags
import org.otpstudy.supervisor.SupervisorStrategy

/**
 * Caller-supplied factory that starts a single worker [GenServerRef] under [scope].
 *
 * Analogous to poolboy's `worker_module` proplist entry: poolboy stores the module
 * name and `Mod:start_link(WorkerArgs)` is called per worker. Here you write the
 * factory directly. Most factories will look like:
 *
 * ```kotlin
 * val factory = WorkerFactory { scope -> GenServers.startLink(scope, MyWorker(args)) }
 * ```
 *
 * The factory is called from inside the dynamic supervisor's slot coroutine, so the
 * worker's [GenServerRef.job] becomes a child of that slot. When the slot is
 * cancelled (e.g. `dismiss` of an overflow worker, or pool shutdown), the worker job
 * is cancelled too.
 */
fun interface WorkerFactory<W> {
    suspend fun start(scope: CoroutineScope): GenServerRef<W>
}

/**
 * Public handle for a running pool. Mirrors the surface area of poolboy:
 * [`checkout`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L52-L72),
 * [`checkin`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L74-L77),
 * [`transaction`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L79-L91),
 * [`status`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L120-L122),
 * [`stop`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L116-L118).
 *
 * Differences from BEAM that show up in the API (and only here):
 * - [checkout]'s optional [borrower] overrides the caller [Job] inferred from [GenServerRef.call]
 *   ([ReplyHandle.callerJob]); omit it for normal coroutine callers.
 * - [checkout] returns `null` when `block = false` and the pool is full, mirroring
 *   poolboy's `full` atom.
 */
class PoolRef<W> internal constructor(
    internal val ref: GenServerRef<PoolState<W>>,
    private val dynSup: DynamicSupervisorRef,
    private val name: String?,
) {
    private val stopped = AtomicBoolean(false)

    /**
     * Borrow a worker. `block = false` returns `null` instead of waiting if the pool
     * is at capacity (poolboy's `full` atom). [borrower] overrides the caller [Job]
     * inferred from [GenServerRef.call]; pass only for non-standard call paths.
     */
    suspend fun checkout(
        block: Boolean = true,
        timeout: Duration = 5.seconds,
        borrower: Job? = null,
    ): GenServerRef<W>? {
        val cref = nextCheckoutRef()
        val req = PoolRequest.Checkout(cref, block, borrower)
        return try {
            @Suppress("UNCHECKED_CAST")
            ref.call<Any?>(req, timeout) as GenServerRef<W>?
        } catch (t: Throwable) {
            ref.cast(PoolCast.CancelWaiting(cref))
            throw t
        }
    }

    /** Return a worker to the pool (poolboy `checkin/2`). Always asynchronous. */
    fun checkin(worker: GenServerRef<W>) {
        ref.cast(PoolCast.Checkin(worker))
    }

    suspend fun status(): PoolStatus = ref.call(PoolRequest.Status)

    /**
     * Cleanly stop the pool: synchronously stops the gen_server, then shuts down the
     * worker supervisor (which cancels every worker). Idempotent — second and later
     * calls return immediately.
     */
    suspend fun stop() {
        if (!stopped.compareAndSet(false, true)) return
        // NonCancellable: shutdown must complete even if the caller's coroutine is being
        // cancelled (e.g. test scope teardown), otherwise the dyn supervisor leaks.
        withContext(NonCancellable) {
            try {
                ref.call<Unit>(PoolRequest.Stop)
            } catch (_: Throwable) {
            }
            ref.job.join()
            dynSup.shutdown()
            if (name != null) GlobalProcessRegistry.unregister(name, ref)
        }
    }

    companion object {
        private val crefSeq = AtomicLong(0L)
        private fun nextCheckoutRef(): CheckoutRef = crefSeq.incrementAndGet()
    }
}

/**
 * Borrow-then-return-then-rethrow helper. Mirror of poolboy's
 * [`transaction/2,3`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L79-L91).
 *
 * The worker is always checked back in, even if [block] throws. If both [block] and
 * the checkin throw, the checkin failure is added to the primary exception's
 * `suppressed` list (so the original cause is preserved).
 */
suspend fun <W, T> PoolRef<W>.transaction(
    timeout: Duration = 5.seconds,
    block: suspend (GenServerRef<W>) -> T,
): T {
    val worker = checkout(block = true, timeout = timeout)
        ?: error("pool returned null worker for blocking checkout")
    var primary: Throwable? = null
    try {
        return block(worker)
    } catch (t: Throwable) {
        primary = t
        throw t
    } finally {
        try {
            checkin(worker)
        } catch (t: Throwable) {
            if (primary == null) throw t else primary.addSuppressed(t)
        }
    }
}

/**
 * Construct and start a pool plus its worker supervisor under [parent].
 *
 * Mirrors [`poolboy:start_link/1,2`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L137-L150).
 *
 * Lifecycle:
 * - The dynamic supervisor and the pool gen_server are both launched as children of [parent].
 * - This call is `suspend` and **awaits the gen_server's `init`** (via
 *   [GenServers.startLinkSync]) — when it returns, all [PoolConfig.size] workers are spawned
 *   and the pool is ready for [PoolRef.checkout].
 * - If `init` throws (e.g. the worker factory raises during pre-population), the dynamic
 *   supervisor is shut down and the registered name is released before the failure
 *   propagates — no orphaned children, no leaked registrations.
 * - Otherwise the caller is responsible for either calling [PoolRef.stop] or cancelling
 *   [parent]; both tear everything down in the right order.
 */
object Poolboy {
    suspend fun <W> startLink(
        parent: CoroutineScope,
        config: PoolConfig,
        factory: WorkerFactory<W>,
        context: CoroutineContext = Dispatchers.Default,
    ): PoolRef<W> {
        val deliver = AtomicReference<((InfoMsg) -> Unit)?>(null)

        val template =
            SimpleOneForOneTemplate<SpawnedWorker<W>>(
                restart = Restart.Temporary,
                shutdown = Shutdown.Timeout(5.seconds),
                start = { scope, childId, ready ->
                    val ref = factory.start(scope)
                    ref.job.invokeOnCompletion {
                        deliver.get()?.invoke(WorkerDown(ref))
                    }
                    ready(SpawnedWorker(childId, ref))
                    ref.job.join()
                },
            )

        val dynSup =
            DynamicSupervisor.startLink(
                parent = parent,
                flags =
                    SupervisorFlags(
                        strategy = SupervisorStrategy.OneForOne,
                        intensity = Int.MAX_VALUE,
                        period = 60.seconds,
                    ),
                template = template,
                context = context,
            )

        val workerHandler =
            object : WorkerHandler<W> {
                override suspend fun spawn(): SpawnedWorker<W> =
                    dynSup.startChildSync<SpawnedWorker<W>>().second

                override suspend fun dismiss(spawned: SpawnedWorker<W>) {
                    dynSup.terminateChild(spawned.childId)
                }
            }

        val genServer = PoolGenServer(config, workerHandler, deliver)

        val ref =
            try {
                GenServers.startLinkSync(
                    parent = parent,
                    server = genServer,
                    context = context,
                    name = config.name,
                )
            } catch (t: Throwable) {
                // Roll back the dyn supervisor (and any partially-spawned workers) if
                // init fails or is cancelled, then rethrow so the caller sees the failure.
                withContext(NonCancellable) { dynSup.shutdown() }
                throw t
            }

        if (config.name != null) {
            try {
                GlobalProcessRegistry.register(config.name, ref)
            } catch (t: Throwable) {
                // Registry collision (duplicate name) or similar: tear everything down so
                // the caller doesn't get a half-alive PoolRef they can't stop cleanly.
                withContext(NonCancellable) {
                    try { ref.stop() } catch (_: Throwable) {}
                    dynSup.shutdown()
                }
                throw t
            }
        }

        return PoolRef(ref, dynSup, config.name)
    }
}
