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
import org.otpstudy.genserver.ServerDownException
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeTransport
import org.otpstudy.registry.GlobalProcessRegistry
import org.otpstudy.distribution.RemoteGenServerRef
import org.otpstudy.global.GlobalRegistry
import org.otpstudy.registry.GlobalProcessRegistryResolver
import org.otpstudy.registry.ProcessResolver
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
    private val dynSup: DynamicSupervisorRef?,
    private val name: String?,
    private val homeNode: LocalNode? = null,
) : PoolHandle<W> {
    override val poolName: String? get() = name
    private val stopped = AtomicBoolean(false)

    /**
     * Borrow a worker. `block = false` returns `null` instead of waiting if the pool
     * is at capacity (poolboy's `full` atom). [borrower] overrides the caller [Job]
     * inferred from [GenServerRef.call]; pass only for non-standard call paths.
     *
     * @throws PoolStoppedException if the pool's gen_server has stopped (e.g. via
     *   [stop] or because the parent supervisor tore it down). Maps the underlying
     *   [ServerDownException] so callers don't have to depend on kotlin-otp internals.
     */
    override suspend fun checkout(
        block: Boolean,
        timeout: Duration,
        borrower: Job?,
    ): PooledWorker<W>? {
        val cref = nextCheckoutRef()
        val req = PoolRequest.Checkout(cref, block, borrower)
        return try {
            @Suppress("UNCHECKED_CAST")
            val workerRef = ref.call<Any?>(req, timeout) as GenServerRef<W>?
            workerRef?.let { LocalPooledWorker(it, ::checkinWorker) }
        } catch (t: ServerDownException) {
            throw PoolStoppedException(name, t)
        } catch (t: Throwable) {
            ref.cast(PoolCast.CancelWaiting(cref))
            throw t
        }
    }

    override fun checkin(worker: PooledWorker<W>) {
        when (worker) {
            is LocalPooledWorker -> checkinWorker(worker.ref)
            else -> worker.checkin()
        }
    }

    private fun checkinWorker(worker: GenServerRef<W>) {
        ref.cast(PoolCast.Checkin(worker))
    }

    /**
     * Snapshot of pool capacity / load (poolboy `status/1`).
     *
     * @throws PoolStoppedException if the pool's gen_server has stopped.
     */
    override suspend fun status(): PoolStatus =
        try {
            ref.call(PoolRequest.Status)
        } catch (t: ServerDownException) {
            throw PoolStoppedException(name, t)
        }

    /**
     * Cleanly stop the pool: synchronously stops the gen_server, then shuts down the
     * worker supervisor (which cancels every worker). Idempotent — second and later
     * calls return immediately.
     */
    override suspend fun stop() {
        if (!stopped.compareAndSet(false, true)) return
        // NonCancellable: shutdown must complete even if the caller's coroutine is being
        // cancelled (e.g. test scope teardown), otherwise the dyn supervisor leaks.
        withContext(NonCancellable) {
            try {
                ref.call<Unit>(PoolRequest.Stop)
            } catch (_: Throwable) {
            }
            ref.job.join()
            dynSup?.shutdown()
            if (name != null) {
                GlobalProcessRegistry.unregister(name, ref)
                homeNode?.unregister(name)
            }
        }
    }

    companion object {
        private val crefSeq = AtomicLong(0L)
        private fun nextCheckoutRef(): CheckoutRef = crefSeq.incrementAndGet()
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
    /**
     * Resolve a [PoolAddress] to a [PoolHandle]. [PoolAddress.OnNode] requires [transport].
     *
     * Local / global / via addresses wrap the registered pool gen_server; [stop] on such a
     * handle stops only the pool actor (not the dynamic supervisor — use the [PoolRef]
     * returned from [startLink] for full teardown).
     */
    fun <W> resolve(
        address: PoolAddress,
        transport: NodeTransport? = null,
    ): PoolHandle<W> =
        when (address) {
            is PoolAddress.Local ->
                localHandle(GlobalProcessRegistryResolver, address.name)
            is PoolAddress.Global ->
                globalHandle(address.name, transport)
            is PoolAddress.Via ->
                localHandle(address.registry, address.name)
            is PoolAddress.OnNode -> {
                val t =
                    transport
                        ?: error("NodeTransport required for PoolAddress.OnNode")
                RemotePoolHandle(t, address.node, address.name)
            }
        }

  @Suppress("UNCHECKED_CAST")
  private fun <W> localHandle(
    resolver: ProcessResolver,
    name: String,
  ): PoolHandle<W> {
    val ref =
      resolver.lookup(name) as? GenServerRef<PoolState<W>>
        ?: error("no pool registered as '$name'")
    return PoolRef(ref, dynSup = null, name = name, homeNode = null)
  }

  private fun <W> globalHandle(
    name: String,
    transport: NodeTransport?,
  ): PoolHandle<W> =
    when (val resolved = GlobalRegistry.resolveName(name)) {
      is GlobalRegistry.NameResolution.LocalRef<*> -> {
        @Suppress("UNCHECKED_CAST")
        PoolRef(resolved.ref as GenServerRef<PoolState<W>>, dynSup = null, name = name, homeNode = null)
      }
      is GlobalRegistry.NameResolution.RemoteRef -> {
        val t =
          transport
            ?: error("NodeTransport required for remote global pool '$name'")
        RemotePoolHandle(t, resolved.stub.homeNode, resolved.stub.localName)
      }
      null -> error("no pool registered globally as '$name'")
    }

    suspend fun <W> checkout(
        address: PoolAddress,
        block: Boolean = true,
        timeout: Duration = 5.seconds,
        borrower: Job? = null,
        transport: NodeTransport? = null,
    ): PooledWorker<W>? = resolve<W>(address, transport).checkout(block, timeout, borrower)

    fun <W> checkin(
        address: PoolAddress,
        worker: PooledWorker<W>,
        transport: NodeTransport? = null,
    ) {
        resolve<W>(address, transport).checkin(worker)
    }

    suspend fun <W> status(
        address: PoolAddress,
        transport: NodeTransport? = null,
    ): PoolStatus = resolve<W>(address, transport).status()

    suspend fun <W, T> transaction(
        address: PoolAddress,
        timeout: Duration = 5.seconds,
        transport: NodeTransport? = null,
        block: suspend (PooledWorker<W>) -> T,
    ): T = resolve<W>(address, transport).transaction(timeout, block)

    suspend fun <W> startLink(
        parent: CoroutineScope,
        config: PoolConfig,
        factory: WorkerFactory<W>,
        context: CoroutineContext = Dispatchers.Default,
        homeNode: LocalNode? = null,
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
                homeNode?.register(config.name, ref)
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

        return PoolRef(ref, dynSup, config.name, homeNode)
    }
}
