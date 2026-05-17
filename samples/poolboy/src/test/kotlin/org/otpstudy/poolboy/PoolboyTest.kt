package org.otpstudy.poolboy

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicInteger
import org.otpstudy.application.ApplicationEnv
import org.otpstudy.application.ApplicationStartResult
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Timeout
import org.otpstudy.distribution.InMemoryTransport
import org.otpstudy.distribution.KotlinNodeTransport
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.registry.ProcessRegistry
import kotlinx.coroutines.cancelChildren
import org.otpstudy.global.GlobalRegistry
import org.otpstudy.genserver.GenServer
import org.otpstudy.registry.MapViaRegistry
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.poolboy.example.ExampleApp
import org.otpstudy.poolboy.example.PoolDef
import org.otpstudy.poolboy.example.SqlResult
import org.otpstudy.poolboy.example.WorkerArgs
import org.otpstudy.poolboy.example.WorkerRequest

data class CounterState(var calls: Int = 0)

/**
 * Minimal worker for non-SQL tests: just counts calls. Avoids pulling H2 into tests
 * that only exercise the pool's own behaviour.
 */
class TestWorker : GenServer<CounterState> {
    override suspend fun init(self: GenServerRef<CounterState>): InitResult<CounterState> = InitResult.Ok(CounterState())

    override suspend fun handleCall(
        request: Any,
        state: CounterState,
    ): ReplyResult<CounterState> {
        state.calls += 1
        return ReplyResult.Reply(state.calls, state)
    }

    override suspend fun handleCast(
        request: Any,
        state: CounterState,
    ): NoreplyResult<CounterState> = NoreplyResult.Noreply(state)
}

fun testFactory(): WorkerFactory<CounterState> =
    WorkerFactory { scope -> GenServers.startLink(scope, TestWorker()) }

@Suppress("UNCHECKED_CAST")
private fun <W> PooledWorker<W>.localRef(): GenServerRef<W> = (this as LocalPooledWorker<W>).ref

/**
 * Worker whose [init] sleeps [initDelay], used to demonstrate parallel pool
 * pre-population (round 3). The factory below uses [GenServers.startLinkSync] so
 * the spawn waits for [init] — that's the only configuration where parallel
 * `PoolGenServer.init` can show a wall-clock improvement (with non-suspending
 * factories, the worker's init runs asynchronously after spawn returns).
 */
class SlowInitWorker(private val initDelay: Duration) : GenServer<CounterState> {
    override suspend fun init(self: GenServerRef<CounterState>): InitResult<CounterState> {
        delay(initDelay)
        return InitResult.Ok(CounterState())
    }

    override suspend fun handleCall(request: Any, state: CounterState): ReplyResult<CounterState> =
        ReplyResult.Reply(Unit, state)

    override suspend fun handleCast(request: Any, state: CounterState): NoreplyResult<CounterState> =
        NoreplyResult.Noreply(state)
}

fun slowFactory(initDelay: Duration): WorkerFactory<CounterState> =
    WorkerFactory { scope -> GenServers.startLinkSync(scope, SlowInitWorker(initDelay)) }

/**
 * Poll the pool's status until [predicate] holds or [timeout] elapses. Tests use this
 * for assertions whose truth depends on async hook propagation through the
 * pool actor mailbox (e.g. [WorkerDown] / [BorrowerDown]).
 */
private suspend fun <W> PoolRef<W>.awaitStatus(
    timeout: Duration = 2.seconds,
    predicate: (PoolStatus) -> Boolean,
): PoolStatus {
    val deadline = System.nanoTime() + timeout.inWholeNanoseconds
    while (System.nanoTime() < deadline) {
        val s = status()
        if (predicate(s)) return s
        delay(20)
    }
    return status()
}

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class PoolboyTest {
    @Test
    fun pool_wire_roundtrip(): Unit =
        runBlocking {
            val req: WirePoolRequest = WirePoolRequest.Checkout(cref = 1L, block = true, borrowerPresent = false)
            val el = org.otpstudy.distribution.DistributionWire.encodeSerializable(req)
            val back = org.otpstudy.distribution.DistributionWire.decodeSerializable<WirePoolRequest>(el)
            assertEquals(req, back)
            val result: WireCheckoutResult = WireCheckoutResult.Worker(WorkerToken(42L, 1L))
            val rel = org.otpstudy.distribution.DistributionWire.encodeSerializable(result)
            val r2 = org.otpstudy.distribution.DistributionWire.decodeSerializable<WireCheckoutResult>(rel)
            assertEquals(result, r2)
            val fwd: WirePoolRequest =
                WirePoolRequest.ForwardCall(
                    7L,
                    42L,
                    org.otpstudy.distribution.DistributionWire.encodePayload(Unit),
                )
            val fwdBack =
                org.otpstudy.distribution.DistributionWire.decodeSerializable<WirePoolRequest>(
                    org.otpstudy.distribution.DistributionWire.encodeSerializable(fwd),
                )
            assertEquals(fwd, fwdBack)
        }

    @Test
    fun pool_startup_size() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 5, maxOverflow = 0), testFactory())
            try {
                assertEquals(PoolStatus(PoolStateName.Ready, 5, 0, 0), pool.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun checkout_returns_distinct_workers() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 3, maxOverflow = 0), testFactory())
            try {
                val w1 = pool.checkout()!!
                val w2 = pool.checkout()!!
                val w3 = pool.checkout()!!
                assertNotEquals(w1, w2)
                assertNotEquals(w2, w3)
                assertNotEquals(w1, w3)
                assertEquals(3, pool.status().monitors)

                val pending: Job =
                    launch {
                        val w4 = pool.checkout(block = true, timeout = 5.seconds)
                        assertNotNull(w4)
                    }
                delay(50)
                assertTrue(pending.isActive)
                pool.checkin(w1)
                pending.join()
            } finally {
                pool.stop()
            }
        }

    @Test
    fun lifo_strategy() =
        runBlocking {
            val pool =
                Poolboy.startLink(
                    this,
                    PoolConfig(size = 3, maxOverflow = 0, strategy = PoolConfig.Strategy.Lifo),
                    testFactory(),
                )
            try {
                val w1 = pool.checkout()!!
                val w2 = pool.checkout()!!
                val w3 = pool.checkout()!!
                pool.checkin(w1)
                pool.checkin(w2)
                pool.checkin(w3)
                pool.awaitStatus { it.available == 3 }
                assertEquals(w3.localRef(), pool.checkout()!!.localRef())
                assertEquals(w2.localRef(), pool.checkout()!!.localRef())
                assertEquals(w1.localRef(), pool.checkout()!!.localRef())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun fifo_strategy() =
        runBlocking {
            val pool =
                Poolboy.startLink(
                    this,
                    PoolConfig(size = 3, maxOverflow = 0, strategy = PoolConfig.Strategy.Fifo),
                    testFactory(),
                )
            try {
                val w1 = pool.checkout()!!
                val w2 = pool.checkout()!!
                val w3 = pool.checkout()!!
                pool.checkin(w1)
                pool.checkin(w2)
                pool.checkin(w3)
                pool.awaitStatus { it.available == 3 }
                assertEquals(w1.localRef(), pool.checkout()!!.localRef())
                assertEquals(w2.localRef(), pool.checkout()!!.localRef())
                assertEquals(w3.localRef(), pool.checkout()!!.localRef())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun max_overflow() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 2, maxOverflow = 1), testFactory())
            try {
                val w1 = pool.checkout()!!
                val w2 = pool.checkout()!!
                val w3 = pool.checkout()!!
                assertNotEquals(w1, w3)
                assertNotEquals(w2, w3)

                val s1 = pool.status()
                assertEquals(PoolStatus(PoolStateName.Full, 0, 1, 3), s1)

                val full = pool.checkout(block = false)
                assertNull(full)

                pool.checkin(w3)
                pool.awaitStatus { it.overflow == 0 }
                assertEquals(PoolStatus(PoolStateName.Overflow, 0, 0, 2), pool.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun borrower_death_returns_worker(): Unit =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                val checkedOut = CompletableDeferred<Unit>()
                // Isolate the borrower under a SupervisorJob (so its intentional crash
                // doesn't poison the runBlocking scope) and a no-op CoroutineExceptionHandler
                // (so the simulated crash isn't logged to stderr). Mirrors the BEAM model
                // where an unlinked process death only produces a 'DOWN' message for monitors.
                val swallowHandler = CoroutineExceptionHandler { _, _ -> }
                val supervisor = SupervisorJob(coroutineContext.job)
                val borrowerScope = CoroutineScope(coroutineContext + supervisor + swallowHandler)
                val borrower =
                    borrowerScope.launch {
                        pool.checkout()!!
                        checkedOut.complete(Unit)
                        error("simulated borrower crash")
                    }
                checkedOut.await()
                borrower.join()
                pool.awaitStatus { it.monitors == 0 && it.available == 1 }
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), pool.status())
                supervisor.cancel()
            } finally {
                pool.stop()
            }
        }

    @Test
    fun worker_death_replaced() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                val w1 = pool.checkout()!!
                w1.localRef().job.cancelAndJoin()
                pool.awaitStatus { it.monitors == 0 && it.available == 1 }
                val w2 = pool.checkout()!!
                assertNotEquals(w1.localRef(), w2.localRef())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun transaction_releases_on_exception() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                assertFails {
                    pool.transaction { _ -> error("intentional") }
                }
                pool.awaitStatus { it.available == 1 }
                assertEquals(0, pool.status().monitors)
            } finally {
                pool.stop()
            }
        }

    @Test
    fun stop_terminates_supervisor() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 2, maxOverflow = 0), testFactory())
            val genJob = pool.ref.job
            pool.stop()
            assertTrue(genJob.isCompleted)
        }

    @Test
    fun cancel_waiting_on_timeout(): Unit =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                val held = pool.checkout()!!

                val timedOut: Throwable? =
                    runCatchingAsync {
                        pool.checkout(block = true, timeout = 200.milliseconds)
                    }
                assertNotNull(timedOut, "second checkout should have timed out")

                pool.checkin(held)
                pool.awaitStatus { it.available == 1 }
                val w2 = withTimeoutOrNull(2.seconds) { pool.checkout() }
                assertNotNull(w2, "subsequent checkout must succeed once worker is free")
                Unit
            } finally {
                pool.stop()
            }
        }

    @Test
    fun startlink_blocks_until_init_done() =
        runBlocking {
            // After startLink returns, the pool must already be fully prepopulated:
            // status() should report `available == size` immediately, with no polling needed.
            val pool = Poolboy.startLink(this, PoolConfig(size = 4, maxOverflow = 0), testFactory())
            try {
                assertEquals(PoolStatus(PoolStateName.Ready, 4, 0, 0), pool.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun init_failure_does_not_leak_supervisor() =
        runBlocking {
            // Worker factory that always throws -> gen-server init fails on first spawn.
            // We assert: (a) startLink propagates the failure synchronously, (b) the dynamic
            // supervisor that was created up-front is shut down (no orphaned coroutines under
            // `parent`), and (c) the parent scope is still healthy for subsequent work.
            val boom = WorkerFactory<CounterState> { _ ->
                error("intentional worker init failure")
            }
            val before = coroutineContext.job.children.count()
            assertFails {
                Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), boom)
            }
            // Nothing left under parent: same number of children (or fewer if dyn-sup completed).
            val after = coroutineContext.job.children.count()
            assertTrue(after <= before, "init failure leaked $after-$before children under parent")

            // Parent is still usable: a second pool with a working factory must come up cleanly.
            val good = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                assertEquals(1, good.status().available)
            } finally {
                good.stop()
            }
        }

    @Test
    fun foreign_checkin_is_ignored() =
        runBlocking {
            val poolA = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            val poolB = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                val workerB = poolB.checkout()!!
                // Foreign checkin: hand poolB's worker to poolA. Must NOT corrupt poolA's accounting.
                poolA.checkin(workerB)
                // Give the cast a moment to be processed.
                delay(50)
                // poolA's view is unchanged: its single worker is still available, no monitors.
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), poolA.status())
                // poolA still serves checkouts normally afterwards.
                val w = poolA.checkout()!!
                poolA.checkin(w)
                poolA.awaitStatus { it.available == 1 }
                // poolB still considers workerB checked out.
                assertEquals(1, poolB.status().monitors)
                poolB.checkin(workerB)
                poolB.awaitStatus { it.monitors == 0 }
            } finally {
                poolA.stop()
                poolB.stop()
            }
        }

    @Test
    fun double_checkin_is_ignored() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                val w = pool.checkout()!!
                pool.checkin(w)
                pool.awaitStatus { it.available == 1 }
                // Double checkin should be a no-op: state must remain (Ready, 1, 0, 0).
                pool.checkin(w)
                delay(50)
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), pool.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun stop_is_idempotent() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 2, maxOverflow = 0), testFactory())
            pool.stop()
            // Second stop must be a clean no-op (no exception, no hang).
            pool.stop()
            // Third for good measure.
            pool.stop()
            assertTrue(pool.ref.job.isCompleted)
        }

    @Test
    fun unknown_call_does_not_crash_pool() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                // Send an unknown call type through the gen-server's mailbox.
                // The pool must reply (with null) rather than crash.
                val reply: Any? = pool.ref.call<Any?>("totally-unknown-call-payload", timeout = 1.seconds)
                assertNull(reply, "unknown call must reply with null")
                // Pool is still alive and serving:
                val w = pool.checkout()!!
                pool.checkin(w)
                pool.awaitStatus { it.available == 1 }
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), pool.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun unknown_cast_does_not_crash_pool() =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            try {
                pool.ref.cast("unknown-cast-payload")
                pool.ref.cast(42)
                pool.ref.cast(Unit)
                delay(50)
                // Pool must still be alive and serving.
                val w = pool.checkout()!!
                pool.checkin(w)
                pool.awaitStatus { it.available == 1 }
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), pool.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun parallel_init_spawns_workers_concurrently() =
        runBlocking {
            // Each worker's init does delay(200ms). With 5 workers serial: ~1000ms.
            // Parallel: ~200ms. Assert well under the serial time, with generous CI headroom.
            // Pre-warm one pool to absorb JIT / dispatcher startup so the assertion measures
            // parallelism, not first-time JVM costs.
            Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), slowFactory(50.milliseconds)).stop()

            val perWorker = 200.milliseconds
            val size = 5
            val pool: PoolRef<CounterState>
            val elapsed = measureTime {
                pool = Poolboy.startLink(this, PoolConfig(size = size, maxOverflow = 0), slowFactory(perWorker))
            }
            try {
                assertEquals(PoolStatus(PoolStateName.Ready, size, 0, 0), pool.status())
                // Serial would be size * perWorker = 1000ms. Cap at 600ms (3x single-worker) to
                // detect serial regressions while tolerating dispatcher / GC jitter under CI load.
                assertTrue(
                    elapsed < perWorker * 3,
                    "init should be parallel: $size workers @ $perWorker each took $elapsed (serial would be ~${perWorker * size})",
                )
            } finally {
                pool.stop()
            }
        }

    @Test
    fun parallel_init_propagates_first_failure_and_rolls_back() =
        runBlocking {
            // Factory that fails on the 3rd worker. All N spawns kick off in parallel; one of them
            // will throw, structured concurrency cancels the rest, init propagates the failure,
            // and Poolboy.startLink's catch shuts down the dyn supervisor + any partial spawns.
            val attempts = AtomicInteger(0)
            val failingFactory =
                WorkerFactory<CounterState> { scope ->
                    val n = attempts.incrementAndGet()
                    if (n == 3) error("intentional spawn failure on attempt #$n")
                    GenServers.startLinkSync(scope, SlowInitWorker(100.milliseconds))
                }

            val before = coroutineContext.job.children.count()
            assertFails {
                Poolboy.startLink(this, PoolConfig(size = 5, maxOverflow = 0), failingFactory)
            }
            // Allow async cleanup (dyn-supervisor shutdown is suspending under NonCancellable).
            delay(100)
            val after = coroutineContext.job.children.count()
            assertTrue(
                after <= before,
                "parallel-init failure leaked ${after - before} children under parent (had $before, now $after)",
            )

            // Parent scope is still healthy: a fresh pool starts fine.
            val good = Poolboy.startLink(this, PoolConfig(size = 2, maxOverflow = 0), testFactory())
            try {
                assertEquals(2, good.status().available)
            } finally {
                good.stop()
            }
        }

    @Test
    fun checkout_on_stopped_pool_throws_pool_stopped_exception(): Unit =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0, name = "stopped-checkout"), testFactory())
            pool.stop()
            // After stop(), checkout must throw PoolStoppedException (mapped from ServerDownException),
            // not the raw kotlin-otp internal exception.
            val ex = assertFailsWith<PoolStoppedException> {
                pool.checkout()
            }
            assertEquals("stopped-checkout", ex.poolName)
            assertNotNull(ex.cause, "PoolStoppedException must wrap the underlying ServerDownException")
            // status() must do the same mapping.
            assertFailsWith<PoolStoppedException> { pool.status() }
            Unit
        }

    @Test
    fun transaction_on_stopped_pool_throws_pool_stopped_exception(): Unit =
        runBlocking {
            val pool = Poolboy.startLink(this, PoolConfig(size = 1, maxOverflow = 0), testFactory())
            pool.stop()
            // transaction { } first calls checkout, which now surfaces PoolStoppedException
            // instead of the previous "pool returned null worker" / ServerDownException leak.
            assertFailsWith<PoolStoppedException> {
                pool.transaction { _ -> error("should never run; checkout fails first") }
            }
            Unit
        }

    @Test
    fun pool_resolvable_via_local_node(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val poolName = "dist-pool-$suffix"
            val nodeIdA = NodeId("a-$suffix", "poolboy-dist")
            val nodeIdB = NodeId("b-$suffix", "poolboy-dist")
            val nodeA = LocalNode(nodeIdA)
            val nodeB = LocalNode(nodeIdB)
            val transport =
                InMemoryTransport().also {
                    it.addNode(nodeA)
                    it.addNode(nodeB)
                    it.connect(nodeA, nodeB)
                }
            val pool =
                Poolboy.startLink(
                    this,
                    PoolConfig(size = 1, maxOverflow = 0, name = poolName),
                    testFactory(),
                    homeNode = nodeA,
                )
            try {
                val remote =
                    Poolboy.resolve<CounterState>(
                        PoolAddress.OnNode(poolName, nodeIdA),
                        transport,
                    )
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), remote.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun pool_checkout_via_remote_node_inmemory(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val poolName = "dist-checkout-$suffix"
            val nodeIdA = NodeId("a-$suffix", "poolboy-dist")
            val nodeA = LocalNode(nodeIdA)
            val nodeB = LocalNode(NodeId("b-$suffix", "poolboy-dist"))
            val transport =
                InMemoryTransport().also {
                    it.addNode(nodeA)
                    it.addNode(nodeB)
                    it.connect(nodeA, nodeB)
                }
            val pool =
                Poolboy.startLink(
                    this,
                    PoolConfig(size = 1, maxOverflow = 0, name = poolName),
                    testFactory(),
                    homeNode = nodeA,
                )
            try {
                val remote =
                    Poolboy.resolve<CounterState>(
                        PoolAddress.OnNode(poolName, nodeIdA),
                        transport,
                    )
                val worker = remote.checkout()!!
                val calls = worker.call<Int>(Unit)
                assertEquals(1, calls)
                remote.checkin(worker)
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), remote.status())
            } finally {
                pool.stop()
            }
        }

    @Test
    fun pool_global_registry_resolution(): Unit =
        runBlocking {
            val poolName = "global-pool-${System.nanoTime()}"
            val pool =
                Poolboy.startLink(
                    this,
                    PoolConfig(size = 2, maxOverflow = 0, name = poolName),
                    testFactory(),
                )
            when (val reg = GlobalRegistry.registerName(poolName, pool.ref)) {
                is GlobalRegistry.RegisterResult.Ok -> { }
                is GlobalRegistry.RegisterResult.Conflict ->
                    error("unexpected global name conflict: ${reg.existing}")
            }
            try {
                val handle = Poolboy.resolve<CounterState>(PoolAddress.Global(poolName))
                assertEquals(2, handle.status().available)
            } finally {
                GlobalRegistry.unregisterName(poolName)
                pool.stop()
            }
        }

    @Test
    fun pool_via_registry_resolution(): Unit =
        runBlocking {
            val poolName = "via-pool-${System.nanoTime()}"
            val via = MapViaRegistry()
            val pool =
                Poolboy.startLink(
                    this,
                    PoolConfig(size = 1, maxOverflow = 0),
                    testFactory(),
                )
            via.register(poolName, pool.ref)
            try {
                val handle = Poolboy.resolve<CounterState>(PoolAddress.Via(via, poolName))
                val worker = handle.checkout()!!
                handle.checkin(worker)
                assertEquals(PoolStatus(PoolStateName.Ready, 1, 0, 0), handle.status())
            } finally {
                via.unregister(poolName, pool.ref)
                pool.stop()
            }
        }

    @Test
    fun remote_checkout_surfaces_pool_stopped(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val poolName = "dist-stopped-$suffix"
            val nodeIdA = NodeId("a-$suffix", "poolboy-dist")
            val nodeA = LocalNode(nodeIdA)
            val nodeB = LocalNode(NodeId("b-$suffix", "poolboy-dist"))
            val transport =
                InMemoryTransport().also {
                    it.addNode(nodeA)
                    it.addNode(nodeB)
                    it.connect(nodeA, nodeB)
                }
            val pool =
                Poolboy.startLink(
                    this,
                    PoolConfig(size = 1, maxOverflow = 0, name = poolName),
                    testFactory(),
                    homeNode = nodeA,
                )
            pool.stop()
            val remote =
                Poolboy.resolve<CounterState>(
                    PoolAddress.OnNode(poolName, nodeIdA),
                    transport,
                )
            val ex =
                assertFailsWith<PoolStoppedException> {
                    remote.checkout()
                }
            assertEquals(poolName, ex.poolName)
            Unit
        }

    @Test
    fun pool_checkout_via_tcp_loopback(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val poolName = "tcp-checkout-$suffix"
            val nodeIdA = NodeId("a-$suffix", "loopback")
            val nodeIdB = NodeId("b-$suffix", "loopback")
            val regA = ProcessRegistry()
            val regB = ProcessRegistry()
            val ta = KotlinNodeTransport(nodeIdA, "", 0)
            val tb = KotlinNodeTransport(nodeIdB, "", 0)
            try {
                tb.startAccepting(this, regB)
                ta.startAccepting(this, regA)
                val home = LocalNode(nodeIdB)
                val pool =
                    Poolboy.startLink(
                        this,
                        PoolConfig(size = 1, maxOverflow = 0, name = poolName),
                        testFactory(),
                        homeNode = home,
                    )
                regB.register(poolName, pool.ref)
                ta.connectOut(this, regA, nodeIdB, "127.0.0.1", tb.boundPort)
                val remote =
                    Poolboy.resolve<CounterState>(
                        PoolAddress.OnNode(poolName, nodeIdB),
                        ta,
                    )
                val worker = remote.checkout()!!
                val calls = worker.call<Int>(Unit)
                assertEquals(1, calls)
                worker.checkin()
            } finally {
                ta.close()
                tb.close()
                coroutineContext.job.cancelChildren()
            }
        }

    @Test
    fun pool_status_via_tcp_loopback(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val poolName = "tcp-status-$suffix"
            val nodeIdA = NodeId("a-$suffix", "loopback")
            val nodeIdB = NodeId("b-$suffix", "loopback")
            val regA = ProcessRegistry()
            val regB = ProcessRegistry()
            val ta = KotlinNodeTransport(nodeIdA, "", 0)
            val tb = KotlinNodeTransport(nodeIdB, "", 0)
            try {
                tb.startAccepting(this, regB)
                ta.startAccepting(this, regA)
                val pool =
                    Poolboy.startLink(
                        this,
                        PoolConfig(size = 2, maxOverflow = 0, name = poolName),
                        testFactory(),
                        homeNode = LocalNode(nodeIdB),
                    )
                regB.register(poolName, pool.ref)
                ta.connectOut(this, regA, nodeIdB, "127.0.0.1", tb.boundPort)
                val remote =
                    Poolboy.resolve<CounterState>(
                        PoolAddress.OnNode(poolName, nodeIdB),
                        ta,
                    )
                assertEquals(PoolStatus(PoolStateName.Ready, 2, 0, 0), remote.status())
            } finally {
                ta.close()
                tb.close()
                coroutineContext.job.cancelChildren()
            }
        }

    @Test
    fun tcp_checkin_returns_worker_to_pool(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val poolName = "tcp-checkin-$suffix"
            val nodeIdA = NodeId("a-$suffix", "loopback")
            val nodeIdB = NodeId("b-$suffix", "loopback")
            val regA = ProcessRegistry()
            val regB = ProcessRegistry()
            val ta = KotlinNodeTransport(nodeIdA, "", 0)
            val tb = KotlinNodeTransport(nodeIdB, "", 0)
            try {
                tb.startAccepting(this, regB)
                ta.startAccepting(this, regA)
                val pool =
                    Poolboy.startLink(
                        this,
                        PoolConfig(size = 1, maxOverflow = 0, name = poolName),
                        testFactory(),
                        homeNode = LocalNode(nodeIdB),
                    )
                regB.register(poolName, pool.ref)
                ta.connectOut(this, regA, nodeIdB, "127.0.0.1", tb.boundPort)
                val remote =
                    Poolboy.resolve<CounterState>(
                        PoolAddress.OnNode(poolName, nodeIdB),
                        ta,
                    )
                val w1 = remote.checkout()!!
                assertEquals(1, w1.call<Int>(Unit))
                w1.checkin()
                delay(50)
                val w2 = remote.checkout()!!
                assertEquals(2, w2.call<Int>(Unit))
                w2.checkin()
            } finally {
                ta.close()
                tb.close()
                coroutineContext.job.cancelChildren()
            }
        }

    @Test
    fun pool_global_via_tcp_loopback(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val poolName = "tcp-global-$suffix"
            val nodeIdA = NodeId("a-$suffix", "loopback")
            val nodeIdB = NodeId("b-$suffix", "loopback")
            val localA = LocalNode(nodeIdA)
            val localB = LocalNode(nodeIdB)
            val regA = ProcessRegistry()
            val regB = ProcessRegistry()
            val ta = KotlinNodeTransport(nodeIdA, "", 0)
            val tb = KotlinNodeTransport(nodeIdB, "", 0)
            try {
                tb.startAccepting(this, regB)
                ta.startAccepting(this, regA)
                GlobalRegistry.install(tb, localB)
                GlobalRegistry.useNode(localA)
                val pool =
                    Poolboy.startLink(
                        this,
                        PoolConfig(size = 1, maxOverflow = 0, name = poolName),
                        testFactory(),
                        homeNode = localB,
                    )
                regB.register(poolName, pool.ref)
                ta.connectOut(this, regA, nodeIdB, "127.0.0.1", tb.boundPort)
                delay(50)
                GlobalRegistry.useNode(localB)
                when (val reg = GlobalRegistry.registerName(poolName, pool.ref)) {
                    is GlobalRegistry.RegisterResult.Ok -> { }
                    is GlobalRegistry.RegisterResult.Conflict -> error("conflict: ${reg.existing}")
                }
                delay(50)
                GlobalRegistry.useNode(localA)
                val remote =
                    Poolboy.resolve<CounterState>(
                        PoolAddress.Global(poolName),
                        ta,
                    )
                val worker = remote.checkout()!!
                assertEquals(1, worker.call<Int>(Unit))
                worker.checkin()
            } finally {
                GlobalRegistry.reset()
                ta.close()
                tb.close()
                coroutineContext.job.cancelChildren()
            }
        }

    @Test
    fun example_app_smoke() =
        runBlocking {
            val env = smallExampleEnv()
            val app = ExampleApp(env)
            when (val started = app.application.start(this)) {
                is ApplicationStartResult.Ok -> {
                    val pool1 = app.handles.getValue("pool1").await()
                    val pool2 = app.handles.getValue("pool2").await()
                    assertEquals(PoolStateName.Ready, pool1.status().state)
                    assertEquals(PoolStateName.Ready, pool2.status().state)
                    app.application.stop()
                }
                else -> error("application failed to start: $started")
            }
        }

    @Test
    fun example_app_runs_real_sql() =
        runBlocking {
            val env = smallExampleEnv()
            val app = ExampleApp(env)
            val started = app.application.start(this)
            assertTrue(started is ApplicationStartResult.Ok, "application should start: $started")
            try {
                val pool1 = app.handles.getValue("pool1").await()
                val count: Int =
                    pool1.transaction { worker ->
                        worker.call<SqlResult>(
                            WorkerRequest.Squery("CREATE TABLE IF NOT EXISTS t(id INT)"),
                        )
                        worker.call<SqlResult>(WorkerRequest.Squery("DELETE FROM t"))
                        worker.call<SqlResult>(WorkerRequest.Squery("INSERT INTO t VALUES (1), (2), (3)"))
                        val rows =
                            worker.call<SqlResult>(WorkerRequest.Squery("SELECT COUNT(*) FROM t"))
                                as SqlResult.Rows
                        (rows.rows.single().single() as Number).toInt()
                    }
                assertEquals(3, count)
            } finally {
                app.application.stop()
            }
        }
}

/** Helper: run a suspend block, swallow + return any throwable, or null on success. */
private suspend inline fun <T> runCatchingAsync(crossinline block: suspend () -> T): Throwable? =
    try {
        block()
        null
    } catch (t: Throwable) {
        t
    }

/** Smaller pools / unique JDBC URLs so tests don't share state across runs. */
private fun smallExampleEnv(): ApplicationEnv {
    val suffix = System.nanoTime().toString()
    return ApplicationEnv(
        mapOf(
            "pools" to
                listOf(
                    PoolDef(
                        name = "pool1",
                        poolConfig = PoolConfig(size = 2, maxOverflow = 1, name = "pool1-$suffix"),
                        workerArgs = WorkerArgs(jdbcUrl = "jdbc:h2:mem:p1-$suffix;DB_CLOSE_DELAY=-1"),
                    ),
                    PoolDef(
                        name = "pool2",
                        poolConfig = PoolConfig(size = 1, maxOverflow = 0, name = "pool2-$suffix"),
                        workerArgs = WorkerArgs(jdbcUrl = "jdbc:h2:mem:p2-$suffix;DB_CLOSE_DELAY=-1"),
                    ),
                ),
        ),
    )
}

