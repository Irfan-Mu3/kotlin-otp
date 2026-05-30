package org.otpstudy.supervisor

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.TimeoutCancellationException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class DynamicSupervisorTest {
    @Test
    fun startChildAndWhichChildren() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Permanent,
                    shutdown = Shutdown.Timeout(100.milliseconds),
                    start = { _, _, ready ->
                        ready(Unit)
                        delay(60_000)
                    },
                )
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val a = ref.startChild()
            val b = ref.startChild()
            val list = ref.whichChildren()
            assertEquals(2, ref.countChildren())
            assertTrue(list.any { it.id == a })
            assertTrue(list.any { it.id == b })
            ref.shutdown()
        }

    @Test
    fun terminateChildRemoves() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Permanent,
                    shutdown = Shutdown.Timeout(100.milliseconds),
                    start = { _, _, ready ->
                        ready(Unit)
                        delay(60_000)
                    },
                )
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val id = ref.startChild()
            assertEquals(1, ref.countChildren())
            ref.terminateChild(id)
            assertEquals(0, ref.countChildren())
            ref.shutdown()
        }

    @Test
    fun startChildSyncReturnsReadyValue() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Temporary,
                    shutdown = Shutdown.BrutalKill,
                ) { _, id, ready ->
                    ready("payload-$id")
                    delay(60_000)
                }
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val (childId, payload) = ref.startChildSync<String>()
            assertTrue(payload.startsWith("payload-"))
            assertTrue(childId.startsWith("dyn-"))
            ref.shutdown()
        }

    @Test
    fun startChildSyncTimesOutWhenReadyNeverCalled() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate<Unit>(
                    restart = Restart.Temporary,
                    shutdown = Shutdown.BrutalKill,
                ) { _, _, _ ->
                    delay(Long.MAX_VALUE)
                }
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            assertFailsWith<TimeoutCancellationException> {
                ref.startChildSync<Unit>(timeout = 100.milliseconds)
            }
            ref.shutdown()
        }

    @Test
    fun startChildSyncConcurrentStartsDoNotSerializeCoordinator() =
        runBlocking {
            val delayBeforeReady = 300.milliseconds
            val n = 5
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Temporary,
                    shutdown = Shutdown.BrutalKill,
                ) { _, _, ready ->
                    delay(delayBeforeReady)
                    ready(Unit)
                    delay(60_000)
                }
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            coroutineScope {
                val jobs = List(n) { async { ref.startChildSync<Unit>(timeout = 60.seconds) } }
                withTimeout(delayBeforeReady * 2) {
                    jobs.awaitAll()
                }
            }
            assertEquals(n, ref.countChildren())
            ref.shutdown()
        }

    @Test
    fun coordinatorServesQueriesWhileSyncStartIsInFlight() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Temporary,
                    shutdown = Shutdown.BrutalKill,
                ) { _, _, ready ->
                    delay(1.seconds)
                    ready(Unit)
                    delay(60_000)
                }
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val sync = async { ref.startChildSync<Unit>(timeout = 30.seconds) }
            withTimeout(2.seconds) {
                while (ref.countChildren() < 1) yield()
            }
            withTimeout(100.milliseconds) {
                assertEquals(1, ref.countChildren())
                assertEquals(1, ref.whichChildren().size)
                ref.startChild()
            }
            sync.await()
            ref.shutdown()
        }

    @Test
    fun startChildSyncCompletesExceptionallyOnShutdown() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate<Unit>(
                    restart = Restart.Temporary,
                    shutdown = Shutdown.BrutalKill,
                ) { _, _, _ ->
                    delay(Long.MAX_VALUE)
                }
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val outcome =
                async {
                    kotlin.runCatching { ref.startChildSync<Unit>(timeout = 60.seconds) }
                }
            delay(50.milliseconds)
            ref.shutdown()
            withTimeout(2.seconds) {
                val r = outcome.await()
                assertTrue(r.isFailure)
                val ex = r.exceptionOrNull()
                assertTrue(ex is IllegalStateException && ex.message == "shutting down")
            }
        }

    @Test
    fun startChildSyncFailureBeforeReadyDoesNotPermanentRestartLoop() {
        repeat(10) {
            runBlocking {
                val template =
                    SimpleOneForOneTemplate<Unit>(
                        restart = Restart.Permanent,
                        shutdown = Shutdown.BrutalKill,
                    ) { _, _, _ ->
                        error("always fail before ready")
                    }
                val ref =
                    DynamicSupervisor.startLink(
                        this,
                        SupervisorFlags(intensity = 100, period = 60.seconds),
                        template,
                    )
                assertFailsWith<IllegalStateException> {
                    ref.startChildSync<Unit>(timeout = 1.seconds)
                }
                delay(500.milliseconds)
                assertEquals(0, ref.countChildren())
                ref.shutdown()
            }
        }
    }

    @Test
    fun supervisorWideIntensityExceededCancelsDynamicSupervisor() = runBlocking {
        val template =
            SimpleOneForOneTemplate<Unit>(
                restart = Restart.Permanent,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, _ ->
                error("always crash")
            }
        val ref =
            DynamicSupervisor.startLink(
                this,
                SupervisorFlags(
                    strategy = SupervisorStrategy.OneForOne,
                    intensity = 1,
                    period = 5.seconds,
                    intensityScope = RestartIntensityScope.SupervisorWide,
                ),
                template,
            )

        ref.startChild()
        withTimeout(2.seconds) { ref.job.join() }
        assertTrue(ref.job.isCancelled, "dynamic supervisor should cancel when supervisor-wide intensity is exceeded")
    }

    @Test
    fun startChildSyncReadyCalledTwiceCompletesAndDoesNotLeakChild() = runBlocking {
        val template =
            SimpleOneForOneTemplate<String>(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, ready ->
                ready("first")
                ready("second")
                delay(Long.MAX_VALUE)
            }
        val ref =
            DynamicSupervisor.startLink(
                this,
                SupervisorFlags(intensity = 10, period = 60.seconds),
                template,
            )

        withTimeout(2.seconds) {
            kotlin.runCatching { ref.startChildSync<String>(timeout = 1.seconds) }
        }
        withTimeout(2.seconds) {
            while (ref.countChildren() != 0) yield()
        }
        ref.shutdown()
    }

    @Test
    fun syncStartReadyThenImmediateNormalExitDoesNotLeakTemporaryChild() = runBlocking {
        val template =
            SimpleOneForOneTemplate<Unit>(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, ready ->
                ready(Unit)
                // immediate normal completion
            }
        val ref =
            DynamicSupervisor.startLink(
                this,
                SupervisorFlags(intensity = 50, period = 60.seconds),
                template,
            )

        repeat(20) {
            ref.startChildSync<Unit>(timeout = 1.seconds)
        }
        withTimeout(2.seconds) {
            while (ref.countChildren() != 0) yield()
        }
        ref.shutdown()
    }

    @Test
    fun concurrentImmediateNormalExitSyncStartsDoNotAccumulateStaleChildren() = runBlocking {
        val template =
            SimpleOneForOneTemplate<Unit>(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, ready ->
                ready(Unit)
                // immediate normal completion
            }
        val ref =
            DynamicSupervisor.startLink(
                this,
                SupervisorFlags(intensity = 100, period = 60.seconds),
                template,
            )

        coroutineScope {
            val jobs = List(30) { async { ref.startChildSync<Unit>(timeout = 2.seconds) } }
            withTimeout(3.seconds) { jobs.awaitAll() }
        }
        withTimeout(2.seconds) {
            while (ref.countChildren() != 0) yield()
        }
        ref.shutdown()
    }

    @Test
    fun longRunSyncChurnWithMixedOutcomesDoesNotLeakChildren() = runBlocking {
        val cycle = java.util.concurrent.atomic.AtomicInteger(0)
        val template =
            SimpleOneForOneTemplate<Unit>(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, ready ->
                when (cycle.incrementAndGet() % 3) {
                    0 -> {
                        ready(Unit)
                        // immediate normal completion
                    }
                    1 -> delay(300.milliseconds) // forces timeout for short sync timeout
                    else -> error("intentional pre-ready failure")
                }
            }
        val ref =
            DynamicSupervisor.startLink(
                this,
                SupervisorFlags(intensity = 200, period = 60.seconds),
                template,
            )

        repeat(120) {
            kotlin.runCatching { ref.startChildSync<Unit>(timeout = 100.milliseconds) }
        }
        withTimeout(5.seconds) {
            while (ref.countChildren() != 0) yield()
        }
        assertEquals(0, ref.countChildren())
        ref.shutdown()
    }

    @Test
    fun concurrentLongRunSyncChurnBoundedAndResponsive() = runBlocking {
        val cycle = java.util.concurrent.atomic.AtomicInteger(0)
        val template =
            SimpleOneForOneTemplate<Unit>(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, ready ->
                when (cycle.incrementAndGet() % 4) {
                    0 -> ready(Unit)
                    1 -> delay(250.milliseconds) // likely timeout
                    2 -> error("intentional fail")
                    else -> {
                        ready(Unit)
                        delay(10.milliseconds)
                    }
                }
            }
        val ref =
            DynamicSupervisor.startLink(
                this,
                SupervisorFlags(intensity = 300, period = 60.seconds),
                template,
            )

        coroutineScope {
            val jobs = List(8) {
                async {
                    repeat(30) {
                        kotlin.runCatching { ref.startChildSync<Unit>(timeout = 100.milliseconds) }
                    }
                }
            }
            withTimeout(15.seconds) { jobs.awaitAll() }
        }
        withTimeout(5.seconds) {
            while (ref.countChildren() != 0) yield()
        }
        assertEquals(0, ref.countChildren())
        ref.shutdown()
    }
}
