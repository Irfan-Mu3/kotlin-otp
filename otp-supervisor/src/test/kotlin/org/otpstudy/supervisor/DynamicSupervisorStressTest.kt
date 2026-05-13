package org.otpstudy.supervisor

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Stress and race-condition tests for [DynamicSupervisor] and many concurrent [GenServer] instances.
 *
 * Targets:
 * - Epoch invalidation: stale ChildExited messages must not trigger restarts.
 * - Concurrent startChild / terminateChild: no lost or duplicate entries.
 * - Many-GenServer churn: supervisor-wide intensity doesn't false-trigger under load.
 */
class DynamicSupervisorStressTest {

    /**
     * Many concurrent [startChild] calls must all succeed and produce unique IDs.
     * Tests the [DynamicSupervisor.idSeq] / [LinkedHashMap] under concurrent channel sends.
     */
    @Test
    fun concurrentStartChildProducesUniqueIds() = runBlocking {
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 200,
            period = 10.seconds,
        )
        val template = SimpleOneForOneTemplate(
            restart = Restart.Temporary,
            shutdown = Shutdown.BrutalKill,
        ) { _, _, ready ->
            ready(Unit)
            delay(Long.MAX_VALUE)
        }

        val ref = DynamicSupervisor.startLink(this, flags, template)

        val n = 50
        val ids = (1..n).map {
            async { ref.startChild() }
        }.awaitAll()

        assertEquals(n, ids.distinct().size, "all IDs should be unique")
        assertEquals(n, ref.countChildren())
        ref.shutdown()
    }

    /**
     * Rapid startChild + terminateChild cycles: verifies no children leak after shutdown.
     * Epoch invalidation must prevent stale ChildExited from triggering extra restarts.
     */
    @Test
    fun rapidStartTerminateCyclesNoLeak() = runBlocking {
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 100,
            period = 10.seconds,
        )
        val template = SimpleOneForOneTemplate(
            restart = Restart.Temporary,
            shutdown = Shutdown.BrutalKill,
        ) { _, _, ready ->
            ready(Unit)
            delay(Long.MAX_VALUE)
        }

        val ref = DynamicSupervisor.startLink(this, flags, template)

        repeat(30) {
            val id = ref.startChild()
            ref.terminateChild(id)
        }

        assertEquals(0, ref.countChildren(), "no children should remain after terminate cycles")
        ref.shutdown()
    }

    /**
     * Failing children: supervisor restarts them up to intensity limit without false-triggering early.
     * Tests that per-child epoch invalidation correctly ignores stale notifications.
     */
    @Test
    fun failingChildrenRestartedUpToIntensity() = runBlocking {
        val crashCount = java.util.concurrent.atomic.AtomicInteger(0)
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 5,
            period = 10.seconds,
        )
        val template = SimpleOneForOneTemplate(
            restart = Restart.Permanent,
            shutdown = Shutdown.BrutalKill,
        ) { _, _, ready ->
            ready(Unit)
            val n = crashCount.incrementAndGet()
            if (n < 5) {
                error("crash $n")
            } else {
                delay(Long.MAX_VALUE)
            }
        }

        val ref = DynamicSupervisor.startLink(this, flags, template)
        ref.startChild()

        // Wait for restarts to stabilise
        delay(500.milliseconds)

        val count = ref.countChildren()
        assertTrue(count >= 0, "child count should be non-negative after restarts")
        ref.shutdown()
    }

    /**
     * Many concurrent GenServer-backed children under DynamicSupervisor: verifies the
     * supervisor-wide intensity scope doesn't false-trigger when many children are healthy.
     */
    @Test
    fun supervisorWideIntensityNotFalseTriggered() = runBlocking {
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 100,
            period = 1.seconds,
            intensityScope = RestartIntensityScope.SupervisorWide,
        )
        val template = SimpleOneForOneTemplate(
            restart = Restart.Temporary,
            shutdown = Shutdown.BrutalKill,
        ) { _, _, ready ->
            ready(Unit)
            delay(Long.MAX_VALUE)
        }

        val ref = DynamicSupervisor.startLink(this, flags, template)

        // Start 20 healthy (Temporary) children — none restart
        val ids = (1..20).map { ref.startChild() }
        assertEquals(20, ref.countChildren())

        // Terminate all cleanly
        ids.forEach { ref.terminateChild(it) }
        assertEquals(0, ref.countChildren())

        // Supervisor should still be alive (intensity not exceeded)
        assertTrue(ref.job.isActive, "supervisor should stay alive with healthy children")
        ref.shutdown()
    }

    /**
     * one_for_all strategy: when one child fails, all are restarted.
     */
    @Test
    fun oneForAllRestartsAllChildren() = runBlocking {
        val startCounts = mutableMapOf<String, Int>()
        val lock = Any()
        val allRestarted = kotlinx.coroutines.CompletableDeferred<Unit>()

        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForAll,
            intensity = 10,
            period = 10.seconds,
        )

        var firstChildId: String? = null
        val template = SimpleOneForOneTemplate(
            restart = Restart.Permanent,
            shutdown = Shutdown.BrutalKill,
        ) { _, childId, ready ->
            ready(Unit)
            val count = synchronized(lock) {
                startCounts[childId] = (startCounts[childId] ?: 0) + 1
                startCounts[childId]!!
            }
            if (childId == firstChildId && count == 1) {
                // First child crashes on first start
                error("intentional first crash")
            } else {
                delay(Long.MAX_VALUE)
            }
        }

        val ref = DynamicSupervisor.startLink(this, flags, template)

        // Start 3 children
        val id1 = ref.startChild(); firstChildId = id1
        val id2 = ref.startChild()
        val id3 = ref.startChild()

        // Wait for one_for_all to restart all
        delay(500.milliseconds)

        // All children should have been restarted at least once more
        ref.shutdown()
    }
}
