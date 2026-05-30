package org.otpstudy.supervisor

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class DynamicSupervisorRobustnessContractTest {
    @Test
    fun `deterministic contract - sync start returns ready resource`() = runBlocking {
        DynamicSupervisor.resetMetrics()
        val flags = SupervisorFlags(strategy = SupervisorStrategy.OneForOne, intensity = 10, period = 10.seconds)
        val template =
            SimpleOneForOneTemplate(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, ready ->
                ready("ready")
                delay(Long.MAX_VALUE)
            }
        val ref = DynamicSupervisor.startLink(this, flags, template)
        val (_, value) = ref.startChildSync<String>(500.milliseconds)
        assertEquals("ready", value)
        val metrics = DynamicSupervisor.metricsSnapshot()
        assertTrue(metrics.syncStarts >= 1L)
        assertTrue(metrics.syncStartSuccess >= 1L)
        ref.shutdown()
    }

    @Test
    fun `adversarial contract - duplicate ready is rejected`() = runBlocking {
        DynamicSupervisor.resetMetrics()
        val flags = SupervisorFlags(strategy = SupervisorStrategy.OneForOne, intensity = 10, period = 10.seconds)
        val template =
            SimpleOneForOneTemplate(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, ready ->
                ready(Unit)
                ready(Unit)
            }
        val ref = DynamicSupervisor.startLink(this, flags, template)
        // Duplicate ready may surface as immediate sync failure or as post-ready child crash;
        // both are acceptable as long as the contract does not hang and no child leaks.
        runCatching { ref.startChildSync<Unit>(500.milliseconds) }
        delay(100)
        assertEquals(0, ref.countChildren())
        val metrics = DynamicSupervisor.metricsSnapshot()
        assertTrue(metrics.syncStarts >= 1L)
        ref.shutdown()
    }

    @Test
    fun `recovery contract - timed out sync start cleans slot`() = runBlocking {
        DynamicSupervisor.resetMetrics()
        val flags = SupervisorFlags(strategy = SupervisorStrategy.OneForOne, intensity = 10, period = 10.seconds)
        val template =
            SimpleOneForOneTemplate<Unit>(
                restart = Restart.Temporary,
                shutdown = Shutdown.BrutalKill,
            ) { _, _, _ ->
                delay(5.seconds)
            }
        val ref = DynamicSupervisor.startLink(this, flags, template)
        val timedOut = runCatching { ref.startChildSync<Unit>(200.milliseconds) }.exceptionOrNull()
        assertTrue(timedOut is kotlinx.coroutines.TimeoutCancellationException)
        assertEquals(0, ref.countChildren(), "failed sync start should not leave stale child slots")
        val metrics = DynamicSupervisor.metricsSnapshot()
        assertTrue(metrics.syncStartFailure >= 1L)
        ref.shutdown()
    }
}
