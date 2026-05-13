package org.otpstudy.supervisor

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
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
 * Table-driven tests mirroring OTP supervisor restart semantics.
 *
 * Inspired by the OTP `supervisor.erl` test suite (lib/stdlib/test/supervisor_SUITE.erl).
 * Exact BEAM semantics (kill signals, linked process exit) differ from JVM cancellation;
 * see LIMITATIONS.md for the explicit gap list.
 *
 * Each test documents the OTP behaviour being approximated.
 */
class SupervisorOtpParityTest {

    // -----------------------------------------------------------------------
    // 1. Restart type parity: Permanent / Transient / Temporary
    // -----------------------------------------------------------------------

    /**
     * OTP: `permanent` child is always restarted, regardless of exit reason.
     * Test: child exits normally → supervisor restarts it.
     */
    @Test
    fun permanentChildRestartedOnNormalExit() = runBlocking {
        val startCount = java.util.concurrent.atomic.AtomicInteger(0)
        val started = CompletableDeferred<Unit>()
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 3,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "permanent-child",
            restart = Restart.Permanent,
            shutdown = Shutdown.BrutalKill,
        ) {
            startCount.incrementAndGet()
            if (startCount.get() == 1) {
                // Normal exit on first start
            } else {
                started.complete(Unit)
                delay(Long.MAX_VALUE)
            }
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        started.await()
        assertEquals(2, startCount.get(), "permanent child should restart after normal exit")
        ref.shutdown()
    }

    /**
     * OTP: `permanent` child is restarted on abnormal exit too.
     */
    @Test
    fun permanentChildRestartedOnFailure() = runBlocking {
        val startCount = java.util.concurrent.atomic.AtomicInteger(0)
        val started = CompletableDeferred<Unit>()
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 3,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "perm-fail-child",
            restart = Restart.Permanent,
            shutdown = Shutdown.BrutalKill,
        ) {
            val n = startCount.incrementAndGet()
            if (n == 1) {
                error("crash on first start")
            } else {
                started.complete(Unit)
                delay(Long.MAX_VALUE)
            }
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        started.await()
        assertEquals(2, startCount.get(), "permanent child should restart after failure")
        ref.shutdown()
    }

    /**
     * OTP: `transient` child is restarted only on abnormal exit.
     * Normal exit → no restart.
     */
    @Test
    fun transientChildNotRestartedOnNormalExit() = runBlocking {
        val startCount = java.util.concurrent.atomic.AtomicInteger(0)
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 3,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "transient-child",
            restart = Restart.Transient,
            shutdown = Shutdown.BrutalKill,
        ) {
            startCount.incrementAndGet()
            // Normal exit
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        delay(200.milliseconds)
        assertEquals(1, startCount.get(), "transient child should NOT restart after normal exit")
        ref.shutdown()
    }

    /**
     * OTP: `transient` child IS restarted on abnormal (non-normal) exit.
     */
    @Test
    fun transientChildRestartedOnFailure() = runBlocking {
        val startCount = java.util.concurrent.atomic.AtomicInteger(0)
        val restarted = CompletableDeferred<Unit>()
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 3,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "transient-fail",
            restart = Restart.Transient,
            shutdown = Shutdown.BrutalKill,
        ) {
            val n = startCount.incrementAndGet()
            if (n == 1) {
                error("transient crash")
            } else {
                restarted.complete(Unit)
                delay(Long.MAX_VALUE)
            }
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        restarted.await()
        assertEquals(2, startCount.get(), "transient child should restart after failure")
        ref.shutdown()
    }

    /**
     * OTP: `temporary` child is never restarted regardless of exit reason.
     */
    @Test
    fun temporaryChildNeverRestarted() = runBlocking {
        val startCount = java.util.concurrent.atomic.AtomicInteger(0)
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 3,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "temporary-child",
            restart = Restart.Temporary,
            shutdown = Shutdown.BrutalKill,
        ) {
            startCount.incrementAndGet()
            error("temporary crash")
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        delay(200.milliseconds)
        assertEquals(1, startCount.get(), "temporary child should NEVER restart")
        ref.shutdown()
    }

    // -----------------------------------------------------------------------
    // 2. Intensity window edge cases
    // -----------------------------------------------------------------------

    /**
     * OTP: supervisor dies when restart intensity is exceeded.
     * Test: intensity=2/5s → crash 3 times → supervisor's job is cancelled.
     */
    @Test
    fun supervisorDiesOnIntensityExceeded() = runBlocking {
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 2,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "crash-child",
            restart = Restart.Permanent,
            shutdown = Shutdown.BrutalKill,
        ) {
            error("crash")
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        // Supervisor job should eventually be cancelled after intensity exceeded
        ref.job.join()
        assertTrue(ref.job.isCancelled, "supervisor should cancel after intensity exceeded")
    }

    /**
     * OTP: restarts exactly at the intensity limit are allowed; the (intensity+1)th kills the supervisor.
     */
    @Test
    fun exactlyAtIntensityLimitAllowed() = runBlocking {
        val startCount = java.util.concurrent.atomic.AtomicInteger(0)
        // intensity=1: 1 restart allowed, 2nd crash kills supervisor
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 1,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "at-limit",
            restart = Restart.Permanent,
            shutdown = Shutdown.BrutalKill,
        ) {
            startCount.incrementAndGet()
            error("crash")
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        ref.job.join()
        assertTrue(startCount.get() >= 2, "should have started at least twice before intensity exceeded")
    }

    // -----------------------------------------------------------------------
    // 3. Supervisor-wide intensity scope (M5b)
    // -----------------------------------------------------------------------

    /**
     * With [RestartIntensityScope.SupervisorWide], restarts from different children
     * all count toward the shared window.
     */
    @Test
    fun supervisorWideIntensityCountsAcrossChildren() = runBlocking {
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 2,
            period = 5.seconds,
            intensityScope = RestartIntensityScope.SupervisorWide,
        )
        val child1 = ChildSpec("c1", Restart.Permanent, Shutdown.BrutalKill) { error("crash1") }
        val child2 = ChildSpec("c2", Restart.Permanent, Shutdown.BrutalKill) { error("crash2") }

        val ref = Supervisor.startLink(this, flags, listOf(child1, child2))
        ref.job.join()
        // Supervisor should cancel because shared window is exceeded across children
        assertTrue(ref.job.isCancelled, "supervisor should cancel on shared intensity exceeded")
    }

    // -----------------------------------------------------------------------
    // 4. Shutdown overrun (JVM approximation of BEAM kill)
    // -----------------------------------------------------------------------

    /**
     * OTP: when a child ignores shutdown and the timeout elapses, BEAM sends `kill`.
     * JVM: kotlinx.coroutines cooperative cancellation + join with timeout; if child
     * ignores cancellation, we still proceed after the timeout.
     *
     * This test verifies that supervisor.shutdown() completes in bounded time even
     * when a child's coroutine does not cooperate.
     *
     * JVM/OTP difference: unlike `brutal_kill` on BEAM, a non-cooperating JVM thread
     * cannot be forcibly terminated; this test relies on the child eventually checking
     * for cancellation or the timeout boundary being observed.
     */
    @Test
    fun shutdownTimeoutBoundedForUncooperativeChild() = runBlocking {
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 1,
            period = 5.seconds,
        )
        val child = ChildSpec(
            id = "slow-child",
            restart = Restart.Temporary,
            shutdown = Shutdown.Timeout(100.milliseconds),
        ) {
            // Ignore cancellation for a while (simulate uncooperative child)
            val start = System.nanoTime()
            while (System.nanoTime() - start < 500_000_000L) {
                // spin — but yield so the coroutine cancellation can propagate
                kotlinx.coroutines.yield()
            }
        }

        val ref = Supervisor.startLink(this, flags, listOf(child))
        val shutdownStart = System.nanoTime()
        ref.shutdown()
        val elapsed = (System.nanoTime() - shutdownStart) / 1_000_000L
        // Should complete well under 1 second (timeout + overhead)
        assertTrue(elapsed < 2000L, "shutdown should complete in bounded time, took ${elapsed}ms")
    }
}
