package org.otpstudy.supervisor

import org.otpstudy.core.Restart
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RestartPolicyTest {
    @Test
    fun permanentAlwaysRestarts() {
        assertTrue(RestartPolicy.shouldRestart(Restart.Permanent, abnormal = false))
        assertTrue(RestartPolicy.shouldRestart(Restart.Permanent, abnormal = true))
    }

    @Test
    fun temporaryNeverRestarts() {
        assertFalse(RestartPolicy.shouldRestart(Restart.Temporary, abnormal = false))
        assertFalse(RestartPolicy.shouldRestart(Restart.Temporary, abnormal = true))
    }

    @Test
    fun transientOnlyOnAbnormal() {
        assertFalse(RestartPolicy.shouldRestart(Restart.Transient, abnormal = false))
        assertTrue(RestartPolicy.shouldRestart(Restart.Transient, abnormal = true))
    }

    @Test
    fun intensityBlocksWhenWindowFull() {
        val period = 5.seconds
        val intensity = 3
        val now = 100_000_000L
        assertTrue(RestartPolicy.canRestart(emptyList(), now, intensity, period))
        assertTrue(RestartPolicy.canRestart(listOf(now - 2, now - 1), now, intensity, period))
        assertFalse(RestartPolicy.canRestart(listOf(now - 3, now - 2, now - 1), now, intensity, period))
    }

    @Test
    fun oldRestartsFallOutsideWindow() {
        val period = 2.seconds
        val intensity = 1
        val old = 0L
        val now = period.inWholeNanoseconds + 10
        assertTrue(RestartPolicy.canRestart(listOf(old), now, intensity, period))
    }

    @Test
    fun backoffNoneIsZero() {
        assertEquals(
            kotlin.time.Duration.ZERO,
            RestartPolicy.backoffDuration(RestartBackoff.None, restartCountSoFar = 3),
        )
    }

    @Test
    fun backoffFixedReturnsDelay() {
        assertEquals(
            200.milliseconds,
            RestartPolicy.backoffDuration(RestartBackoff.Fixed(200.milliseconds), 0),
        )
    }

    @Test
    fun backoffExponentialGrowsAndCaps() {
        val b = RestartBackoff.Exponential(initial = 10.milliseconds, max = 25.milliseconds, factor = 2.0)
        val d0 = RestartPolicy.backoffDuration(b, 0)
        val d1 = RestartPolicy.backoffDuration(b, 1)
        val d2 = RestartPolicy.backoffDuration(b, 2)
        assertTrue(d1 > d0)
        assertEquals(25.milliseconds, d2)
    }
}
