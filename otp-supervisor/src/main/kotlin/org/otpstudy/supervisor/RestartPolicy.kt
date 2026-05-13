package org.otpstudy.supervisor

import org.otpstudy.core.Restart
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.ZERO

/**
 * Pure helpers mirroring OTP restart semantics and restart intensity checks.
 */
object RestartPolicy {
    fun shouldRestart(
        restart: Restart,
        abnormal: Boolean,
    ): Boolean =
        when (restart) {
            Restart.Permanent -> true
            Restart.Temporary -> false
            Restart.Transient -> abnormal
        }

    /**
     * Returns whether another restart is allowed before exceeding [intensity]
     * restarts in the sliding [period] window (OTP `intensity` + `period`).
     *
     * @param restartTimestampsNanos monotonically appended timestamps on each restart
     * @param nowNanos typically [System.nanoTime]
     */
    fun canRestart(
        restartTimestampsNanos: List<Long>,
        nowNanos: Long,
        intensity: Int,
        period: Duration,
    ): Boolean {
        val windowNanos = period.inWholeNanoseconds
        val cutoff = nowNanos - windowNanos
        val countInWindow = restartTimestampsNanos.count { it >= cutoff }
        return countInWindow < intensity
    }

    /**
     * Delay to apply after intensity allows a restart and before starting the replacement child.
     * [restartCountSoFar] is the number of restarts already recorded for this child (e.g. list size).
     */
    fun backoffDuration(
        backoff: RestartBackoff,
        restartCountSoFar: Int,
    ): Duration =
        when (backoff) {
            RestartBackoff.None -> ZERO
            is RestartBackoff.Fixed -> backoff.delay.coerceAtLeast(ZERO)
            is RestartBackoff.Exponential -> {
                val cappedAttempts = restartCountSoFar.coerceIn(0, 64)
                val mult = backoff.factor.pow(cappedAttempts.toDouble())
                val scaled =
                    if (!mult.isFinite()) {
                        backoff.max
                    } else {
                        backoff.initial * mult
                    }
                minOf(scaled, backoff.max).coerceAtLeast(ZERO)
            }
        }
}
