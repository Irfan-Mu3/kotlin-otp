package org.otpstudy.supervisor

import kotlin.time.Duration

/**
 * Optional delay between restart attempts (after intensity allows another restart).
 */
sealed class RestartBackoff {
    data object None : RestartBackoff()

    data class Fixed(
        val delay: Duration,
    ) : RestartBackoff()

    data class Exponential(
        val initial: Duration,
        val max: Duration,
        val factor: Double = 2.0,
    ) : RestartBackoff()
}
