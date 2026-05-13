package org.otpstudy.core

import kotlin.time.Duration

/**
 * Shutdown policy for a child (OTP `shutdown` in child_spec).
 */
sealed class Shutdown {
    data object BrutalKill : Shutdown()

    data class Timeout(val duration: Duration) : Shutdown()

    data object Infinity : Shutdown()
}
