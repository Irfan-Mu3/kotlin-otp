package org.otpstudy.jobs

import java.time.Instant

/**
 * Microsecond timestamps with OTP jobs epoch offset (see `jobs_lib:timestamp/0`).
 */
internal object JobsTimestamp {
    private const val EPOCH_OFFSET_US = 1258L * 1_000_000_000L

    fun now(): Long {
        val instant = Instant.now()
        val epochSecond = instant.epochSecond
        val micro = instant.nano / 1_000
        return (epochSecond * 1_000_000L + micro) - EPOCH_OFFSET_US
    }
}
