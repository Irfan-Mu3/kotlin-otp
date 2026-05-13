package org.otpstudy.core

import java.util.concurrent.atomic.AtomicLong

/**
 * Stable logical identity for a "process" (actor / gen_server / worker).
 * Not a BEAM PID; used for logging, naming, and correlation.
 */
@JvmInline
value class OtpProcessId(val value: Long) {
    companion object {
        private val next = AtomicLong(1L)

        fun allocate(): OtpProcessId = OtpProcessId(next.getAndIncrement())
    }

    override fun toString(): String = "OtpProcessId($value)"
}
