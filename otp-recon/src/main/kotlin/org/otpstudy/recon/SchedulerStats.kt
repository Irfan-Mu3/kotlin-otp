package org.otpstudy.recon

import java.lang.management.ManagementFactory

/**
 * Per-scheduler utilisation — analogous to erlang:statistics(scheduler_wall_time).
 *
 * Returns a snapshot of JVM thread activity for the coroutine dispatchers.
 * Two snapshots taken some time apart give a utilisation estimate.
 *
 * OTP source: erts/emulator/beam/erl_process.c — statistics_scheduler_wall_time
 */
object SchedulerStats {
    data class Sample(
        val timestamp: Long,
        val activeThreads: Int,
        val poolSize: Int,
    )

    /** Capture a utilisation sample. */
    fun sample(): Sample {
        val mxBean = ManagementFactory.getThreadMXBean()
        val available = Runtime.getRuntime().availableProcessors()
        return Sample(System.nanoTime(), mxBean.threadCount, available)
    }

    /**
     * Estimate utilisation from two samples as `activeThreads / poolSize`.
     *
     * Values above 1.0 indicate more threads than processors (i.e. context switching).
     * Returns 0.0 if poolSize is zero.
     */
    fun utilisation(before: Sample, after: Sample): Double =
        if (after.poolSize == 0) 0.0
        else after.activeThreads.toDouble() / after.poolSize.toDouble()
}
