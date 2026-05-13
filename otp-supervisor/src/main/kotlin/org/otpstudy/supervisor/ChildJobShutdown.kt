package org.otpstudy.supervisor

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withTimeout
import org.otpstudy.core.Shutdown

/**
 * Stops a child [Job] according to OTP-style [Shutdown] (cooperative on the JVM).
 *
 * [Shutdown.BrutalKill] cancels and does not wait for cleanup (unlike BEAM `kill`).
 */
suspend fun stopChildJob(
    job: Job,
    shutdown: Shutdown,
) {
    when (shutdown) {
        Shutdown.BrutalKill -> {
            job.cancel(CancellationException("brutal_kill"))
        }
        is Shutdown.Timeout -> {
            job.cancel()
            withTimeout(shutdown.duration) { job.join() }
        }
        Shutdown.Infinity -> {
            job.cancel()
            job.join()
        }
    }
}
