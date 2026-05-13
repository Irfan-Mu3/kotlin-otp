package org.otpstudy.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel

/**
 * Library-level monitor (OTP `erlang:monitor/2` flavour): when [job] completes, a [DownMessage] is delivered to [down].
 */
data class DownMessage(
    val monitoredJob: Job,
    val cause: Throwable?,
)

class ProcessMonitor(
    val job: Job,
    val down: Channel<DownMessage> = Channel(Channel.UNLIMITED),
) {
    private val disposable: DisposableHandle =
        job.invokeOnCompletion { cause ->
            val normalized: Throwable? =
                when (cause) {
                    null -> null
                    is CancellationException -> cause
                    else -> cause
                }
            down.trySend(DownMessage(job, normalized))
        }

    fun dispose() {
        disposable.dispose()
    }
}

/**
 * Links two [Job]s so that when [primary] completes, [dependent] is cancelled (cooperative; not BEAM links).
 */
fun linkJobs(
    primary: Job,
    dependent: Job,
) {
    primary.invokeOnCompletion { cause ->
        dependent.cancel(
            CancellationException("linked primary completed", cause),
        )
    }
}
