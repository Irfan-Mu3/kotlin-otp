package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.time.Duration

class TimerRef internal constructor(private val job: Job) {
    fun cancel() { job.cancel() }
    val isActive: Boolean get() = job.isActive
}

object OtpTimers {
    fun sendAfter(
        scope: CoroutineScope,
        delay: Duration,
        target: GenServerRef<*>,
        msg: InfoMsg = TimerTick(ref = Unit),
    ): TimerRef {
        val job = scope.launch {
            kotlinx.coroutines.delay(delay)
            target.sendInfo(msg)
        }
        return TimerRef(job)
    }

    fun sendInterval(
        scope: CoroutineScope,
        interval: Duration,
        target: GenServerRef<*>,
        msg: InfoMsg = TimerTick(ref = Unit),
        initialDelay: Duration = interval,
    ): TimerRef {
        val job = scope.launch {
            kotlinx.coroutines.delay(initialDelay)
            while (isActive) {
                target.sendInfo(msg)
                kotlinx.coroutines.delay(interval)
            }
        }
        return TimerRef(job)
    }
}
