package org.otpstudy.supervisor

import kotlinx.coroutines.delay
import kotlin.time.Duration

internal suspend fun delayDuration(d: Duration) {
    if (d <= Duration.ZERO) return
    val ms = d.inWholeMilliseconds
    if (ms > 0L) delay(ms)
    val remNs = d.inWholeNanoseconds - ms * 1_000_000L
    if (remNs > 0L) delay(1L)
}
