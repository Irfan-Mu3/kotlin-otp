package org.otpstudy.core

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class ProcessMonitorTest {
    @Test
    fun deliversDownWhenJobCompletes() =
        runBlocking {
            val down = Channel<DownMessage>(Channel.UNLIMITED)
            val job = launch { }
            ProcessMonitor(job, down)
            job.join()
            val msg = withTimeout(2.seconds) { down.receive() }
            assertEquals(job, msg.monitoredJob)
            assertTrue(msg.cause == null)
        }
}
