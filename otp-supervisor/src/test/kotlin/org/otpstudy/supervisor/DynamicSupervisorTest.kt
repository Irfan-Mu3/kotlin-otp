package org.otpstudy.supervisor

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class DynamicSupervisorTest {
    @Test
    fun startChildAndWhichChildren() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Permanent,
                    shutdown = Shutdown.Timeout(100.milliseconds),
                    start = { id ->
                        delay(60_000)
                    },
                )
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val a = ref.startChild()
            val b = ref.startChild()
            val list = ref.whichChildren()
            assertEquals(2, ref.countChildren())
            assertTrue(list.any { it.id == a })
            assertTrue(list.any { it.id == b })
            ref.shutdown()
        }

    @Test
    fun terminateChildRemoves() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Permanent,
                    shutdown = Shutdown.Timeout(100.milliseconds),
                    start = { delay(60_000) },
                )
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val id = ref.startChild()
            assertEquals(1, ref.countChildren())
            ref.terminateChild(id)
            assertEquals(0, ref.countChildren())
            ref.shutdown()
        }
}
