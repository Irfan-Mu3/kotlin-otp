package org.otpstudy.supervisor

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.ChildType
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class SupervisorWhichChildrenTest {

    @Test
    fun `whichChildren lists declared children with activity`() =
        runBlocking {
            val ref =
                Supervisor.startLink(
                    this,
                    SupervisorFlags(strategy = SupervisorStrategy.OneForOne, intensity = 5, period = 60.seconds),
                    listOf(
                        ChildSpec(
                            id = "w1",
                            restart = Restart.Permanent,
                            shutdown = Shutdown.Timeout(100.milliseconds),
                            type = ChildType.Worker,
                            start = { delay(60_000) },
                        ),
                    ),
                )
            delay(30)
            val ch = ref.whichChildren()
            assertEquals(1, ch.size)
            assertEquals("w1", ch[0].id)
            assertEquals(ChildType.Worker, ch[0].childType)
            assertTrue(ch[0].isActive)
            assertEquals(0, ch[0].restartCount)
            ref.shutdown()
        }
}
