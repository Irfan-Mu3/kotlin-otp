package org.otpstudy.observer

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.ChildType
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.supervisor.ChildSpec
import org.otpstudy.supervisor.Supervisor
import org.otpstudy.supervisor.SupervisorFlags
import org.otpstudy.supervisor.SupervisorStrategy
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class SupervisorTreeNestedTest {

    @Test
    fun `nested map attaches inner whichChildren under outer child id`() =
        runBlocking {
            val inner =
                Supervisor.startLink(
                    this,
                    SupervisorFlags(strategy = SupervisorStrategy.OneForOne, intensity = 5, period = 60.seconds),
                    listOf(
                        ChildSpec(
                            id = "leaf",
                            restart = Restart.Permanent,
                            shutdown = Shutdown.Timeout(100.milliseconds),
                            type = ChildType.Worker,
                            start = { delay(60_000) },
                        ),
                    ),
                )
            val outer =
                Supervisor.startLink(
                    this,
                    SupervisorFlags(strategy = SupervisorStrategy.OneForOne, intensity = 5, period = 60.seconds),
                    listOf(
                        ChildSpec(
                            id = "mid",
                            restart = Restart.Permanent,
                            shutdown = Shutdown.Timeout(100.milliseconds),
                            type = ChildType.Worker,
                            start = { delay(60_000) },
                        ),
                    ),
                )
            delay(30)
            val tree = SupervisorTree.fromSupervisorRef(outer, "root", mapOf("mid" to inner))
            val mid = tree.children.single { it.label == "mid" }
            assertTrue(mid.children.any { it.label == "leaf" })
            assertTrue(SupervisorTree.toAscii(tree).contains("leaf"))
            inner.shutdown()
            outer.shutdown()
        }
}
