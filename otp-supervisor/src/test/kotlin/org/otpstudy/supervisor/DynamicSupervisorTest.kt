package org.otpstudy.supervisor

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.TimeoutCancellationException
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
                    start = { _, _, ready ->
                        ready(Unit)
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
                    start = { _, _, ready ->
                        ready(Unit)
                        delay(60_000)
                    },
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

    @Test
    fun startChildSyncReturnsReadyValue() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate(
                    restart = Restart.Temporary,
                    shutdown = Shutdown.BrutalKill,
                ) { _, id, ready ->
                    ready("payload-$id")
                    delay(60_000)
                }
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            val (childId, payload) = ref.startChildSync<String>()
            assertTrue(payload.startsWith("payload-"))
            assertTrue(childId.startsWith("dyn-"))
            ref.shutdown()
        }

    @Test
    fun startChildSyncTimesOutWhenReadyNeverCalled() =
        runBlocking {
            val template =
                SimpleOneForOneTemplate<Unit>(
                    restart = Restart.Temporary,
                    shutdown = Shutdown.BrutalKill,
                ) { _, _, _ ->
                    delay(Long.MAX_VALUE)
                }
            val ref =
                DynamicSupervisor.startLink(
                    this,
                    SupervisorFlags(intensity = 10, period = 60.seconds),
                    template,
                )
            assertFailsWith<TimeoutCancellationException> {
                ref.startChildSync<Unit>(timeout = 100.milliseconds)
            }
            ref.shutdown()
        }
}
