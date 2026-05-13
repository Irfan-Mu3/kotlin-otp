package org.otpstudy.supervisor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class SupervisorStrategyTest {
    @Test
    fun oneForOneRestartsOnlyFailedChild() =
        runBlocking {
            val order = mutableListOf<String>()
            val children =
                listOf(
                    ChildSpec(
                        id = "a",
                        restart = Restart.Permanent,
                        shutdown = Shutdown.Timeout(100.milliseconds),
                        start = {
                            order.add("a-start")
                            delay(20)
                            error("fail-a")
                        },
                    ),
                    ChildSpec(
                        id = "b",
                        restart = Restart.Permanent,
                        shutdown = Shutdown.Timeout(100.milliseconds),
                        start = {
                            order.add("b-start")
                            delay(60_000)
                        },
                    ),
                )
            val ref =
                Supervisor.startLink(
                    this,
                    SupervisorFlags(strategy = SupervisorStrategy.OneForOne, intensity = 50, period = 60.seconds),
                    children,
                )
            delay(100)
            assertTrue(order.count { it == "a-start" } >= 2, "expected a to restart, order=$order")
            assertEquals(1, order.count { it == "b-start" }, "b should not restart, order=$order")
            ref.shutdown()
        }

    @Test
    fun oneForAllRestartsAllWhenOneFails() =
        runBlocking {
            val starts = mutableListOf<String>()
            fun child(
                id: String,
                fail: Boolean,
            ): suspend CoroutineScope.() -> Unit =
                {
                    starts.add(id)
                    if (fail) {
                        delay(15)
                        error("boom-$id")
                    }
                    delay(60_000)
                }
            val children =
                listOf(
                    ChildSpec("a", Restart.Permanent, shutdown = Shutdown.Timeout(100.milliseconds), start = child("a", fail = false)),
                    ChildSpec("b", Restart.Permanent, shutdown = Shutdown.Timeout(100.milliseconds), start = child("b", fail = true)),
                    ChildSpec("c", Restart.Permanent, shutdown = Shutdown.Timeout(100.milliseconds), start = child("c", fail = false)),
                )
            val ref =
                Supervisor.startLink(
                    this,
                    SupervisorFlags(strategy = SupervisorStrategy.OneForAll, intensity = 5, period = 60.seconds),
                    children,
                )
            delay(150)
            val aCount = starts.count { it == "a" }
            val bCount = starts.count { it == "b" }
            val cCount = starts.count { it == "c" }
            assertTrue(aCount >= 2 && bCount >= 2 && cCount >= 2, "all should restart after b fails: $starts")
            ref.shutdown()
        }

    @Test
    fun restForOneRestartsSuffix() =
        runBlocking {
            val starts = mutableListOf<String>()
            fun child(
                id: String,
                fail: Boolean,
            ): suspend CoroutineScope.() -> Unit =
                {
                    starts.add(id)
                    if (fail) {
                        delay(15)
                        error("boom-$id")
                    }
                    delay(60_000)
                }
            val children =
                listOf(
                    ChildSpec("a", Restart.Permanent, shutdown = Shutdown.Timeout(100.milliseconds), start = child("a", fail = false)),
                    ChildSpec("b", Restart.Permanent, shutdown = Shutdown.Timeout(100.milliseconds), start = child("b", fail = true)),
                    ChildSpec("c", Restart.Permanent, shutdown = Shutdown.Timeout(100.milliseconds), start = child("c", fail = false)),
                )
            val ref =
                Supervisor.startLink(
                    this,
                    SupervisorFlags(strategy = SupervisorStrategy.RestForOne, intensity = 5, period = 60.seconds),
                    children,
                )
            delay(150)
            assertEquals(1, starts.count { it == "a" }, "older sibling should not restart: $starts")
            assertTrue(starts.count { it == "b" } >= 2, "b should restart: $starts")
            assertTrue(starts.count { it == "c" } >= 2, "younger sibling should restart: $starts")
            ref.shutdown()
        }

    @Test
    fun shutdownStopsChildrenInReverseStartOrder() =
        runBlocking {
            val stops = mutableListOf<String>()
            fun child(id: String): suspend CoroutineScope.() -> Unit =
                {
                    try {
                        delay(60_000)
                    } finally {
                        stops.add(id)
                    }
                }
            val children =
                listOf(
                    ChildSpec("first", Restart.Permanent, shutdown = Shutdown.Timeout(50.milliseconds), start = child("first")),
                    ChildSpec("second", Restart.Permanent, shutdown = Shutdown.Timeout(50.milliseconds), start = child("second")),
                    ChildSpec("third", Restart.Permanent, shutdown = Shutdown.Timeout(50.milliseconds), start = child("third")),
                )
            val ref =
                Supervisor.startLink(
                    this,
                    SupervisorFlags(),
                    children,
                )
            delay(40)
            ref.shutdown()
            assertEquals(listOf("third", "second", "first"), stops)
        }
}
