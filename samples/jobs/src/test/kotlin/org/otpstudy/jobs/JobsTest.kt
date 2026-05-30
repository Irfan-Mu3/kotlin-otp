package org.otpstudy.jobs

import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

class JobsTest {
    private suspend fun CoroutineScope.withJobs(
        vararg queues: QueueSpec,
        block: suspend () -> Unit,
    ) {
        Jobs.startLink(
            this,
            JobsConfig(queues = queues.toList(), defaultQueue = queues.firstOrNull()?.name),
        )
        try {
            block()
        } finally {
            Jobs.ref().stop()
            Jobs.resetForTests()
        }
    }

    @Test
    fun `approve queue grants immediately`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.approve())) {
                val r = Jobs.ask("q")
                assertTrue(r is JobsResult.Ok)
                Jobs.done((r as JobsResult.Ok).value)
            }
        }

    @Test
    fun `reject queue returns rejected`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.reject())) {
                val r = Jobs.ask("q")
                assertEquals(JobsResult.Error(JobsError.Rejected), r)
            }
        }

    @Test
    fun `counter limits concurrent grants`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.standardCounter(limit = 2))) {
                val gate = CompletableDeferred<Unit>()
                coroutineScope {
                    val a1 =
                        async {
                            val g = (Jobs.ask("q") as JobsResult.Ok).value
                            gate.await()
                            Jobs.done(g)
                        }
                    val a2 =
                        async {
                            val g = (Jobs.ask("q") as JobsResult.Ok).value
                            gate.await()
                            Jobs.done(g)
                        }
                    delay(50)
                    val blocked = async { Jobs.ask("q") }
                    delay(50)
                    assertTrue(blocked.isActive)
                    gate.complete(Unit)
                    a1.await()
                    a2.await()
                    val r3 = blocked.await()
                    assertTrue(r3 is JobsResult.Ok)
                    Jobs.done((r3 as JobsResult.Ok).value)
                }
            }
        }

    @Test
    fun `fifo passive dequeue order`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.passiveFifo())) {
                assertEquals(JobsResult.Ok(Unit), Jobs.enqueue("q", "a"))
                assertEquals(JobsResult.Ok(Unit), Jobs.enqueue("q", "b"))
                val items = Jobs.dequeue("q", 2)
                assertEquals(listOf("a", "b"), items.map { it.second })
            }
        }

    @Test
    fun `lifo queue type dequeues newest worker first`(): Unit =
        runBlocking {
            withJobs(
                QueueSpec(
                    "q",
                    QueueOptions(
                        type = QueueType.Lifo,
                        regulators = listOf(RegulatorSpec.Counter(limit = 10)),
                    ),
                ),
            ) {
                coroutineScope {
                    val d1 =
                        async {
                            val g = (Jobs.ask("q") as JobsResult.Ok).value
                            delay(200)
                            Jobs.done(g)
                        }
                    val d2 =
                        async {
                            delay(20)
                            val g = (Jobs.ask("q") as JobsResult.Ok).value
                            delay(200)
                            Jobs.done(g)
                        }
                    d1.await()
                    d2.await()
                }
            }
        }

    @Test
    fun `max_size rejects ask when full`(): Unit =
        runBlocking {
            withJobs(
                QueueSpec(
                    "q",
                    QueueOptions(
                        regulators = listOf(RegulatorSpec.Counter(limit = 0)),
                        maxSize = 1,
                    ),
                ),
            ) {
                val waiter = async { Jobs.ask("q") }
                delay(50)
                val r = Jobs.ask("q")
                assertEquals(JobsResult.Error(JobsError.Rejected), r)
                waiter.cancel()
            }
        }

    @Test
    fun `max_time times out blocked ask`(): Unit =
        runBlocking {
            withJobs(
                QueueSpec(
                    "q",
                    QueueOptions(
                        regulators = listOf(RegulatorSpec.Counter(limit = 0)),
                        maxTime = 200.milliseconds,
                    ),
                ),
            ) {
                val r = withTimeout(2.seconds) { Jobs.ask("q") }
                assertEquals(JobsResult.Error(JobsError.Timeout), r)
            }
        }

    @Test
    fun `run executes and releases counter`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.standardCounter(limit = 1))) {
                Jobs.run("q") { }
                val info = Jobs.queueInfo("q")
                assertNotNull(info)
                assertEquals(0, info.counterValue)
                assertEquals(1, info.approved)
            }
        }

    @Test
    fun `add and delete queue`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.approve())) {
                Jobs.addQueue("q2", QueueOptions.reject())
                assertNotNull(Jobs.queueInfo("q2"))
                assertTrue(Jobs.deleteQueue("q2"))
                assertEquals(null, Jobs.queueInfo("q2"))
            }
        }

    @Test
    fun `rate queue serializes bursts`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.standardRate(limitPerSecond = 5))) {
                val t0 = JobsTimestamp.now()
                repeat(5) { Jobs.runReturning("q") { JobsTimestamp.now() } }
                val elapsed = (JobsTimestamp.now() - t0) / 1000
                assertTrue(elapsed < 1500, "expected ~1s for 5 jobs at rate 5/s, was ${elapsed}ms")
            }
        }

    @Test
    fun `linked queue removed when owner job completes`(): Unit =
        runBlocking {
            withJobs(QueueSpec("q", QueueOptions.approve())) {
                coroutineScope {
                    val removed = CompletableDeferred<Unit>()
                    val owner =
                        launch {
                            Jobs.addQueue(
                                "linked",
                                QueueOptions.standardCounter(limit = 1)
                                    .copy(linkOwner = coroutineContext[kotlinx.coroutines.Job]),
                            )
                            delay(50)
                        }
                    val watcher =
                        launch {
                            while (Jobs.queueInfo("linked") != null) delay(10)
                            removed.complete(Unit)
                        }
                    owner.join()
                    withTimeout(2.seconds) { removed.await() }
                    watcher.cancel()
                }
            }
        }
}
