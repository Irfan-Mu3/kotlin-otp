package org.otpstudy.recon

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.observer.OtpObserver
import org.otpstudy.observer.ProcessTable
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private class NoopServer : GenServer<Unit> {
    override suspend fun init() = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class ReconTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        ProcessTable.clear()
        OtpObserver.install()
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        ProcessTable.clear()
    }

    @Test
    fun `procCount returns top n by reductions`() = runTest {
        repeat(5) { GenServers.startLink(scope, NoopServer()) }
        val top3 = Recon.procCount(Recon.ProcessAttribute.Reductions, 3)
        assertEquals(3, top3.size)
        // Should be sorted descending
        for (i in 0 until top3.size - 1) {
            assertTrue(top3[i].value >= top3[i + 1].value)
        }
    }

    @Test
    fun `procCount returns top n by queue length`() = runTest {
        GenServers.startLink(scope, NoopServer())
        val top1 = Recon.procCount(Recon.ProcessAttribute.MessageQueueLen, 1)
        assertEquals(1, top1.size)
        assertTrue(top1.first().value >= 0)
    }

    @Test
    fun `procCount with n larger than process count returns all`() = runTest {
        repeat(3) { GenServers.startLink(scope, NoopServer()) }
        val all = Recon.procCount(Recon.ProcessAttribute.Reductions, 100)
        assertTrue(all.size >= 3)
    }

    @Test
    fun `procList returns all actors sorted descending`() = runTest {
        repeat(4) { GenServers.startLink(scope, NoopServer()) }
        val list = Recon.procList(Recon.ProcessAttribute.Reductions)
        assertTrue(list.size >= 4)
        for (i in 0 until list.size - 1) {
            assertTrue(list[i].value >= list[i + 1].value)
        }
    }

    @Test
    fun `procWindow returns delta over window`() = runTest {
        GenServers.startLink(scope, NoopServer())
        val window = Recon.procWindow(Recon.ProcessAttribute.Reductions, 5, 50L)
        assertTrue(window.isNotEmpty())
        for (entry in window) {
            assertTrue(entry.delta >= 0)
        }
    }

    @Test
    fun `ProcessInfoEntry contains correct info`() = runTest {
        GenServers.startLink(scope, NoopServer(), name = "recon-test-actor")
        val top = Recon.procCount(Recon.ProcessAttribute.Reductions, 100)
        val entry = top.firstOrNull { it.info.name == "recon-test-actor" }
        assertTrue(entry != null, "expected 'recon-test-actor' in proc list")
        assertEquals("recon-test-actor", entry!!.info.name)
    }
}
