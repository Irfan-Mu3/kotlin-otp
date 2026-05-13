package org.otpstudy.observer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.otpstudy.core.OtpProcessId
import org.otpstudy.ets.OtpTableRegistry
import org.otpstudy.ets.TableType
import org.otpstudy.genserver.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class OtpObserverTest {

    @Test
    fun `inspectProcess returns current state via sys`() = runTest {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val server = object : GenServer<String> {
            override suspend fun init() = InitResult.Ok("observed-state")
            override suspend fun handleCall(r: Any, s: String) = ReplyResult.Reply(s, s)
            override suspend fun handleCast(r: Any, s: String) = NoreplyResult.Noreply(s)
        }
        val ref = GenServers.startLink(scope, server, name = "observable")
        val snap = OtpObserver.inspectProcess(ref)
        assertEquals("observable", snap.name)
        assertEquals("observed-state", snap.state)
        scope.cancel()
    }

    @Test
    fun `tableStats includes registered tables`() {
        val t = OtpTableRegistry.new<String, Int>("obs-tbl")
        t.insert("k", 42)
        val stats = OtpObserver.tableStats()
        val found = stats.firstOrNull { it.name == "obs-tbl" }
        assertNotNull(found)
        assertEquals(1, found.size)
    }

    @Test
    fun `ProcessTable register probe and list`() {
        ProcessTable.clear()
        val id = OtpProcessId.allocate()
        val handle = ProcessTable.register(id) {
            ProcessInfo(id, "test-proc", ProcessStatus.Running, "TestServer")
        }
        val all = ProcessTable.all()
        assertEquals(1, all.size)
        assertEquals("test-proc", all[0].name)
        assertEquals(ProcessStatus.Running, all[0].status)
        handle.close()
        assertTrue(ProcessTable.all().isEmpty())
    }

    @Test
    fun `ProcessTable info returns null for unknown id`() {
        ProcessTable.clear()
        val id = OtpProcessId.allocate()
        assertEquals(null, ProcessTable.info(id))
    }
}
