package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InfoMsg
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.GenServerRef
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private class EventCollectorServer : GenServer<MutableList<NodeEvent>> {
    override suspend fun init(self: GenServerRef<MutableList<NodeEvent>>) = InitResult.Ok(mutableListOf<NodeEvent>())
    override suspend fun handleCall(request: Any, state: MutableList<NodeEvent>) =
        ReplyResult.Reply<MutableList<NodeEvent>>(state.toList(), state)
    override suspend fun handleCast(request: Any, state: MutableList<NodeEvent>) =
        NoreplyResult.Noreply(state)
    override suspend fun handleInfo(msg: InfoMsg, state: MutableList<NodeEvent>): NoreplyResult<MutableList<NodeEvent>> {
        if (msg is NodeEvent) state.add(msg)
        return NoreplyResult.Noreply(state)
    }
}

class NodeMonitorTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        NodeMonitor.reset()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        NodeMonitor.reset()
    }

    @Test
    fun `NodeUp delivered on connect`() = runBlocking {
        val nodeA = NodeId("a")
        val nodeB = NodeId("b")
        val watcher = GenServers.startLink(scope, EventCollectorServer())

        NodeMonitor.monitorNode(nodeA, watcher)

        val transport = InMemoryTransport()
        transport.connect(LocalNode(nodeA), LocalNode(nodeB))

        delay(50)
        @Suppress("UNCHECKED_CAST")
        val events = watcher.call<List<NodeEvent>>("get")
        assertTrue(events.any { it is NodeEvent.NodeUp && it.nodeId == nodeA })
        watcher.stop()
    }

    @Test
    fun `NodeDown delivered on disconnect`() = runBlocking {
        val nodeId = NodeId("target")
        val watcher = GenServers.startLink(scope, EventCollectorServer())

        NodeMonitor.monitorNode(nodeId, watcher)
        NodeMonitor.notifyDown(nodeId, "test-reason")

        delay(50)
        @Suppress("UNCHECKED_CAST")
        val events = watcher.call<List<NodeEvent>>("get")
        assertTrue(events.any { it is NodeEvent.NodeDown && it.nodeId == nodeId })
        watcher.stop()
    }

    @Test
    fun `disconnect removes subscription after notifying`() = runBlocking {
        val nodeId = NodeId("gone")
        val watcher = GenServers.startLink(scope, EventCollectorServer())

        NodeMonitor.monitorNode(nodeId, watcher)
        NodeMonitor.notifyDown(nodeId, "shutdown")
        // Second notifyDown should have no subscribers left
        NodeMonitor.notifyDown(nodeId, "second")

        delay(50)
        @Suppress("UNCHECKED_CAST")
        val events = watcher.call<List<NodeEvent>>("get")
        assertEquals(1, events.count { it is NodeEvent.NodeDown }, "should receive exactly one NodeDown")
        watcher.stop()
    }

    @Test
    fun `subscription removed on close`() = runBlocking {
        val nodeId = NodeId("watched")
        val watcher = GenServers.startLink(scope, EventCollectorServer())

        val sub = NodeMonitor.monitorNode(nodeId, watcher)
        sub.close()
        NodeMonitor.notifyUp(nodeId)

        delay(50)
        @Suppress("UNCHECKED_CAST")
        val events = watcher.call<List<NodeEvent>>("get")
        assertTrue(events.isEmpty(), "no events after close")
        watcher.stop()
    }

    @Test
    fun `multiple watchers each receive NodeDown`() = runBlocking {
        val nodeId = NodeId("multi")
        val w1 = GenServers.startLink(scope, EventCollectorServer())
        val w2 = GenServers.startLink(scope, EventCollectorServer())

        NodeMonitor.monitorNode(nodeId, w1)
        NodeMonitor.monitorNode(nodeId, w2)
        NodeMonitor.notifyDown(nodeId, "gone")

        delay(50)
        @Suppress("UNCHECKED_CAST")
        assertTrue(w1.call<List<NodeEvent>>("get").any { it is NodeEvent.NodeDown })
        @Suppress("UNCHECKED_CAST")
        assertTrue(w2.call<List<NodeEvent>>("get").any { it is NodeEvent.NodeDown })
        w1.stop()
        w2.stop()
    }
}
