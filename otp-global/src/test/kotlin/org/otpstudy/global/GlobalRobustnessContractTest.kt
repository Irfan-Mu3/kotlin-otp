package org.otpstudy.global

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.distribution.GlobalDistMsg
import org.otpstudy.distribution.GlobalReplicationBus
import org.otpstudy.distribution.InMemoryTransport
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

private class ContractNoopServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class GlobalRobustnessContractTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        GlobalRegistry.reset()
        GlobalRegistry.resetMetrics()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        GlobalRegistry.reset()
    }

    @Test
    fun `deterministic contract - latest version wins over stale replay`() = runBlocking {
        val suffix = System.nanoTime().toString()
        val nodeA = LocalNode(NodeId("gca-$suffix", "global"))
        val nodeB = LocalNode(NodeId("gcb-$suffix", "global"))
        val transport = InMemoryTransport()
        transport.addNode(nodeA)
        transport.addNode(nodeB)
        GlobalRegistry.install(transport, nodeA)
        GlobalRegistry.useNode(nodeB)
        transport.connect(nodeA, nodeB)

        val wire = "${nodeA.id.name}@${nodeA.id.host}"
        val pid = 101L
        val regV1 = GlobalDistMsg.Register("contract-name", wire, "contract-name", pid, version = 1)
        val unregV2 = GlobalDistMsg.Unregister("contract-name", wire, pid, version = 2)

        val handler = GlobalReplicationBus.handler!!
        handler.onMessage(regV1, nodeA.id)
        handler.onMessage(unregV2, nodeA.id)
        handler.onMessage(regV1, nodeA.id)
        assertNull(GlobalRegistry.resolveName("contract-name"))
    }

    @Test
    fun `adversarial contract - mixed replay churn drops stale messages`() = runBlocking {
        val suffix = System.nanoTime().toString()
        val nodeA = LocalNode(NodeId("gra-$suffix", "global"))
        val nodeB = LocalNode(NodeId("grb-$suffix", "global"))
        val transport = InMemoryTransport()
        transport.addNode(nodeA)
        transport.addNode(nodeB)
        GlobalRegistry.install(transport, nodeA)
        GlobalRegistry.useNode(nodeB)
        transport.connect(nodeA, nodeB)

        val wire = "${nodeA.id.name}@${nodeA.id.host}"
        val pid = 202L
        val regV1 = GlobalDistMsg.Register("replay", wire, "replay", pid, version = 1)
        val regV3 = GlobalDistMsg.Register("replay", wire, "replay", pid, version = 3)
        val unregV4 = GlobalDistMsg.Unregister("replay", wire, pid, version = 4)
        val staleSnapshot = GlobalDistMsg.SyncSnapshot(listOf(regV1))
        val handler = GlobalReplicationBus.handler!!
        handler.onMessage(regV1, nodeA.id)
        handler.onMessage(regV3, nodeA.id)
        handler.onMessage(unregV4, nodeA.id)
        handler.onMessage(staleSnapshot, nodeA.id)

        val metrics = GlobalRegistry.metricsSnapshot()
        assertTrue(metrics.staleMessagesDropped >= 1L)
        assertNull(GlobalRegistry.resolveName("replay"))
    }

    @Test
    fun `recovery contract - peer sync snapshot converges and records latency`() = runBlocking {
        val suffix = System.nanoTime().toString()
        val nodeA = LocalNode(NodeId("gsa-$suffix", "global"))
        val nodeB = LocalNode(NodeId("gsb-$suffix", "global"))
        val transport = InMemoryTransport()
        transport.addNode(nodeA)
        transport.addNode(nodeB)
        GlobalRegistry.install(transport, nodeA)

        val ref = org.otpstudy.genserver.GenServers.startLink(scope, ContractNoopServer())
        GlobalRegistry.useNode(nodeA)
        GlobalRegistry.registerName("sync-contract", ref)
        transport.connect(nodeA, nodeB)
        delay(20)

        GlobalRegistry.useNode(nodeB)
        assertNotNull(GlobalRegistry.resolveName("sync-contract"))
        val metrics = GlobalRegistry.metricsSnapshot()
        assertTrue(metrics.syncBroadcasts >= 1L)
        assertNotNull(metrics.lastSyncBroadcastLatencyMillis)
    }
}
