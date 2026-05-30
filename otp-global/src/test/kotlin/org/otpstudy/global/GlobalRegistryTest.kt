package org.otpstudy.global

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.otpstudy.distribution.InMemoryTransport
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.distribution.GlobalDistMsg
import org.otpstudy.distribution.GlobalReplicationBus
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.GenServerRef
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

private class NoopServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class GlobalRegistryTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        GlobalRegistry.reset()
        GlobalRegistry.conflictResolver = GlobalRegistry.ConflictResolver.KeepFirst
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        GlobalRegistry.reset()
    }

    @Test
    fun `registerName succeeds for a new name`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        val result = GlobalRegistry.registerName("alice", ref)
        assertEquals(GlobalRegistry.RegisterResult.Ok, result)
    }

    @Test
    fun `whereisName returns registered ref`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("alice", ref)
        assertSame(ref, GlobalRegistry.whereisName<Unit>("alice"))
    }

    @Test
    fun `whereisName returns null for unknown name`() = runTest {
        assertNull(GlobalRegistry.whereisName<Unit>("nobody"))
    }

    @Test
    fun `KeepFirst returns Conflict on duplicate`() = runTest {
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("srv", r1)
        val result = GlobalRegistry.registerName("srv", r2)
        assertTrue(result is GlobalRegistry.RegisterResult.Conflict)
        assertSame(r1, (result as GlobalRegistry.RegisterResult.Conflict).existing)
        assertSame(r1, GlobalRegistry.whereisName<Unit>("srv"))
    }

    @Test
    fun `KeepLast replaces on duplicate`() = runTest {
        GlobalRegistry.conflictResolver = GlobalRegistry.ConflictResolver.KeepLast
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("srv", r1)
        val result = GlobalRegistry.registerName("srv", r2)
        assertEquals(GlobalRegistry.RegisterResult.Ok, result)
        assertSame(r2, GlobalRegistry.whereisName<Unit>("srv"))
    }

    @Test
    fun `Custom resolver picks winner`() = runTest {
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.conflictResolver = GlobalRegistry.ConflictResolver.Custom { _, _, incoming -> incoming }
        GlobalRegistry.registerName("srv", r1)
        GlobalRegistry.registerName("srv", r2)
        assertSame(r2, GlobalRegistry.whereisName<Unit>("srv"))
    }

    @Test
    fun `unregisterName removes entry`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("temp", ref)
        GlobalRegistry.unregisterName("temp")
        assertNull(GlobalRegistry.whereisName<Unit>("temp"))
    }

    @Test
    fun `name auto-unregistered when actor stops`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("dying", ref)
        ref.stop()
        ref.job.join()
        assertNull(GlobalRegistry.whereisName<Unit>("dying"))
    }

    @Test
    fun register_replicates_to_peer(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val nodeA = LocalNode(NodeId("ga-$suffix", "global"))
            val nodeB = LocalNode(NodeId("gb-$suffix", "global"))
            val transport = InMemoryTransport()
            transport.addNode(nodeA)
            transport.addNode(nodeB)
            GlobalRegistry.install(transport, nodeA)
            GlobalRegistry.useNode(nodeB)
            transport.connect(nodeA, nodeB)
            val ref = GenServers.startLink(scope, NoopServer())
            GlobalRegistry.useNode(nodeA)
            GlobalRegistry.registerName("shared", ref)
            delay(20)
            GlobalRegistry.useNode(nodeB)
            val resolved = GlobalRegistry.resolveName("shared")
            assertTrue(resolved is GlobalRegistry.NameResolution.RemoteRef)
            Unit
        }

    @Test
    fun whereis_on_peer_returns_remote_stub(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val nodeA = LocalNode(NodeId("wa-$suffix", "global"))
            val nodeB = LocalNode(NodeId("wb-$suffix", "global"))
            val transport = InMemoryTransport()
            transport.addNode(nodeA)
            transport.addNode(nodeB)
            GlobalRegistry.install(transport, nodeA)
            GlobalRegistry.useNode(nodeB)
            transport.connect(nodeA, nodeB)
            val ref = GenServers.startLink(scope, NoopServer())
            GlobalRegistry.useNode(nodeA)
            GlobalRegistry.registerName("svc", ref)
            delay(20)
            GlobalRegistry.useNode(nodeB)
            val resolved = GlobalRegistry.resolveName("svc")
            assertTrue(resolved is GlobalRegistry.NameResolution.RemoteRef)
            Unit
        }

    @Test
    fun unregister_replicates(): Unit =
        runBlocking {
            val suffix = System.nanoTime().toString()
            val nodeA = LocalNode(NodeId("ua-$suffix", "global"))
            val nodeB = LocalNode(NodeId("ub-$suffix", "global"))
            val transport = InMemoryTransport()
            transport.addNode(nodeA)
            transport.addNode(nodeB)
            GlobalRegistry.install(transport, nodeA)
            GlobalRegistry.useNode(nodeB)
            transport.connect(nodeA, nodeB)
            val ref = GenServers.startLink(scope, NoopServer())
            GlobalRegistry.useNode(nodeA)
            GlobalRegistry.registerName("gone", ref)
            delay(20)
            GlobalRegistry.unregisterName("gone")
            delay(20)
            GlobalRegistry.useNode(nodeB)
            assertNull(GlobalRegistry.resolveName("gone"))
            Unit
        }

    @Test
    fun `registeredNames returns all names`() = runTest {
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("a", r1)
        GlobalRegistry.registerName("b", r2)
        val names = GlobalRegistry.registeredNames()
        assertTrue("a" in names)
        assertTrue("b" in names)
    }

    @Test
    fun `stale register after unregister is ignored by version guard`() = runBlocking {
        val suffix = System.nanoTime().toString()
        val nodeA = LocalNode(NodeId("oa-$suffix", "global"))
        val nodeB = LocalNode(NodeId("ob-$suffix", "global"))
        val transport = InMemoryTransport()
        transport.addNode(nodeA)
        transport.addNode(nodeB)

        GlobalRegistry.install(transport, nodeA)
        GlobalRegistry.useNode(nodeB)
        transport.connect(nodeA, nodeB)

        val pid = 42L
        val wire = "${nodeA.id.name}@${nodeA.id.host}"
        val registerV1 = GlobalDistMsg.Register("race", wire, "race", pid, version = 1)
        val unregisterV2 = GlobalDistMsg.Unregister("race", wire, pid, version = 2)

        GlobalReplicationBus.handler!!.onMessage(registerV1, nodeA.id)
        GlobalReplicationBus.handler!!.onMessage(unregisterV2, nodeA.id)
        GlobalReplicationBus.handler!!.onMessage(registerV1, nodeA.id) // stale replay

        val resolved = GlobalRegistry.resolveName("race")
        assertNull(resolved, "stale register replay must be ignored")
    }

    @Test
    fun `stale sync snapshot is ignored by version guard`() = runBlocking {
        val suffix = System.nanoTime().toString()
        val nodeA = LocalNode(NodeId("sa-$suffix", "global"))
        val nodeB = LocalNode(NodeId("sb-$suffix", "global"))
        val transport = InMemoryTransport()
        transport.addNode(nodeA)
        transport.addNode(nodeB)

        GlobalRegistry.install(transport, nodeA)
        GlobalRegistry.useNode(nodeB)
        transport.connect(nodeA, nodeB)

        val pid = 99L
        val wire = "${nodeA.id.name}@${nodeA.id.host}"
        val registerV1 = GlobalDistMsg.Register("sync-race", wire, "sync-race", pid, version = 1)
        val unregisterV2 = GlobalDistMsg.Unregister("sync-race", wire, pid, version = 2)
        val staleSnapshot = GlobalDistMsg.SyncSnapshot(listOf(registerV1))

        GlobalReplicationBus.handler!!.onMessage(registerV1, nodeA.id)
        GlobalReplicationBus.handler!!.onMessage(unregisterV2, nodeA.id)
        GlobalReplicationBus.handler!!.onMessage(staleSnapshot, nodeA.id)

        val resolved = GlobalRegistry.resolveName("sync-race")
        assertNull(resolved, "stale snapshot must be ignored")
    }

    @Test
    fun `mixed replay churn converges to latest register version`() = runBlocking {
        val suffix = System.nanoTime().toString()
        val nodeA = LocalNode(NodeId("ca-$suffix", "global"))
        val nodeB = LocalNode(NodeId("cb-$suffix", "global"))
        val transport = InMemoryTransport()
        transport.addNode(nodeA)
        transport.addNode(nodeB)
        GlobalRegistry.install(transport, nodeA)
        GlobalRegistry.useNode(nodeB)
        transport.connect(nodeA, nodeB)

        val wire = "${nodeA.id.name}@${nodeA.id.host}"
        val pid = 77L
        val regV1 = GlobalDistMsg.Register("churn", wire, "churn", pid, version = 1)
        val unregV2 = GlobalDistMsg.Unregister("churn", wire, pid, version = 2)
        val regV3 = GlobalDistMsg.Register("churn", wire, "churn", pid, version = 3)
        val staleSync = GlobalDistMsg.SyncSnapshot(entries = listOf(regV1))

        // Deliberately out-of-order/replay-heavy sequence
        val handler = GlobalReplicationBus.handler!!
        handler.onMessage(regV1, nodeA.id)
        handler.onMessage(unregV2, nodeA.id)
        handler.onMessage(staleSync, nodeA.id)
        handler.onMessage(regV3, nodeA.id)
        handler.onMessage(regV1, nodeA.id)
        handler.onMessage(unregV2, nodeA.id)

        val resolved = GlobalRegistry.resolveName("churn")
        assertTrue(resolved is GlobalRegistry.NameResolution.RemoteRef, "latest register version should win")
    }

    @Test
    fun `mixed replay churn converges to latest unregister version`() = runBlocking {
        val suffix = System.nanoTime().toString()
        val nodeA = LocalNode(NodeId("da-$suffix", "global"))
        val nodeB = LocalNode(NodeId("db-$suffix", "global"))
        val transport = InMemoryTransport()
        transport.addNode(nodeA)
        transport.addNode(nodeB)
        GlobalRegistry.install(transport, nodeA)
        GlobalRegistry.useNode(nodeB)
        transport.connect(nodeA, nodeB)

        val wire = "${nodeA.id.name}@${nodeA.id.host}"
        val pid = 88L
        val regV1 = GlobalDistMsg.Register("churn-gone", wire, "churn-gone", pid, version = 1)
        val regV3 = GlobalDistMsg.Register("churn-gone", wire, "churn-gone", pid, version = 3)
        val unregV4 = GlobalDistMsg.Unregister("churn-gone", wire, pid, version = 4)
        val staleSync = GlobalDistMsg.SyncSnapshot(entries = listOf(regV3))

        val handler = GlobalReplicationBus.handler!!
        handler.onMessage(regV1, nodeA.id)
        handler.onMessage(regV3, nodeA.id)
        handler.onMessage(unregV4, nodeA.id)
        handler.onMessage(staleSync, nodeA.id)
        handler.onMessage(regV1, nodeA.id)

        val resolved = GlobalRegistry.resolveName("churn-gone")
        assertNull(resolved, "latest unregister version should win")
    }
}
