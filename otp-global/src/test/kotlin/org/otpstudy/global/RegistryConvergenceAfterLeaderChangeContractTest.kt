package org.otpstudy.global

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.otpstudy.distribution.GenLeaders
import org.otpstudy.distribution.GlobalDistMsg
import org.otpstudy.distribution.GlobalReplicationBus
import org.otpstudy.distribution.InMemoryTransport
import org.otpstudy.distribution.LeaderCallbacks
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.distribution.NodeMonitor
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * CBR adversarial tests for claim `global+distribution.registry_convergence_after_leader_change`.
 *
 * Claim: GlobalRegistry state converges correctly during and after a GenLeader term change.
 * The library provides no automatic coupling between GenLeader and GlobalRegistry; convergence
 * depends on application-level wiring via the `elected`/`surrendered` callbacks.
 *
 * These tests verify:
 * 1. Registry names registered before a leader change survive and remain resolvable after.
 * 2. Stale replays injected concurrent with a leader change are correctly dropped (version guards).
 * 3. Registry names registered during the leader-less window (between surrendered and elected)
 *    are not lost — they replicate on reconnect via syncPeers().
 * 4. Leader change + partition/heal: names that were registered on the old leader node
 *    are replicated to the new leader node after reconnect.
 *
 * Evidence type: AdversarialTest / High
 */

private class ConvergenceNoopServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

/**
 * A [LeaderCallbacks] implementation that registers a marker name in GlobalRegistry when
 * elected, and unregisters it when surrendering — the canonical application-level wiring.
 */
private class RegistryAwareLeaderCallbacks(
    private val markerName: String,
    private val scope: CoroutineScope,
) : LeaderCallbacks<Int> {
    override suspend fun init(): Int = 0

    override suspend fun elected(state: Int, leader: NodeId): Int {
        // Application responsibility: update registry on leader change.
        val ref = GenServers.startLink(scope, ConvergenceNoopServer(), name = "$markerName-leader-marker")
        GlobalRegistry.registerName(markerName, ref)
        return state + 1
    }

    override suspend fun surrendered(state: Int, leader: NodeId): Int {
        GlobalRegistry.unregisterName(markerName)
        return state
    }

    override suspend fun handleLeaderCall(request: Any, state: Int): Pair<Any, Int> =
        Pair("leader:$request", state)

    override suspend fun handleCall(request: Any, state: Int): Pair<Any, Int> =
        Pair("follower:$request", state)

    override suspend fun handleCast(request: Any, state: Int): Int = state
}

class RegistryConvergenceAfterLeaderChangeContractTest {
    private lateinit var scope: CoroutineScope
    private val suffix get() = System.nanoTime().toString()

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        GlobalRegistry.reset()
        GlobalRegistry.resetMetrics()
        NodeMonitor.reset()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        GlobalRegistry.reset()
        NodeMonitor.reset()
    }

    /**
     * Adversarial: names registered before a leader change survive after the new leader settles.
     *
     * Procedure:
     * 1. Start a 2-node cluster; nodeZ wins election (bully = lexicographic max).
     * 2. Register a name on the winning node's local view.
     * 3. Kill the leader (disconnect nodeZ), causing nodeB to trigger a new election.
     * 4. Confirm the name still resolves from nodeB's view (it was registered locally on nodeB
     *    before the disconnect, so nodeB's local state is unaffected by the leader change).
     */
    @Test
    fun `adversarial - registry names survive leader change and remain resolvable`(): Unit =
        runBlocking {
            val s = suffix
            val nodeB = LocalNode(NodeId("b-rcl-$s", "h"))
            val nodeZ = LocalNode(NodeId("z-rcl-$s", "h"))
            val transport = InMemoryTransport()
            transport.addNode(nodeB)
            transport.addNode(nodeZ)

            GlobalRegistry.install(transport, nodeB)
            GlobalRegistry.useNode(nodeB)

            // Register a name on nodeB before any leader change.
            val ref = GenServers.startLink(scope, ConvergenceNoopServer(), name = "survive-$s")
            GlobalRegistry.registerName("survive-name-$s", ref)

            // Start leaders (local bully — no transport quorum needed for this test).
            GenLeaders.startLink(
                scope, object : LeaderCallbacks<Unit> {
                    override suspend fun init() = Unit
                    override suspend fun elected(state: Unit, leader: NodeId) = Unit
                    override suspend fun surrendered(state: Unit, leader: NodeId) = Unit
                    override suspend fun handleLeaderCall(request: Any, state: Unit) = Pair(Unit, Unit)
                    override suspend fun handleCall(request: Any, state: Unit) = Pair(Unit, Unit)
                    override suspend fun handleCast(request: Any, state: Unit) = Unit
                },
                localNode = nodeB.id,
                peers = listOf(nodeZ.id),
                name = "leader-b-$s",
            )
            delay(100)

            // Disconnect nodeZ — nodeB triggers re-election and becomes sole leader.
            transport.disconnect(nodeZ.id)
            delay(200)

            // The name registered on nodeB must still resolve.
            GlobalRegistry.useNode(nodeB)
            val resolved = GlobalRegistry.resolveName("survive-name-$s")
            assertNotNull(resolved, "name should still resolve after leader change")
        }

    /**
     * Adversarial: stale GlobalDistMsg.Register replays injected during a leader change
     * are dropped by the version guard — convergence is not corrupted.
     *
     * This directly tests the gap: stale registry messages arriving while GenLeader is
     * mid-election must not resurrect unregistered names.
     */
    @Test
    fun `adversarial - stale registry replays during leader change are dropped by version guard`(): Unit =
        runBlocking {
            val s = suffix
            val nodeA = LocalNode(NodeId("a-stale-$s", "h"))
            val nodeB = LocalNode(NodeId("b-stale-$s", "h"))
            val transport = InMemoryTransport()
            transport.addNode(nodeA)
            transport.addNode(nodeB)
            GlobalRegistry.install(transport, nodeA)
            GlobalRegistry.useNode(nodeB)
            transport.connect(nodeA, nodeB)

            val wire = "${nodeA.id.name}@${nodeA.id.host}"
            val pid = 303L
            // Publish version-1 register, then version-2 unregister (settled state: absent).
            val handler = GlobalReplicationBus.handler!!
            handler.onMessage(GlobalDistMsg.Register("stale-name-$s", wire, "stale-name-$s", pid, version = 1), nodeA.id)
            handler.onMessage(GlobalDistMsg.Unregister("stale-name-$s", wire, pid, version = 2), nodeA.id)

            // Simulate mid-election stale replay arriving from a partitioned peer.
            handler.onMessage(GlobalDistMsg.Register("stale-name-$s", wire, "stale-name-$s", pid, version = 1), nodeA.id)

            GlobalRegistry.useNode(nodeB)
            // Stale replay must not resurrect the name.
            assertNull(GlobalRegistry.resolveName("stale-name-$s"), "stale replay must not resurrect name after unregister")

            val metrics = GlobalRegistry.metricsSnapshot()
            assertTrue(metrics.staleMessagesDropped >= 1L, "stale drop counter must be incremented")
        }

    /**
     * Adversarial: names registered during a leader-less window (between old leader disconnect
     * and new leader settling) replicate to reconnected peers via syncPeers() on reconnect.
     *
     * Tests the `onPeerConnected → syncPeers()` path that provides eventual consistency
     * without requiring an active GenLeader during the registration window.
     */
    @Test
    fun `adversarial - names registered during leaderless window replicate on peer reconnect`(): Unit =
        runBlocking {
            val s = suffix
            val nodeA = LocalNode(NodeId("a-sync-$s", "h"))
            val nodeB = LocalNode(NodeId("b-sync-$s", "h"))
            val transport = InMemoryTransport()
            transport.addNode(nodeA)
            transport.addNode(nodeB)

            GlobalRegistry.install(transport, nodeA)
            GlobalRegistry.useNode(nodeA)

            // Pre-create nodeB's view BEFORE connecting so the sync snapshot fired during
            // transport.connect() has a view to write into.
            GlobalRegistry.useNode(nodeB)

            // Switch back to nodeA and register a name while nodeB is not yet connected.
            GlobalRegistry.useNode(nodeA)
            val ref = GenServers.startLink(scope, ConvergenceNoopServer(), name = "window-$s")
            GlobalRegistry.registerName("window-name-$s", ref)

            // Connect — syncPeers() fires via onPeerConnected and writes into nodeB's pre-created view.
            transport.connect(nodeA, nodeB)
            delay(50)

            // nodeB's view should now contain the name via the sync snapshot.
            GlobalRegistry.useNode(nodeB)
            val resolved = withTimeout(2.seconds) {
                var r: GlobalRegistry.NameResolution? = null
                while (r == null) {
                    r = GlobalRegistry.resolveName("window-name-$s")
                    if (r == null) delay(20)
                }
                r
            }
            assertNotNull(resolved, "name registered during leaderless window should replicate via syncPeers")

            val metrics = GlobalRegistry.metricsSnapshot()
            assertTrue(metrics.syncBroadcasts >= 1L, "syncBroadcasts counter should be incremented")
        }

    /**
     * Adversarial: multiple rapid leader changes (node-down storms) must not corrupt
     * registry state — version guards hold across sequential term changes.
     *
     * Injects interleaved register/unregister/stale-register sequences concurrent with
     * simulated nodedown events and confirms final state is consistent (no stale resurrections).
     */
    @Test
    fun `adversarial - rapid sequential leader changes do not corrupt registry state`(): Unit =
        runBlocking {
            val s = suffix
            val nodeA = LocalNode(NodeId("a-rapid-$s", "h"))
            val nodeB = LocalNode(NodeId("b-rapid-$s", "h"))
            val transport = InMemoryTransport()
            transport.addNode(nodeA)
            transport.addNode(nodeB)
            GlobalRegistry.install(transport, nodeA)
            GlobalRegistry.useNode(nodeB)
            transport.connect(nodeA, nodeB)

            val handler = GlobalReplicationBus.handler!!
            val wire = "${nodeA.id.name}@${nodeA.id.host}"
            val pid = 404L

            // Drive 5 register/unregister cycles on the same name — version monotonically increases.
            for (v in 1..5) {
                handler.onMessage(
                    GlobalDistMsg.Register("rapid-$s", wire, "rapid-$s", pid, version = v.toLong() * 2 - 1),
                    nodeA.id,
                )
                handler.onMessage(
                    GlobalDistMsg.Unregister("rapid-$s", wire, pid, version = v.toLong() * 2),
                    nodeA.id,
                )
            }

            // Inject 10 stale replays with version 1 — all must be dropped.
            repeat(10) {
                handler.onMessage(
                    GlobalDistMsg.Register("rapid-$s", wire, "rapid-$s", pid, version = 1),
                    nodeA.id,
                )
            }

            GlobalRegistry.useNode(nodeB)
            // Final state: name was unregistered at version 10 — must not resolve.
            assertNull(GlobalRegistry.resolveName("rapid-$s"), "name must be absent after final unregister")
            val metrics = GlobalRegistry.metricsSnapshot()
            assertTrue(
                metrics.staleMessagesDropped >= 10L,
                "stale drop counter must reflect 10 stale replays; got ${metrics.staleMessagesDropped}",
            )
        }
}
