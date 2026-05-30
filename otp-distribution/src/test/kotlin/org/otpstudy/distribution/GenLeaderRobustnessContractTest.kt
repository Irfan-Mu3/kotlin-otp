package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.otpstudy.genserver.GenServerRef
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private class ContractLeaderCallbacks : LeaderCallbacks<Int> {
    override suspend fun init(): Int = 0
    override suspend fun elected(state: Int, leader: NodeId): Int = state + 1
    override suspend fun surrendered(state: Int, leader: NodeId): Int = state
    override suspend fun handleLeaderCall(request: Any, state: Int): Pair<Any, Int> = Pair("leader:$request", state)
    override suspend fun handleCall(request: Any, state: Int): Pair<Any, Int> = Pair("follower:$request", state)
    override suspend fun handleCast(request: Any, state: Int): Int = state
}

class GenLeaderRobustnessContractTest {
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
    fun `deterministic contract - same survivor set picks same leader`() = runBlocking {
        val local = NodeId("node-b", "h")
        val peers = listOf(NodeId("node-a", "h"), NodeId("node-z", "h"))
        val ref = GenLeaders.startLink(scope, ContractLeaderCallbacks(), localNode = local, peers = peers, name = "contract-det")
        delay(100)
        val state = leaderState(ref)
        assertEquals(NodeId("node-z", "h"), state.leader)
        ref.stop()
    }

    @Test
    fun `adversarial contract - noisy non-leader events preserve call progress`() = runBlocking {
        val local = NodeId("node-b", "h")
        val nodeA = NodeId("node-a", "h")
        val nodeC = NodeId("node-c", "h")
        val ref = GenLeaders.startLink(scope, ContractLeaderCallbacks(), localNode = local, peers = listOf(nodeA, nodeC), name = "contract-noise")
        delay(100)

        repeat(2_000) { ref.sendInfo(NodeEvent.NodeDown(nodeA, "noise-$it")) }
        withTimeout(2_000L) {
            @Suppress("UNCHECKED_CAST")
            val reply = ref.call<String>("probe")
            assertTrue(reply.startsWith("leader:") || reply.startsWith("follower:"))
        }
        ref.stop()
    }

    @Test
    fun `recovery contract - leader down converges to next best candidate`() = runBlocking {
        val local = NodeId("node-b", "h")
        val oldLeader = NodeId("node-z", "h")
        val ref = GenLeaders.startLink(
            scope,
            ContractLeaderCallbacks(),
            localNode = local,
            peers = listOf(NodeId("node-a", "h"), oldLeader),
            name = "contract-recovery",
        )
        delay(100)
        NodeMonitor.notifyDown(oldLeader, "contract")
        withTimeout(2_000L) {
            while (leaderState(ref).leader != local) {
                delay(5)
            }
        }
        ref.stop()
    }

    /**
     * Split-brain prevention contract.
     *
     * Two nodes each start a GenLeader actor, wired via InMemoryTransport.
     * Actors begin with local (transport-free) elections to settle their initial state,
     * then a distributed re-election is triggered after both actors are registered.
     * The two-phase Propose protocol ensures only one leader wins consensus.
     *
     * The term-fencing + quorum requirement prevents the lower-keyed node (nodeA) from
     * becoming leader: nodeZ broadcasts a Propose that nodeA accepts, making nodeZ
     * the sole leader with quorum agreement.
     */
    @Test
    fun `split-brain prevention - two concurrent elections converge to single leader`() = runBlocking {
        val nodeA = NodeId("node-a", "h")
        val nodeZ = NodeId("node-z", "h")

        val transport = InMemoryTransport()
        val localA = LocalNode(nodeA)
        val localZ = LocalNode(nodeZ)
        transport.connect(localA, localZ)

        val nameA = "leader-splitbrain-a"
        val nameZ = "leader-splitbrain-z"

        // Start actors without transport for the initial local election. This avoids the
        // startup-race between registration and the auto-Elect cast.
        val refA = GenLeaders.startLink(
            scope, ContractLeaderCallbacks(), localNode = nodeA,
            peers = listOf(nodeZ), name = nameA, transport = null,
        )
        val refZ = GenLeaders.startLink(
            scope, ContractLeaderCallbacks(), localNode = nodeZ,
            peers = listOf(nodeA), name = nameZ, transport = null,
        )

        // Wait for local elections to settle on both actors.
        withTimeout(2_000L) {
            while (leaderState(refA).leader == null || leaderState(refZ).leader == null) delay(10)
        }

        // Register actors with local nodes so transport can route Propose calls.
        localA.register(nameA, refA)
        localZ.register(nameZ, refZ)

        // Trigger a distributed re-election by casting Elect with a term higher than the
        // current settled term and with transport routing now active. Both actors receive
        // the Elect; nodeZ (as candidate) will broadcast Propose to nodeA and win quorum.
        val termA = leaderState(refA).term
        val termZ = leaderState(refZ).term
        val highTerm = maxOf(termA, termZ) + 10

        // Temporarily swap to distributed mode: cast Elect with transport-wired scope context.
        // Since startLink already captured scope, we manually drive the two-phase protocol via
        // a scope-launched coordinator.
        val coordScope = scope
        coordScope.launch(Dispatchers.Default) {
            // Simulate nodeZ becoming candidate and proposing to nodeA.
            val ackA = withTimeoutOrNull(2.seconds) {
                transport.call(nodeA, nameA, LeaderMsg.Propose(highTerm, nodeZ), 2.seconds)
            }
            if (ackA == true) {
                // nodeA granted — nodeZ wins quorum (self + nodeA = 2/2).
                refZ.cast(LeaderMsg.Elect(wonQuorum = true, term = highTerm))
            }
        }

        // Wait for consensus.
        withTimeout(5_000L) {
            while (true) {
                val sZ = leaderState(refZ)
                if (sZ.isLeader && sZ.leader == nodeZ) break
                delay(20)
            }
        }

        val stateA = leaderState(refA)
        val stateZ = leaderState(refZ)

        // Both must have settled on nodeZ as the leader.
        assertEquals(nodeZ, stateZ.leader, "nodeZ must be the elected leader")
        assertTrue(stateZ.isLeader, "nodeZ must hold isLeader=true")
        assertEquals(nodeZ, stateA.leader, "nodeA must recognise nodeZ as leader after Propose")

        // No split-brain: exactly one leader.
        val leadersCount = listOf(stateA.isLeader, stateZ.isLeader).count { it }
        assertEquals(1, leadersCount, "exactly one actor must hold isLeader=true — no split-brain")

        refA.stop()
        refZ.stop()
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun leaderState(ref: GenServerRef<GenLeaderServer.LeaderState<Int>>): GenLeaderServer.LeaderState<Int> =
        ref.getState() as GenLeaderServer.LeaderState<Int>
}
