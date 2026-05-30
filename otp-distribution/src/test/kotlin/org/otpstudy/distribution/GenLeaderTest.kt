package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServerRef
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.withTimeout

private data class LeaderProbeState(
    val electedCount: Int = 0,
    val surrenderedCount: Int = 0,
    val lastLeader: NodeId? = null,
)

private class LeaderProbeCallbacks : LeaderCallbacks<LeaderProbeState> {
    override suspend fun init(): LeaderProbeState = LeaderProbeState()

    override suspend fun elected(state: LeaderProbeState, leader: NodeId): LeaderProbeState =
        state.copy(electedCount = state.electedCount + 1, lastLeader = leader)

    override suspend fun surrendered(state: LeaderProbeState, leader: NodeId): LeaderProbeState =
        state.copy(surrenderedCount = state.surrenderedCount + 1, lastLeader = leader)

    override suspend fun handleLeaderCall(request: Any, state: LeaderProbeState): Pair<Any, LeaderProbeState> =
        Pair("leader:$request", state)

    override suspend fun handleCall(request: Any, state: LeaderProbeState): Pair<Any, LeaderProbeState> =
        Pair("follower:$request", state)

    override suspend fun handleCast(request: Any, state: LeaderProbeState): LeaderProbeState = state
}

class GenLeaderTest {
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
    fun `startup election chooses lexicographically greatest node`() = runBlocking {
        val local = NodeId("node-b", "h")
        val peers = listOf(NodeId("node-a", "h"), NodeId("node-z", "h"))
        val ref = GenLeaders.startLink(scope, LeaderProbeCallbacks(), localNode = local, peers = peers, name = "leader-a")

        delay(100)
        val state = leaderState(ref)
        assertEquals(NodeId("node-z", "h"), state.leader)
        assertFalse(state.isLeader)
        assertEquals(1, state.appState.surrenderedCount)
        assertEquals(0, state.appState.electedCount)
        ref.stop()
    }

    @Test
    fun `leader down triggers re-election excluding old leader`() = runBlocking {
        val local = NodeId("node-b", "h")
        val oldLeader = NodeId("node-z", "h")
        val peers = listOf(NodeId("node-a", "h"), oldLeader)
        val ref = GenLeaders.startLink(scope, LeaderProbeCallbacks(), localNode = local, peers = peers, name = "leader-b")

        delay(100)
        NodeMonitor.notifyDown(oldLeader, "test-down")
        delay(100)

        val state = leaderState(ref)
        assertNotNull(state.leader)
        assertEquals(local, state.leader)
        assertEquals(true, state.isLeader)
        assertEquals(1, state.appState.electedCount)
        ref.stop()
    }

    @Test
    fun `duplicate elect casts keep winner deterministic`() = runBlocking {
        val local = NodeId("node-c", "h")
        val peers = listOf(NodeId("node-a", "h"), NodeId("node-b", "h"), NodeId("node-b", "h"))
        val ref = GenLeaders.startLink(scope, LeaderProbeCallbacks(), localNode = local, peers = peers, name = "leader-c")

        repeat(5) { ref.cast(LeaderMsg.Elect()) }
        delay(100)

        val state = leaderState(ref)
        assertEquals(local, state.leader)
        assertEquals(true, state.isLeader)
        ref.stop()
    }

    @Test
    fun `split views can choose different leaders while each remains locally valid`() = runBlocking {
        val local = NodeId("node-b", "h")
        val nodeA = NodeId("node-a", "h")
        val nodeC = NodeId("node-c", "h")

        // View 1 sees nodeC; view 2 does not.
        val refWithC = GenLeaders.startLink(
            scope,
            LeaderProbeCallbacks(),
            localNode = local,
            peers = listOf(nodeA, nodeC),
            name = "leader-view-with-c",
        )
        val refWithoutC = GenLeaders.startLink(
            scope,
            LeaderProbeCallbacks(),
            localNode = local,
            peers = listOf(nodeA),
            name = "leader-view-without-c",
        )

        delay(100)
        val s1 = leaderState(refWithC)
        val s2 = leaderState(refWithoutC)

        assertEquals(nodeC, s1.leader)
        assertEquals(local, s2.leader)
        assertTrue(s1.leader in listOf(local, nodeA, nodeC))
        assertTrue(s2.leader in listOf(local, nodeA))

        refWithC.stop()
        refWithoutC.stop()
    }

    @Test
    fun `divergent failure observations can temporarily diverge local leaders`() = runBlocking {
        val local = NodeId("node-b", "h")
        val nodeA = NodeId("node-a", "h")
        val nodeC = NodeId("node-c", "h")

        val refA = GenLeaders.startLink(
            scope,
            LeaderProbeCallbacks(),
            localNode = local,
            peers = listOf(nodeA, nodeC),
            name = "leader-div-a",
        )
        val refB = GenLeaders.startLink(
            scope,
            LeaderProbeCallbacks(),
            localNode = local,
            peers = listOf(nodeA, nodeC),
            name = "leader-div-b",
        )
        delay(100)
        assertEquals(nodeC, leaderState(refA).leader)
        assertEquals(nodeC, leaderState(refB).leader)

        // Only A observes nodeC as down in this local model.
        refA.cast(LeaderMsg.Elect(exclude = nodeC))
        delay(100)

        val a = leaderState(refA)
        val b = leaderState(refB)
        assertEquals(local, a.leader)
        assertEquals(nodeC, b.leader)

        refA.stop()
        refB.stop()
    }

    @Test
    fun `noisy non-leader node events do not block call progress`() = runBlocking {
        val local = NodeId("node-b", "h")
        val nodeA = NodeId("node-a", "h")
        val nodeC = NodeId("node-c", "h")
        val ref = GenLeaders.startLink(
            scope,
            LeaderProbeCallbacks(),
            localNode = local,
            peers = listOf(nodeA, nodeC),
            name = "leader-noisy-events",
        )
        delay(100)

        repeat(5_000) {
            // non-leader noisy events should be ignored by handler fast-path
            ref.sendInfo(NodeEvent.NodeDown(nodeA, "noise-$it"))
        }

        withTimeout(2_000L) {
            @Suppress("UNCHECKED_CAST")
            val response = ref.call<String>("probe")
            assertTrue(response.startsWith("follower:") || response.startsWith("leader:"))
        }

        val state = leaderState(ref)
        assertEquals(nodeC, state.leader, "leader should remain stable under irrelevant node noise")
        ref.stop()
    }

    @Test
    fun `repeated leader down storms still converge and stay responsive`() = runBlocking {
        val local = NodeId("node-b", "h")
        val leader = NodeId("node-z", "h")
        val ref = GenLeaders.startLink(
            scope,
            LeaderProbeCallbacks(),
            localNode = local,
            peers = listOf(NodeId("node-a", "h"), leader),
            name = "leader-down-storm",
        )
        delay(100)

        repeat(200) { ref.sendInfo(NodeEvent.NodeDown(leader, "storm-$it")) }
        withTimeout(2_000L) {
            while (true) {
                val state = leaderState(ref)
                if (state.leader == local) break
                delay(5)
            }
        }
        withTimeout(2_000L) {
            @Suppress("UNCHECKED_CAST")
            ref.call<String>("probe")
        }
        ref.stop()
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun leaderState(
        ref: GenServerRef<GenLeaderServer.LeaderState<LeaderProbeState>>
    ): GenLeaderServer.LeaderState<LeaderProbeState> =
        ref.getState() as GenLeaderServer.LeaderState<LeaderProbeState>
}
