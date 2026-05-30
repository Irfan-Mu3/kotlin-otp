package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.otpstudy.genserver.GenServerRef
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

    @Suppress("UNCHECKED_CAST")
    private suspend fun leaderState(ref: GenServerRef<GenLeaderServer.LeaderState<Int>>): GenLeaderServer.LeaderState<Int> =
        ref.getState() as GenLeaderServer.LeaderState<Int>
}
