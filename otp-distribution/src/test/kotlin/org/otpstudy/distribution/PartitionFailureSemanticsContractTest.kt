package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * CBR adversarial tests for claim `distribution.partition_failure_semantics`.
 *
 * Claim (Fidelity / Parity): under node partition/rejoin in the in-memory transport,
 * call/cast semantics remain consistent with the library's OTP-shaped contract:
 * - call to unavailable node fails promptly (transport error)
 * - cast to unavailable node is best-effort and swallowed
 * - multi_call separates replies from no_reply nodes under partition
 * - after rejoin, new messages are delivered; there is no replay of messages lost
 *   during the partition window
 *
 * Evidence type: AdversarialTest / High
 */
private class PartitionEchoServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)

    override suspend fun handleCall(request: Any, state: Int): ReplyResult<Int> = when (request) {
        "count" -> ReplyResult.Reply(state, state)
        else -> ReplyResult.Reply("echo:$request", state)
    }

    override suspend fun handleCast(request: Any, state: Int): NoreplyResult<Int> = when (request) {
        "bump" -> NoreplyResult.Noreply(state + 1)
        else -> NoreplyResult.Noreply(state)
    }
}

class PartitionFailureSemanticsContractTest {
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
    fun `adversarial - multiCall classifies reply and noReply across partition`() = runBlocking {
        val transport = InMemoryTransport()
        val nodeA = LocalNode(NodeId("a", "partition-multi"))
        val nodeB = LocalNode(NodeId("b", "partition-multi"))
        transport.addNode(nodeA)
        transport.addNode(nodeB)

        nodeA.register("svc", GenServers.startLink(scope, PartitionEchoServer()))
        nodeB.register("svc", GenServers.startLink(scope, PartitionEchoServer()))

        // Partition nodeB by removing it from transport lookup.
        transport.disconnect(nodeB.id)

        val result = DistributedGenServers.multiCall<String>(
            nodes = listOf(nodeA.id, nodeB.id),
            name = "svc",
            request = "ping",
            transport = transport,
            timeout = 2.seconds,
        )

        assertEquals(1, result.replies.size, "only reachable node should reply")
        assertEquals(nodeA.id, result.replies.single().first)
        assertEquals("echo:ping", result.replies.single().second)
        assertEquals(listOf(nodeB.id), result.noReplyNodes, "partitioned node must be classified as noReply")
    }

    @Test
    fun `adversarial - call fails during partition and succeeds after rejoin`() = runBlocking {
        val transport = InMemoryTransport()
        val nodeB = LocalNode(NodeId("b", "partition-rejoin"))
        transport.addNode(nodeB)
        nodeB.register("svc", GenServers.startLink(scope, PartitionEchoServer()))
        val stub = RemoteNodeStub(nodeB.id, transport)

        // Baseline: reachable.
        val baseline: String = stub.call("svc", "before", 2.seconds)
        assertEquals("echo:before", baseline)

        // Partition: node removed from transport -> unknown node transport error.
        transport.disconnect(nodeB.id)
        val failed = runCatching { stub.call<String>("svc", "during", 2.seconds) }
        assertTrue(failed.isFailure, "call during partition must fail")
        assertTrue(
            failed.exceptionOrNull() is IllegalStateException,
            "partition failure must be transport IllegalStateException",
        )

        // Rejoin: add node back -> new calls succeed.
        transport.addNode(nodeB)
        val after: String = stub.call("svc", "after", 2.seconds)
        assertEquals("echo:after", after)
    }

    @Test
    fun `adversarial - cast during partition is swallowed and not replayed after rejoin`() = runBlocking {
        val transport = InMemoryTransport()
        val nodeB = LocalNode(NodeId("b", "partition-cast"))
        transport.addNode(nodeB)
        val ref = GenServers.startLink(scope, PartitionEchoServer())
        nodeB.register("svc", ref)
        val stub = RemoteNodeStub(nodeB.id, transport)

        // Delivered cast before partition.
        stub.cast("svc", "bump")
        val before: Int = ref.call("count")
        assertEquals(1, before)

        // Partition and cast while unavailable; this must be swallowed (no throw, no replay).
        transport.disconnect(nodeB.id)
        repeat(3) { stub.cast("svc", "bump") }

        // Rejoin and verify no replay of partition-window casts.
        transport.addNode(nodeB)
        val afterRejoin: Int = ref.call("count")
        assertEquals(1, afterRejoin, "casts sent during partition must not replay on rejoin")

        // Fresh cast post-rejoin should be delivered.
        stub.cast("svc", "bump")
        val final: Int = ref.call("count")
        assertEquals(2, final, "post-rejoin cast should be delivered normally")
    }

    @Test
    fun `adversarial - abcast best-effort drops partitioned nodes and resumes after rejoin`() = runBlocking {
        val transport = InMemoryTransport()
        val nodeA = LocalNode(NodeId("a", "partition-abcast"))
        val nodeB = LocalNode(NodeId("b", "partition-abcast"))
        transport.addNode(nodeA)
        transport.addNode(nodeB)
        val refA = GenServers.startLink(scope, PartitionEchoServer())
        val refB = GenServers.startLink(scope, PartitionEchoServer())
        nodeA.register("svc", refA)
        nodeB.register("svc", refB)

        // Partition nodeB; abcast should still deliver to nodeA and swallow nodeB failure.
        transport.disconnect(nodeB.id)
        DistributedGenServers.abcast(
            nodes = listOf(nodeA.id, nodeB.id),
            name = "svc",
            message = "bump",
            transport = transport,
        )
        assertEquals(1, refA.call<Int>("count"))
        assertEquals(0, refB.call<Int>("count"), "partitioned node must not receive abcast message")

        // Rejoin and abcast again; both nodes should now receive.
        transport.addNode(nodeB)
        DistributedGenServers.abcast(
            nodes = listOf(nodeA.id, nodeB.id),
            name = "svc",
            message = "bump",
            transport = transport,
        )
        assertEquals(2, refA.call<Int>("count"))
        assertEquals(1, refB.call<Int>("count"))
    }
}
