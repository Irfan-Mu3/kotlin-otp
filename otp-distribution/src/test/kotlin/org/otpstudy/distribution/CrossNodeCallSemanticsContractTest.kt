package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
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
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * CBR adversarial tests for claim `genserver+distribution.call_semantics_across_node_boundary`.
 *
 * Claim: cross-boundary call failure modes (timeout, transport error, unknown node) are
 * structurally distinct — no conflation between [TimeoutCancellationException],
 * [IllegalStateException] transport errors, and local [ServerDownException].
 *
 * These tests verify:
 * 1. A successful cross-node call returns the correct reply.
 * 2. A call to an unknown node throws a transport error (not timeout, not ServerDownException).
 * 3. A call that exceeds the timeout throws TimeoutCancellationException from the transport,
 *    not a transport error.
 * 4. N concurrent cross-node calls to two nodes complete correctly with no cross-contamination.
 * 5. Cast to an unknown node is silently swallowed (OTP cast semantics for remote unreliable).
 *
 * Evidence type: AdversarialTest / High
 */

private fun slowEchoServer(delayMs: Long = 0) = object : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> {
        if (delayMs > 0) delay(delayMs)
        return ReplyResult.Reply("echo:$request", state)
    }
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class CrossNodeCallSemanticsContractTest {
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

    /**
     * Deterministic: a successful call across a node boundary returns the correct reply.
     * Baseline test confirming the happy-path works before adversarial cases.
     */
    @Test
    fun `deterministic - successful cross-node call returns correct reply`(): Unit = runBlocking {
        val transport = InMemoryTransport()
        val nodeA = LocalNode(NodeId("a", "cross-happy"))
        val nodeB = LocalNode(NodeId("b", "cross-happy"))
        transport.addNode(nodeA)
        transport.addNode(nodeB)
        nodeB.register("echo", GenServers.startLink(this, slowEchoServer()))

        val stub = RemoteNodeStub(nodeB.id, transport)
        val reply: String = stub.call("echo", "hello", 5.seconds)
        assertEquals("echo:hello", reply)
    }

    /**
     * Adversarial: a call to an unknown node throws a transport-level error
     * (IllegalStateException "unknown node"), NOT a TimeoutCancellationException or
     * ServerDownException. Confirms failure mode is not conflated.
     */
    @Test
    fun `adversarial - call to unknown node throws transport error, not timeout`(): Unit = runBlocking {
        val transport = InMemoryTransport()
        val ghost = NodeId("ghost", "cross-err")
        val stub = RemoteNodeStub(ghost, transport)

        val result = runCatching { stub.call<String>("svc", "ping", 5.seconds) }
        assertTrue(result.isFailure, "call to unknown node must fail")

        val ex = result.exceptionOrNull()
        assertNotNull(ex, "exception must not be null")
        // Must be a transport error (IllegalStateException "unknown node") — not a timeout.
        val isTransportError = ex is IllegalStateException
        assertTrue(
            isTransportError,
            "expected IllegalStateException (transport error), got ${ex.javaClass.simpleName}: ${ex.message}",
        )
        assertTrue(
            ex.message?.contains("unknown node") == true,
            "error message should mention 'unknown node', got: ${ex.message}",
        )
    }

    /**
     * Adversarial: a call that exceeds the timeout is surfaced as a timeout-derived exception,
     * not as a transport error or ServerDownException.
     *
     * The slow server takes 800ms; we call with a 200ms timeout. The transport wraps
     * LocalNode.call → GenServerRef.call which uses withTimeout internally.
     */
    @Test
    fun `adversarial - call that exceeds timeout is distinct from transport error`(): Unit = runBlocking {
        val transport = InMemoryTransport()
        val nodeA = LocalNode(NodeId("a", "cross-timeout"))
        transport.addNode(nodeA)
        nodeA.register("slow", GenServers.startLink(this, slowEchoServer(delayMs = 800)))

        val stub = RemoteNodeStub(nodeA.id, transport)
        val result = runCatching { stub.call<String>("slow", "ping", 200.milliseconds) }

        assertTrue(result.isFailure, "call exceeding timeout must fail")
        val ex = result.exceptionOrNull()
        assertNotNull(ex)
        // Must NOT be an IllegalStateException (transport error).
        val isNotTransportError = ex !is IllegalStateException
        assertTrue(isNotTransportError, "timeout failure must not be wrapped as transport error; got ${ex.javaClass.simpleName}")
        // Must be a cancellation/timeout family exception.
        val isTimeoutOrCancellation = ex is kotlinx.coroutines.TimeoutCancellationException
            || ex is kotlinx.coroutines.CancellationException
            || ex.cause is kotlinx.coroutines.TimeoutCancellationException
        assertTrue(isTimeoutOrCancellation, "expected timeout/cancellation exception; got ${ex.javaClass.name}: ${ex.message}")
    }

    /**
     * Adversarial: N concurrent cross-node calls to two different nodes complete
     * correctly with the right node-specific reply — no reply cross-contamination.
     */
    @Test
    fun `adversarial - concurrent cross-node calls to two nodes return correct node-specific replies`(): Unit =
        runBlocking {
            val transport = InMemoryTransport()
            val nodeA = LocalNode(NodeId("a", "cross-concurrent"))
            val nodeB = LocalNode(NodeId("b", "cross-concurrent"))
            transport.addNode(nodeA)
            transport.addNode(nodeB)
            nodeA.register("svc", GenServers.startLink(this, slowEchoServer()))
            nodeB.register("svc", GenServers.startLink(this, slowEchoServer()))

            val stubA = RemoteNodeStub(nodeA.id, transport)
            val stubB = RemoteNodeStub(nodeB.id, transport)

            val concurrency = 50
            val callsA = (1..concurrency).map { i ->
                async { stubA.call<String>("svc", "msg-a-$i", 5.seconds) }
            }
            val callsB = (1..concurrency).map { i ->
                async { stubB.call<String>("svc", "msg-b-$i", 5.seconds) }
            }

            val repliesA = withTimeout(10.seconds) { callsA.map { it.await() } }
            val repliesB = withTimeout(10.seconds) { callsB.map { it.await() } }

            // Every reply from nodeA must echo its own request (no cross-contamination with B's messages).
            repliesA.forEachIndexed { i, reply ->
                assertEquals("echo:msg-a-${i + 1}", reply, "nodeA reply $i was wrong: $reply")
            }
            repliesB.forEachIndexed { i, reply ->
                assertEquals("echo:msg-b-${i + 1}", reply, "nodeB reply $i was wrong: $reply")
            }
        }

    /**
     * Adversarial: cast to an unknown node is silently swallowed — does not throw,
     * does not affect subsequent calls to reachable nodes.
     *
     * OTP semantics: remote casts are unreliable; transport failures are swallowed.
     * This matches RemoteNodeStub.cast which wraps transport errors in runCatching.
     */
    @Test
    fun `adversarial - cast to unknown node is silently swallowed`(): Unit = runBlocking {
        val transport = InMemoryTransport()
        val nodeA = LocalNode(NodeId("a", "cross-cast"))
        transport.addNode(nodeA)
        nodeA.register("svc", GenServers.startLink(this, slowEchoServer()))

        val ghostStub = RemoteNodeStub(NodeId("ghost", "cross-cast"), transport)
        // Must not throw.
        ghostStub.cast("svc", "fire-and-forget")

        // Subsequent call to a real node must still work.
        val realStub = RemoteNodeStub(nodeA.id, transport)
        val reply: String = realStub.call("svc", "ping", 5.seconds)
        assertEquals("echo:ping", reply, "real node call after ghost cast must succeed")
    }

    /**
     * Adversarial: mixed load — concurrent calls where some target a reachable node and
     * some target an unknown node. Successful calls must complete; unknown-node calls must
     * fail with transport errors. No cross-contamination of results.
     */
    @Test
    fun `adversarial - mixed reachable and unreachable concurrent calls are correctly separated`(): Unit =
        runBlocking {
            val transport = InMemoryTransport()
            val nodeA = LocalNode(NodeId("a", "cross-mixed"))
            transport.addNode(nodeA)
            nodeA.register("svc", GenServers.startLink(this, slowEchoServer()))

            val realStub = RemoteNodeStub(nodeA.id, transport)
            val ghostStub = RemoteNodeStub(NodeId("ghost", "cross-mixed"), transport)

            val realCalls = (1..20).map { i ->
                async { runCatching { realStub.call<String>("svc", "real-$i", 5.seconds) } }
            }
            val ghostCalls = (1..20).map { i ->
                async { runCatching { ghostStub.call<String>("svc", "ghost-$i", 5.seconds) } }
            }

            val realResults = withTimeout(10.seconds) { realCalls.map { it.await() } }
            val ghostResults = withTimeout(10.seconds) { ghostCalls.map { it.await() } }

            val realSuccesses = realResults.count { it.isSuccess }
            val realFailures = realResults.count { it.isFailure }
            val ghostFailures = ghostResults.count { it.isFailure }
            val ghostTransportErrors = ghostResults.count { it.exceptionOrNull() is IllegalStateException }

            assertEquals(20, realSuccesses, "all real calls should succeed; got $realSuccesses successes, $realFailures failures")
            assertEquals(20, ghostFailures, "all ghost calls should fail; got ${20 - ghostFailures} successes")
            assertEquals(20, ghostTransportErrors, "all ghost failures should be IllegalStateException; got $ghostTransportErrors")
        }
}
