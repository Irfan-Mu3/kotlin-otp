package org.otpstudy.distribution

import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private fun echoServer() = object : GenServer<Unit> {
    override suspend fun init() = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) =
        ReplyResult.Reply("echo:$request", state)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(state)
}

@ExtendWith(OtpStudyDebugTestExtension::class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class DistributedGenServersTest {

    @Test
    fun `multiCall gathers replies from two nodes`() {
        runBlocking {
            // Do not use runBlocking(Dispatchers.Default): blocks a Default worker while GenServers
            // and multiCall's async children also use Default — can deadlock under parallel calls.
            val transport = InMemoryTransport()
            val na = LocalNode(NodeId("a", "cluster"))
            val nb = LocalNode(NodeId("b", "cluster"))
            transport.addNode(na)
            transport.addNode(nb)
            na.register("svc", GenServers.startLink(this, echoServer()))
            nb.register("svc", GenServers.startLink(this, echoServer()))

            val r =
                DistributedGenServers.multiCall<String>(
                    nodes = listOf(NodeId("a", "cluster"), NodeId("b", "cluster")),
                    name = "svc",
                    request = "ping",
                    transport = transport,
                    timeout = 5.seconds,
                )
            assertEquals(2, r.replies.size)
            assertTrue(r.noReplyNodes.isEmpty())
            val byNode = r.replies.toMap()
            assertEquals("echo:ping", byNode[NodeId("a", "cluster")])
            assertEquals("echo:ping", byNode[NodeId("b", "cluster")])
            // runBlocking waits for child jobs; GenServers may not have exited yet — cancel explicitly
            // so JUnit can start the next test (see kotlin-coroutines-boundaries / GenServer lifecycle).
            coroutineContext.job.cancelChildren()
        }
    }

    @Test
    fun `abcast does not throw when one node missing`() {
        runBlocking {
            val transport = InMemoryTransport()
            val na = LocalNode(NodeId("a", "cluster"))
            transport.addNode(na)
            na.register("svc", GenServers.startLink(this, echoServer()))
            DistributedGenServers.abcast(
                nodes = listOf(NodeId("a", "cluster"), NodeId("ghost", "cluster")),
                name = "svc",
                message = "hi",
                transport = transport,
            )
            coroutineContext.job.cancelChildren()
        }
    }
}
