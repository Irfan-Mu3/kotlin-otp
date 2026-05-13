package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

private fun echoServer() = object : GenServer<Unit> {
    override suspend fun init() = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) =
        ReplyResult.Reply("echo:$request", state)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(state)
}

@ExtendWith(OtpStudyDebugTestExtension::class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class NodeTest {

    @Test
    fun `LocalNode register and call by name`() = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val node = LocalNode(NodeId("test-node"))
        val ref = GenServers.startLink(scope, echoServer())
        node.register("echo", ref)

        val result: String = node.call("echo", "world")
        assertEquals("echo:world", result)

        node.unregister("echo")
        assertNull(node.whereis<Unit>("echo"))
        scope.cancel()
    }

    @Test
    fun `LocalNode cast delivers to registered process`() = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val node = LocalNode(NodeId("cast-node"))
        val server = object : GenServer<MutableList<Any>> {
            override suspend fun init() = InitResult.Ok(mutableListOf<Any>())
            override suspend fun handleCall(r: Any, s: MutableList<Any>) =
                ReplyResult.Reply(s.toList(), s)
            override suspend fun handleCast(r: Any, s: MutableList<Any>): NoreplyResult<MutableList<Any>> {
                s.add(r); return NoreplyResult.Noreply(s)
            }
        }
        val ref = GenServers.startLink(scope, server)
        node.register("collector", ref)
        node.cast("collector", "msg1")
        delay(50)
        val msgs: List<Any> = ref.call("get")
        assertEquals(listOf("msg1"), msgs)
        scope.cancel()
    }

    @Test
    fun `InMemoryTransport routes between two LocalNodes`() = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val transport = InMemoryTransport()
        val nodeA = LocalNode(NodeId("a", "test"))
        val nodeB = LocalNode(NodeId("b", "test"))
        transport.addNode(nodeA)
        transport.addNode(nodeB)

        val ref = GenServers.startLink(scope, echoServer())
        nodeB.register("svc", ref)

        val result = transport.call(NodeId("b", "test"), "svc", "hello", 5.seconds)
        assertEquals("echo:hello", result)
        scope.cancel()
    }

    @Test
    fun `RemoteNodeStub routes calls via transport`() = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val transport = InMemoryTransport()
        val nodeB = LocalNode(NodeId("b", "remote"))
        transport.addNode(nodeB)

        nodeB.register("svc", GenServers.startLink(scope, echoServer()))

        val stub = RemoteNodeStub(NodeId("b", "remote"), transport)
        val result: String = stub.call("svc", "ping")
        assertEquals("echo:ping", result)
        scope.cancel()
    }
}
