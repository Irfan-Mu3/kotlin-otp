package org.otpstudy.distribution

import java.util.concurrent.TimeUnit
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.registry.ProcessRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

@ExtendWith(OtpStudyDebugTestExtension::class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class KotlinNodeTransportLoopbackTest {

    @Test
    fun `TCP loopback call reaches remote registry GenServer`() {
        runBlocking {
            val nodeA = NodeId("a", "loopback")
            val nodeB = NodeId("b", "loopback")
            val regA = ProcessRegistry()
            val regB = ProcessRegistry()
            val ta = KotlinNodeTransport(nodeA, "", 0)
            val tb = KotlinNodeTransport(nodeB, "", 0)
            try {
                ta.startAccepting(this, regA)
                tb.startAccepting(this, regB)
                regB.register("svc", GenServers.startLink(this, echoServer()))
                ta.connectOut(this, regA, nodeB, "127.0.0.1", tb.boundPort)
                @Suppress("UNCHECKED_CAST")
                val r = ta.call(nodeB, "svc", "ping", 5.seconds) as String
                assertEquals("echo:ping", r)
            } finally {
                ta.close()
                tb.close()
                coroutineContext.job.cancelChildren()
            }
        }
    }

    private fun echoServer() = object : GenServer<Unit> {
        override suspend fun init() = InitResult.Ok(Unit)
        override suspend fun handleCall(request: Any, state: Unit) =
            ReplyResult.Reply("echo:$request", state)
        override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(state)
    }
}
