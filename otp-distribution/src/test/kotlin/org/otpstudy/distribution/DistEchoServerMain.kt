package org.otpstudy.distribution

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.registry.ProcessRegistry

/**
 * Entry point for subprocess tests: listens on an ephemeral port, registers `svc` echo GenServer,
 * prints `PORT <n>` on stdout, then blocks until stdin closes.
 */
fun main() {
    runBlocking {
        val node = NodeId("remote", "proc")
        val reg = ProcessRegistry()
        val transport = KotlinNodeTransport(node, "", 0)
        transport.startAccepting(this, reg)
        reg.register("svc", GenServers.startLink(this, echoServer()))
        println("PORT ${transport.boundPort}")
        System.out.flush()
        try {
            withContext(Dispatchers.IO) {
                System.`in`.readBytes()
            }
        } finally {
            transport.close()
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
