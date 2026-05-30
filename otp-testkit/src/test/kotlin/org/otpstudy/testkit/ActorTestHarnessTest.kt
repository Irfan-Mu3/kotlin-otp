package org.otpstudy.testkit

import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private class HarnessServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>): InitResult<Int> = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int): ReplyResult<Int> = ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: Int): NoreplyResult<Int> = NoreplyResult.Noreply(state)
}

class ActorTestHarnessTest {
    @Test
    fun `withGenServer stops actor in finally`(): Unit = runBlocking {
        var captured: GenServerRef<Int>? = null
        withGenServer(
            start = { GenServers.startLink(this, HarnessServer(), name = "harness-test") },
            block = { ref ->
                captured = ref
                assertTrue(ref.job.isActive)
            },
        )
        assertFalse(captured!!.job.isActive)
    }
}

