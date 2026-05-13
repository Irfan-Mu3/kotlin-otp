package org.otpstudy.registry

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.GenServerRef
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame

private class Dummy : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>): InitResult<Unit> = InitResult.Ok(Unit)

    override suspend fun handleCall(
        request: Any,
        state: Unit,
    ): ReplyResult<Unit> = ReplyResult.Reply(null, state)

    override suspend fun handleCast(
        request: Any,
        state: Unit,
    ): NoreplyResult<Unit> = NoreplyResult.Noreply(state)
}

class ProcessRegistryTest {
    @Test
    fun registerLookupAndAutoUnregisterOnCompletion() =
        runBlocking {
            val reg = ProcessRegistry()
            val ref = GenServers.startLink(this, Dummy())
            reg.register("svc", ref)
            assertSame(ref, reg.lookup("svc"))
            ref.stop()
            delay(50)
            assertNull(reg.lookup("svc"))
        }

    @Test
    fun globalUnregister() =
        runBlocking {
            val ref = GenServers.startLink(this, Dummy())
            GlobalProcessRegistry.register("g", ref)
            assertNotNull(GlobalProcessRegistry.lookup("g"))
            GlobalProcessRegistry.unregister("g")
            ref.stop()
        }
}
