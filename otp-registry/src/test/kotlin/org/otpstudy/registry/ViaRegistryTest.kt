package org.otpstudy.registry

import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame

private class ViaDummy : GenServer<Unit> {
  override suspend fun init(self: GenServerRef<Unit>): InitResult<Unit> = InitResult.Ok(Unit)

  override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> =
    ReplyResult.Reply(null, state)

  override suspend fun handleCast(request: Any, state: Unit): NoreplyResult<Unit> =
    NoreplyResult.Noreply(state)
}

class ViaRegistryTest {
  @Test
  fun mapViaRegistry_registerWhereisUnregister(): Unit =
    runBlocking {
      val via = MapViaRegistry()
      val ref = GenServers.startLink(this, ViaDummy())
      via.register("via-svc", ref)
      assertSame(ref, via.whereis<Unit>("via-svc"))
      via.unregister("via-svc", ref)
      assertNull(via.lookup("via-svc"))
      ref.stop()
    }

  @Test
  fun globalProcessRegistryResolver_delegates(): Unit =
    runBlocking {
      val ref = GenServers.startLink(this, ViaDummy())
      GlobalProcessRegistry.register("resolver-test", ref)
      try {
        assertSame(ref, GlobalProcessRegistryResolver.whereis<Unit>("resolver-test"))
      } finally {
        GlobalProcessRegistry.unregister("resolver-test", ref)
        ref.stop()
      }
    }
}
