package org.otpstudy.genserver

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.test.runTest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private class OkServer : GenServer<Int> {
    override suspend fun init() = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int) = ReplyResult.Reply<Int>(state, state)
    override suspend fun handleCast(request: Any, state: Int) = NoreplyResult.Noreply(state + 1)
}

private class StopOnInitServer(val reason: TerminateReason = TerminateReason.Normal) : GenServer<Unit> {
    override suspend fun init() = InitResult.Stop(reason)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

private class CrashOnInitServer : GenServer<Unit> {
    override suspend fun init(): InitResult<Unit> = throw RuntimeException("init boom")
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class ProcLibTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
    }

    @Test
    fun `startLinkSync returns ref when init succeeds`() = runTest {
        val ref = GenServers.startLinkSync(scope, OkServer())
        assertTrue(ref.job.isActive)
        ref.stop()
    }

    @Test
    fun `startLinkSync throws InitFailedException when init returns Stop`() = runTest {
        assertFailsWith<InitFailedException> {
            GenServers.startLinkSync(scope, StopOnInitServer())
        }
    }

    @Test
    fun `startLinkSync propagates exception thrown from init`() = runTest {
        assertFailsWith<RuntimeException> {
            GenServers.startLinkSync(scope, CrashOnInitServer())
        }
    }

    @Test
    fun `startLinkSync with name works`() = runTest {
        val ref = GenServers.startLinkSync(scope, OkServer(), name = "sync-server")
        assertTrue(ref.job.isActive)
        ref.stop()
    }

    @Test
    fun `InitFailedException carries terminate reason`() = runTest {
        val ex = assertFailsWith<InitFailedException> {
            GenServers.startLinkSync(scope, StopOnInitServer(TerminateReason.Shutdown))
        }
        assertEquals(TerminateReason.Shutdown, ex.reason)
    }

    @Test
    fun `plain startLink does not throw when init stops`() = runTest {
        // Old behaviour: ref is returned; job is already dead
        val ref = GenServers.startLink(scope, StopOnInitServer())
        ref.job.join()
        assertFalse(ref.job.isActive)
    }

    @Test
    fun `withArena starts actor with ActorArena context element`() = runTest {
        // Use CompletableDeferred to safely communicate the arena reference across threads.
        val arenaDeferred = CompletableDeferred<org.otpstudy.memory.ActorArena?>()

        class ArenaProbeServer : GenServer<Unit> {
            override suspend fun init(): InitResult<Unit> {
                arenaDeferred.complete(currentCoroutineContext()[org.otpstudy.memory.ActorArena])
                return InitResult.Stop(TerminateReason.Normal)
            }
            override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
            override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
        }

        val ref = GenServers.startLink(scope, ArenaProbeServer(), withArena = true)
        val arena = arenaDeferred.await()
        ref.job.join()
        assertTrue(arena != null, "ActorArena should have been present in coroutine context")
    }

    @Test
    fun `withArena = false leaves ActorArena absent`() = runTest {
        val arenaDeferred = CompletableDeferred<org.otpstudy.memory.ActorArena?>()

        class ArenaProbeServer : GenServer<Unit> {
            override suspend fun init(): InitResult<Unit> {
                arenaDeferred.complete(currentCoroutineContext()[org.otpstudy.memory.ActorArena])
                return InitResult.Stop(TerminateReason.Normal)
            }
            override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
            override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
        }

        GenServers.startLink(scope, ArenaProbeServer(), withArena = false).job.join()
        val arena = arenaDeferred.await()
        assertTrue(arena == null, "ActorArena should be absent without withArena=true")
    }
}
