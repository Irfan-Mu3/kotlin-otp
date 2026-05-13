package org.otpstudy.pg

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.GenServerRef
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private class NoopServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class PgTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        Pg.reset()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        Pg.reset()
    }

    @Test
    fun `join adds member to group`() = runTest {
        val pg = Pg.start("test", scope)
        val ref = GenServers.startLink(scope, NoopServer())
        pg.join("workers", ref)
        assertEquals(listOf(ref), pg.getMembers("workers"))
    }

    @Test
    fun `leave removes member from group`() = runTest {
        val pg = Pg.start("test", scope)
        val ref = GenServers.startLink(scope, NoopServer())
        pg.join("workers", ref)
        pg.leave("workers", ref)
        assertTrue(pg.getMembers("workers").isEmpty())
    }

    @Test
    fun `empty group returns empty list`() = runTest {
        val pg = Pg.start("test", scope)
        assertTrue(pg.getMembers("nonexistent").isEmpty())
    }

    @Test
    fun `member auto-removed when actor stops`() = runTest {
        val pg = Pg.start("test", scope)
        val ref = GenServers.startLink(scope, NoopServer())
        pg.join("workers", ref)
        ref.stop()
        ref.job.join()
        assertTrue(pg.getMembers("workers").isEmpty())
    }

    @Test
    fun `multiple members in group`() = runTest {
        val pg = Pg.start("test", scope)
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        val r3 = GenServers.startLink(scope, NoopServer())
        pg.join("workers", r1)
        pg.join("workers", r2)
        pg.join("workers", r3)
        assertEquals(3, pg.getMembers("workers").size)
    }

    @Test
    fun `whichGroups returns all groups with members`() = runTest {
        val pg = Pg.start("test", scope)
        val ref = GenServers.startLink(scope, NoopServer())
        pg.join("groupA", ref)
        pg.join("groupB", ref)
        val groups = pg.whichGroups()
        assertTrue("groupA" in groups)
        assertTrue("groupB" in groups)
    }

    @Test
    fun `broadcast sends cast to all members`() = runTest {
        val received = mutableListOf<Any>()
        val latch = kotlinx.coroutines.CompletableDeferred<Unit>()

        class CollectingServer : GenServer<Unit> {
            override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
            override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
            override suspend fun handleCast(request: Any, state: Unit): NoreplyResult<Unit> {
                synchronized(received) { received.add(request) }
                if (synchronized(received) { received.size } == 2) latch.complete(Unit)
                return NoreplyResult.Noreply(Unit)
            }
        }

        val pg = Pg.start("test", scope)
        val r1 = GenServers.startLink(scope, CollectingServer())
        val r2 = GenServers.startLink(scope, CollectingServer())
        pg.join("listeners", r1)
        pg.join("listeners", r2)
        pg.broadcast("listeners", "hello")
        latch.await()
        assertEquals(2, received.size)
    }

    @Test
    fun `separate scopes are independent`() = runTest {
        val scope1 = Pg.start("scope1", scope)
        val scope2 = Pg.start("scope2", scope)
        val ref = GenServers.startLink(scope, NoopServer())
        scope1.join("group", ref)
        assertTrue(scope2.getMembers("group").isEmpty())
    }

    @Test
    fun `Pg scope retrieval by name`() = runTest {
        Pg.start("myScope", scope)
        val retrieved = Pg.scope("myScope")
        assertEquals("myScope", retrieved.name)
    }

    @Test
    fun `accessing unknown scope throws`() = runTest {
        try {
            Pg.scope("doesNotExist")
            assertFalse(true, "expected error")
        } catch (_: IllegalStateException) { }
    }
}
