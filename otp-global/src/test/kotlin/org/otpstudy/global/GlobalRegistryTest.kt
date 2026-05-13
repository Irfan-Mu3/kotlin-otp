package org.otpstudy.global

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
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

private class NoopServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class GlobalRegistryTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        GlobalRegistry.reset()
        GlobalRegistry.conflictResolver = GlobalRegistry.ConflictResolver.KeepFirst
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        GlobalRegistry.reset()
    }

    @Test
    fun `registerName succeeds for a new name`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        val result = GlobalRegistry.registerName("alice", ref)
        assertEquals(GlobalRegistry.RegisterResult.Ok, result)
    }

    @Test
    fun `whereisName returns registered ref`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("alice", ref)
        assertSame(ref, GlobalRegistry.whereisName<Unit>("alice"))
    }

    @Test
    fun `whereisName returns null for unknown name`() = runTest {
        assertNull(GlobalRegistry.whereisName<Unit>("nobody"))
    }

    @Test
    fun `KeepFirst returns Conflict on duplicate`() = runTest {
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("srv", r1)
        val result = GlobalRegistry.registerName("srv", r2)
        assertTrue(result is GlobalRegistry.RegisterResult.Conflict)
        assertSame(r1, (result as GlobalRegistry.RegisterResult.Conflict).existing)
        assertSame(r1, GlobalRegistry.whereisName<Unit>("srv"))
    }

    @Test
    fun `KeepLast replaces on duplicate`() = runTest {
        GlobalRegistry.conflictResolver = GlobalRegistry.ConflictResolver.KeepLast
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("srv", r1)
        val result = GlobalRegistry.registerName("srv", r2)
        assertEquals(GlobalRegistry.RegisterResult.Ok, result)
        assertSame(r2, GlobalRegistry.whereisName<Unit>("srv"))
    }

    @Test
    fun `Custom resolver picks winner`() = runTest {
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.conflictResolver = GlobalRegistry.ConflictResolver.Custom { _, _, incoming -> incoming }
        GlobalRegistry.registerName("srv", r1)
        GlobalRegistry.registerName("srv", r2)
        assertSame(r2, GlobalRegistry.whereisName<Unit>("srv"))
    }

    @Test
    fun `unregisterName removes entry`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("temp", ref)
        GlobalRegistry.unregisterName("temp")
        assertNull(GlobalRegistry.whereisName<Unit>("temp"))
    }

    @Test
    fun `name auto-unregistered when actor stops`() = runTest {
        val ref = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("dying", ref)
        ref.stop()
        ref.job.join()
        assertNull(GlobalRegistry.whereisName<Unit>("dying"))
    }

    @Test
    fun `registeredNames returns all names`() = runTest {
        val r1 = GenServers.startLink(scope, NoopServer())
        val r2 = GenServers.startLink(scope, NoopServer())
        GlobalRegistry.registerName("a", r1)
        GlobalRegistry.registerName("b", r2)
        val names = GlobalRegistry.registeredNames()
        assertTrue("a" in names)
        assertTrue("b" in names)
    }
}
