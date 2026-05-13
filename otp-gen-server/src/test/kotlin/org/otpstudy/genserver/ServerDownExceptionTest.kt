package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.seconds
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private class EchoServer : GenServer<Unit> {
    override suspend fun init() = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(request, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

private class HangingServer : GenServer<Unit> {
    override suspend fun init() = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> {
        delay(60_000) // never actually replies during test window
        return ReplyResult.Reply(Unit, Unit)
    }
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class ServerDownExceptionTest {
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
    fun `call on stopped server throws ServerDownException`() = runBlocking {
        val ref = GenServers.startLink(scope, EchoServer())
        ref.stop()
        ref.job.join()
        assertFalse(ref.job.isActive)

        assertFailsWith<ServerDownException> {
            ref.call<Unit>("anything")
        }
    }

    @Test
    fun `ServerDownException carries the server pid`() = runBlocking {
        val ref = GenServers.startLink(scope, EchoServer())
        ref.stop()
        ref.job.join()

        val ex = assertFailsWith<ServerDownException> {
            ref.call<Unit>("anything")
        }
        assertTrue(ex.pid == ref.id, "ServerDownException.pid should match ref.id")
    }

    @Test
    fun `call completes with ServerDownException when server stops mid-call`() = runBlocking {
        val ref = GenServers.startLink(scope, HangingServer())

        var caughtException: ServerDownException? = null
        val callJob = launch {
            try {
                ref.call<Unit>("slow", timeout = 10.seconds)
            } catch (e: ServerDownException) {
                caughtException = e
            }
        }

        delay(50) // let call get into the await
        ref.job.cancel()  // crash the server while the call is in-flight
        callJob.join()

        assertTrue(caughtException != null, "expected ServerDownException")
        assertTrue(caughtException!!.pid == ref.id)
    }

    @Test
    fun `normal call succeeds without ServerDownException`() = runBlocking {
        val ref = GenServers.startLink(scope, EchoServer())
        val reply: String = ref.call("hello")
        assertTrue(reply == "hello")
        ref.stop()
    }
}
