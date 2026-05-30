package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

private class HibernatingCounter : GenServer<Long> {
    override suspend fun init(self: GenServerRef<Long>) = InitResult.Ok(0L)

    override suspend fun handleCall(request: Any, state: Long): ReplyResult<Long> = when (request) {
        "get" -> ReplyResult.Reply(state, state)
        else  -> ReplyResult.Reply("unknown", state)
    }

    override suspend fun handleCast(request: Any, state: Long): NoreplyResult<Long> = when (request) {
        "hibernate" -> NoreplyResult.Hibernate(state) { msg, s ->
            when (msg) {
                "inc" -> NoreplyResult.Noreply(s + 1)
                else  -> NoreplyResult.Noreply(s)
            }
        }
        "inc"   -> NoreplyResult.Noreply(state + 1)
        "reset" -> NoreplyResult.Noreply(0L)
        else    -> NoreplyResult.Noreply(state)
    }
}

class HibernateTest {
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
    fun `actor wakes from hibernate on cast and applies onWake`() = runBlocking {
        val ref = GenServers.startLink(scope, HibernatingCounter())
        ref.cast("inc")
        ref.cast("inc")
        assertEquals(2L, ref.call("get"))

        ref.cast("hibernate")
        // Next cast wakes the actor via onWake
        ref.cast("inc")
        // Give the coroutine time to process
        kotlinx.coroutines.delay(50)
        assertEquals(3L, ref.call("get"))
        ref.stop()
    }

    @Test
    fun `actor wakes from hibernate on call`() = runBlocking {
        val ref = GenServers.startLink(scope, HibernatingCounter())
        ref.cast("hibernate")
        // A call message wakes the actor; handleCallFrom processes it normally
        val result: Long = ref.call("get")
        assertEquals(0L, result)
        ref.stop()
    }

    @Test
    fun `state is preserved across hibernate`() = runBlocking {
        val ref = GenServers.startLink(scope, HibernatingCounter())
        ref.cast("inc")
        ref.cast("inc")
        ref.cast("inc")
        ref.cast("hibernate")
        // Wake with a no-op cast to exercise onWake's default (returns Noreply with same state)
        ref.cast("noop")
        kotlinx.coroutines.delay(50)
        assertEquals(3L, ref.call("get"))
        ref.stop()
    }

    @Test
    fun `hibernate followed by normal ops works`() = runBlocking {
        val ref = GenServers.startLink(scope, HibernatingCounter())
        ref.cast("hibernate")
        ref.cast("inc")        // wakes via onWake -> state 1
        kotlinx.coroutines.delay(50)
        ref.cast("inc")        // normal handleCast -> state 2
        ref.cast("inc")        // normal handleCast -> state 3
        kotlinx.coroutines.delay(50)
        assertEquals(3L, ref.call("get"))
        ref.stop()
    }

    @Test
    fun `actor with hibernateAfter remains functional after idle timeout`() = runBlocking {
        val ref = GenServers.startLink(
            scope, HibernatingCounter(),
            hibernateAfter = 50.milliseconds,
        )
        ref.cast("inc"); ref.cast("inc")
        assertEquals(2L, ref.call("get"))

        // Let the actor idle past hibernateAfter
        delay(120)

        // Actor must still be alive and process messages normally
        ref.cast("inc")
        delay(20)
        assertEquals(3L, ref.call("get"))
        ref.stop()
    }

    @Test
    fun `actor cycles through multiple hibernateAfter timeouts without error`() = runBlocking {
        val ref = GenServers.startLink(
            scope, HibernatingCounter(),
            hibernateAfter = 30.milliseconds,
        )
        // Three idle windows pass
        delay(120)
        // Actor still alive
        ref.cast("inc")
        delay(20)
        assertEquals(1L, ref.call("get"))
        ref.stop()
    }
}
