package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private data class ContractFairState(val controlCount: Int = 0, val userCount: Int = 0)
private data object ContractControlTick : InfoMsg

private class ContractFairnessServer : GenServer<ContractFairState> {
    override suspend fun init(self: GenServerRef<ContractFairState>) = InitResult.Ok(ContractFairState())
    override suspend fun handleCall(request: Any, state: ContractFairState) = ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: ContractFairState): NoreplyResult<ContractFairState> =
        if (request == "user-work") NoreplyResult.Noreply(state.copy(userCount = state.userCount + 1)) else NoreplyResult.Noreply(state)
    override suspend fun handleInfo(msg: InfoMsg, state: ContractFairState): NoreplyResult<ContractFairState> =
        if (msg == ContractControlTick) NoreplyResult.Noreply(state.copy(controlCount = state.controlCount + 1)) else NoreplyResult.Noreply(state)
}

private class ContractHibernateServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(1)
    override suspend fun handleCall(request: Any, state: Int) = ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: Int): NoreplyResult<Int> =
        when (request) {
            "hibernate" -> NoreplyResult.Hibernate(state) { msg, s ->
                if (msg == "wake") NoreplyResult.Noreply(s + 1) else NoreplyResult.Noreply(s)
            }
            "wake" -> NoreplyResult.Noreply(state + 1)
            else -> NoreplyResult.Noreply(state)
        }
}

private class ContractEchoServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int) = ReplyResult.Reply(request, state)
    override suspend fun handleCast(request: Any, state: Int) = NoreplyResult.Noreply(state)
}

class GenServerRobustnessContractTest {
    @Test
    fun `deterministic contract - finite control flood still lets user work run`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val ref = GenServers.startLink(scope, ContractFairnessServer(), name = "contract-fairness")
        repeat(2_000) { ref.sendControl(ContractControlTick) }
        ref.cast("user-work")
        withTimeout(3.seconds) {
            while (true) {
                val state: ContractFairState = ref.call("snapshot")
                if (state.userCount > 0) break
                delay(5)
            }
        }
        scope.cancel()
    }

    @Test
    fun `adversarial contract - hibernation delays sys until wake without deadlock`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val ref = GenServers.startLink(scope, ContractHibernateServer(), name = "contract-hibernate")
        ref.cast("hibernate")
        delay(50)
        val pending = async { ref.sysGetState() }
        val early = withTimeoutOrNull(120.milliseconds) { pending.await() }
        assertEquals(null, early)
        ref.cast("wake")
        val resumed = withTimeout(2.seconds) { pending.await() }
        assertEquals(2, resumed)
        scope.cancel()
    }

    @Test
    fun `recovery contract - call fails promptly when server stops`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val ref = GenServers.startLink(scope, ContractEchoServer(), name = "contract-stop")
        ref.stop()
        assertFailsWith<ServerDownException> {
            ref.call<String>("after-stop", 400.milliseconds)
        }
        assertTrue(!ref.job.isActive)
        scope.cancel()
    }
}
