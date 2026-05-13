package org.otpstudy.hotcode

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.CodeChangeRegistry
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

// --- V1: counter that increments by 1 ---
private class CounterV1 : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int) =
        ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: Int) =
        NoreplyResult.Noreply(state + 1)
    override suspend fun codeChange(oldVersion: String, newVersion: String, state: Int): Int {
        // V1 → V2 migration: multiply existing count by 10 to simulate a schema change
        return state * 10
    }
}

// --- V2: counter that increments by 2; codeChange doubles the inherited state ---
private class CounterV2 : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int) =
        ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: Int) =
        NoreplyResult.Noreply(state + 2)
    override suspend fun codeChange(oldVersion: String, newVersion: String, state: Int): Int =
        state * 10
}

class HotCodeTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        CodeChangeRegistry.clear()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        CodeChangeRegistry.clear()
    }

    @Test
    fun `hot upgrade migrates state and swaps implementation`() = runBlocking {
        val ref = GenServers.startLink(scope, CounterV1(), name = "counter-hot")

        // Send 3 casts to increment state to 3 via V1 (increment by 1)
        repeat(3) { ref.cast(Unit) }
        // Drain by calling — waits until casts are processed
        val beforeUpgrade = ref.call<Int>("get")
        assertEquals(3, beforeUpgrade)

        // Apply upgrade synchronously via sys channel (deterministic; mirrors OTP sys:change_code).
        // codeChange on V1 multiplies state * 10: 3 → 30
        ref.sysCodeChange(CounterV2::class.java, "1.0", "2.0")

        // After upgrade, V2 increments by 2: cast brings 30 → 32
        ref.cast(Unit)
        val afterUpgrade = ref.call<Int>("get")
        assertEquals(32, afterUpgrade)

        ref.stop()
    }

    @Test
    fun `CodeChangeRegistry consumeUpgrade returns null when empty`() {
        assertEquals(null, CodeChangeRegistry.consumeUpgrade("no-such-server"))
    }

    @Test
    fun `no upgrade scheduled means server continues with original impl`() = runBlocking {
        val ref = GenServers.startLink(scope, CounterV1(), name = "counter-plain")
        repeat(5) { ref.cast(Unit) }
        val state = ref.call<Int>("get")
        assertEquals(5, state)
        ref.stop()
    }
}
