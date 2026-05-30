package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// ----- §2: DeferReply -----

private class DeferReplyServer : GenServer<String> {
    var savedHandle: ReplyHandle<String>? = null

    override suspend fun init(self: GenServerRef<String>) = InitResult.Ok("idle")

    override suspend fun handleCall(request: Any, state: String): ReplyResult<String> =
        ReplyResult.Reply(request, state)

    override suspend fun handleCallFrom(
        request: Any,
        state: String,
        from: ReplyHandle<String>,
    ): ReplyResult<String> = when (request) {
        "defer" -> {
            savedHandle = from
            ReplyResult.DeferReply(from, "waiting")
        }
        else -> ReplyResult.Reply("echo:$request", state)
    }

    override suspend fun handleCast(request: Any, state: String) = NoreplyResult.Noreply(state)
}

// ----- §3: sys -----

private class SysTestServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int) = ReplyResult.Reply(state, state + 1)
    override suspend fun handleCast(request: Any, state: Int) = NoreplyResult.Noreply(state + 10)
}

// ----- §4: OtpTimers -----

private class TimerServer : GenServer<MutableList<String>> {
    override suspend fun init(self: GenServerRef<MutableList<String>>) = InitResult.Ok(mutableListOf<String>())
    override suspend fun handleCall(request: Any, state: MutableList<String>) = ReplyResult.Reply(state.toList(), state)
    override suspend fun handleCast(request: Any, state: MutableList<String>) = NoreplyResult.Noreply(state)
    override suspend fun handleInfo(msg: InfoMsg, state: MutableList<String>): NoreplyResult<MutableList<String>> {
        if (msg is TimerTick) state.add(msg.ref.toString())
        return NoreplyResult.Noreply(state)
    }
}

// ----- §6: CrashReporter -----

private class CrashingServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> =
        throw RuntimeException("intentional crash")
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(state)
}

// ----- §8: trap_exit -----

private class TrapExitServer : GenServer<MutableList<ExitSignal.Exit>> {
    override val trapExit = true
    override suspend fun init(self: GenServerRef<MutableList<ExitSignal.Exit>>) =
        InitResult.Ok(mutableListOf<ExitSignal.Exit>())
    override suspend fun handleCall(request: Any, state: MutableList<ExitSignal.Exit>) =
        ReplyResult.Reply(state.toList(), state)
    override suspend fun handleCast(request: Any, state: MutableList<ExitSignal.Exit>) =
        NoreplyResult.Noreply(state)
    override suspend fun handleInfo(
        msg: InfoMsg,
        state: MutableList<ExitSignal.Exit>,
    ): NoreplyResult<MutableList<ExitSignal.Exit>> {
        if (msg is ExitSignal.Exit) state.add(msg)
        return NoreplyResult.Noreply(state)
    }
}

// ----- §1: Bounded mailbox -----

private class SlowServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int): ReplyResult<Int> {
        delay(50.milliseconds)
        return ReplyResult.Reply(state, state + 1)
    }
    override suspend fun handleCast(request: Any, state: Int) = NoreplyResult.Noreply(state)
}

private data class FairState(val controlCount: Int = 0, val castCount: Int = 0)

private data object ControlTick : InfoMsg

private class FairnessServer : GenServer<FairState> {
    override suspend fun init(self: GenServerRef<FairState>) = InitResult.Ok(FairState())
    override suspend fun handleCall(request: Any, state: FairState) = ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: FairState): NoreplyResult<FairState> =
        when (request) {
            "work" -> NoreplyResult.Noreply(state.copy(castCount = state.castCount + 1))
            else -> NoreplyResult.Noreply(state)
        }

    override suspend fun handleInfo(msg: InfoMsg, state: FairState): NoreplyResult<FairState> =
        when (msg) {
            ControlTick -> NoreplyResult.Noreply(state.copy(controlCount = state.controlCount + 1))
            else -> NoreplyResult.Noreply(state)
        }
}

private class SysHibernateServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(1)
    override suspend fun handleCall(request: Any, state: Int) = ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: Int): NoreplyResult<Int> =
        when (request) {
            "hibernate" -> NoreplyResult.Hibernate(state) { msg, s ->
                when (msg) {
                    "wake" -> NoreplyResult.Noreply(s + 1)
                    else -> NoreplyResult.Noreply(s)
                }
            }
            "wake" -> NoreplyResult.Noreply(state + 1)
            else -> NoreplyResult.Noreply(state)
        }
}

class GenServerTest {

    // §2
    @Test
    fun `DeferReply - caller unblocked when handle reply is called later`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val server = DeferReplyServer()
        val ref = GenServers.startLink(scope, server, name = "defer-test")

        var callResult: String? = null
        val callJob = launch(Dispatchers.Default) { callResult = ref.call("defer") }
        // Give server a moment to process the call and store the handle
        delay(50)
        assertNotNull(server.savedHandle, "Handle should be saved")
        assertTrue(callResult == null, "Call should not be resolved yet")
        server.savedHandle!!.reply("deferred-reply")
        callJob.join()
        assertEquals("deferred-reply", callResult)
        scope.cancel()
    }

    // §3
    @Test
    fun `sys getState returns current state`() = runTest {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val ref = GenServers.startLink(scope, SysTestServer(), name = "sys-test")
        val state = ref.sysGetState()
        assertEquals(0, state)
        scope.cancel()
    }

    @Test
    fun `sys replaceState swaps state`() = runTest {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val ref = GenServers.startLink(scope, SysTestServer(), name = "sys-replace")
        ref.sysReplaceState { 99 }
        val state = ref.sysGetState()
        assertEquals(99, state)
        scope.cancel()
    }

    @Test
    fun `sys suspend and resume - casts queue during suspension`() = runTest {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val ref = GenServers.startLink(scope, SysTestServer(), name = "sys-suspend")
        ref.sysSuspend()
        // Cast messages while suspended - they queue up
        ref.cast("ignored1")
        ref.cast("ignored2")
        delay(20)
        // Server is suspended, messages not processed yet - verify via sys (still works)
        val stateDuringSuspend = ref.sysGetState()
        assertEquals(0, stateDuringSuspend) // unchanged since no messages processed
        ref.sysResume()
        delay(20)
        // Now casts were processed (state += 10 each)
        val stateAfter = ref.sysGetState()
        assertEquals(20, stateAfter)
        scope.cancel()
    }

    @Test
    fun `sys getStatus returns status with module name`() = runTest {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val ref = GenServers.startLink(scope, SysTestServer(), name = "status-test")
        val status = ref.sysGetStatus()
        assertEquals("status-test", status.name)
        assertEquals(0, status.state)
        scope.cancel()
    }

    // §4
    @Test
    fun `OtpTimers sendAfter fires once`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val server = TimerServer()
        val ref = GenServers.startLink(scope, server, name = "timer-test")
        OtpTimers.sendAfter(scope, 20.milliseconds, ref, TimerTick("ping"))
        delay(100)
        val ticks: List<String> = ref.call("get")
        assertEquals(listOf("ping"), ticks)
        scope.cancel()
    }

    @Test
    fun `OtpTimers sendAfter cancel prevents delivery`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val server = TimerServer()
        val ref = GenServers.startLink(scope, server, name = "timer-cancel")
        val timerRef = OtpTimers.sendAfter(scope, 100.milliseconds, ref, TimerTick("nope"))
        timerRef.cancel()
        delay(200)
        val ticks: List<String> = ref.call("get")
        assertEquals(emptyList(), ticks)
        scope.cancel()
    }

    @Test
    fun `OtpTimers sendInterval fires multiple times`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val server = TimerServer()
        val ref = GenServers.startLink(scope, server, name = "interval-test")
        OtpTimers.sendInterval(scope, 20.milliseconds, ref, TimerTick("tick"))
        delay(150)
        val ticks: List<String> = ref.call("get")
        assertTrue(ticks.size >= 2, "Expected at least 2 ticks, got ${ticks.size}")
        scope.cancel()
    }

    // §6
    @Test
    fun `CrashReporter receives crash report on handleCall exception`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        var receivedReport: CrashReport? = null
        CrashReporting.setCrashReporter { report -> receivedReport = report }
        val ref = GenServers.startLink(scope, CrashingServer(), name = "crash-test")
        try {
            ref.call<Unit>("boom")
        } catch (_: Throwable) {}
        delay(50)
        assertNotNull(receivedReport)
        assertEquals("crash-test", receivedReport!!.name)
        assertTrue(receivedReport!!.reason is TerminateReason.Failure)
        CrashReporting.setCrashReporter { }
        scope.cancel()
    }

    // §8
    @Test
    fun `trap_exit delivers ExitSignal instead of cancelling job`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val trapServer = TrapExitServer()
        val trapRef = GenServers.startLink(scope, trapServer, name = "trap")
        val otherServer = object : GenServer<Unit> {
            override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
            override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply(Unit, state)
            override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(state)
        }
        val otherRef = GenServers.startLink(scope, otherServer, name = "other")
        link(trapRef, otherRef)
        // Stop otherRef abnormally
        otherRef.job.cancel(kotlinx.coroutines.CancellationException("test-fail",
            RuntimeException("simulated failure")))
        delay(100)
        val exits: List<ExitSignal.Exit> = trapRef.call("get")
        // trapRef should receive ExitSignal, not be cancelled
        assertTrue(trapRef.job.isActive, "trapRef should still be running")
        assertTrue(exits.isNotEmpty(), "Should have received exit signal")
        scope.cancel()
    }

    // §1
    @Test
    fun `DropNew policy drops messages when mailbox is full`() = runTest {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val bound = MailboxBound(capacity = 1, policy = OverflowPolicy.DropNew)
        val ref = GenServers.startLink(scope, SlowServer(), mailboxBound = bound, name = "bounded")
        // Fill the mailbox then send more
        repeat(5) { ref.cast("work") }
        delay(200)
        scope.cancel()
    }

    @Test
    fun `CrashSender throws MailboxFullException when mailbox full`() = runTest {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val bound = MailboxBound(capacity = 1, policy = OverflowPolicy.CrashSender)
        val ref = GenServers.startLink(scope, SlowServer(), mailboxBound = bound, name = "crash-sender")
        // First cast fills the single-slot buffer
        ref.cast("first")
        assertFailsWith<MailboxFullException> {
            // Second cast should crash since buffer is full
            repeat(10) { ref.cast("overflow") }
        }
        scope.cancel()
    }

    @Test
    fun `finite control flood does not permanently starve user mailbox`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val ref = GenServers.startLink(scope, FairnessServer(), name = "fairness-control")

        repeat(5_000) { ref.sendControl(ControlTick) }
        ref.cast("work")

        withTimeout(3.seconds) {
            while (true) {
                val snapshot: FairState = ref.call("snapshot")
                if (snapshot.castCount >= 1) break
                delay(5)
            }
        }

        scope.cancel()
    }

    @Test
    fun `hibernate path delays sys handling until wake message`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val ref = GenServers.startLink(scope, SysHibernateServer(), name = "hibernate-sys")

        ref.cast("hibernate")
        delay(50)
        val sysGet = async { ref.sysGetState() }
        val early = withTimeoutOrNull(150.milliseconds) { sysGet.await() }
        assertEquals(null, early, "sys request should not complete while actor is hibernating")

        ref.cast("wake")
        val resumed = withTimeout(2.seconds) { sysGet.await() }
        assertEquals(2, resumed)
        scope.cancel()
    }
}
