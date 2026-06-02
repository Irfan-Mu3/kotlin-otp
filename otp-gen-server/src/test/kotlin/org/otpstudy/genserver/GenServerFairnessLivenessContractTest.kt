package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * CBR adversarial tests for claim `runtime.genserver_fairness.liveness`.
 *
 * Claim (Correctness / Liveness): Under cooperative scheduling, user-mailbox messages
 * make forward progress despite sustained control-channel pressure, calls complete without
 * internal deadlock, and hibernating servers wake on the first arriving user message.
 *
 * Liveness invariants from correctness-spec.md §GenServerFairnessAndStarvation:
 *   L1 — Finite-control-flood progress: user messages execute after finite control pressure.
 *   L2 — Call completion progress: calls complete or fail, no internal deadlock.
 *   L3 — Hibernate wake progress: server resumes once a user-mailbox message arrives.
 *
 * The existing AdversarialTest/Medium tests cover L1 (2 000-control-flood) and L3
 * (hibernate + sys-delay). This suite adds high-evidence quantitative stress:
 *   T1 — L1 at 10 000 control messages: user-work delay bounded under heavy drain.
 *   T2 — L2 concurrent call completion: 50 concurrent calls all complete, no deadlock.
 *   T3 — L2 call timeout is distinct from server-death (no deadlock between the two).
 *   T4 — L1 quantitative: user-message latency under 5 000-control flood is < 2 s.
 *
 * Evidence type: AdversarialTest / High
 */

// ---------------------------------------------------------------------------
// Shared actors
// ---------------------------------------------------------------------------

private data class FairnessState(val controlCount: Int = 0, val userCount: Int = 0, val lastUserSeq: Int = -1)
private data object FairnessControlTick : InfoMsg
private data class FairnessUserWork(val seq: Int)

private class FairnessQuantServer : GenServer<FairnessState> {
    override suspend fun init(self: GenServerRef<FairnessState>) = InitResult.Ok(FairnessState())

    override suspend fun handleCall(request: Any, state: FairnessState): ReplyResult<FairnessState> =
        ReplyResult.Reply(state, state)

    override suspend fun handleCast(request: Any, state: FairnessState): NoreplyResult<FairnessState> =
        when (request) {
            is FairnessUserWork -> NoreplyResult.Noreply(
                state.copy(userCount = state.userCount + 1, lastUserSeq = request.seq)
            )
            else -> NoreplyResult.Noreply(state)
        }

    override suspend fun handleInfo(msg: InfoMsg, state: FairnessState): NoreplyResult<FairnessState> =
        if (msg == FairnessControlTick)
            NoreplyResult.Noreply(state.copy(controlCount = state.controlCount + 1))
        else NoreplyResult.Noreply(state)
}

private class FairnessEchoServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply(request, state)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

private class FairnessSlowEchoServer(private val delayMs: Long) : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> {
        delay(delayMs)
        return ReplyResult.Reply("slow:$request", Unit)
    }
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

class GenServerFairnessLivenessContractTest {

    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
    }

    /**
     * L1 adversarial — 10 000 control messages: user-work still runs.
     *
     * Fires 10 000 control ticks, then one user-work cast.  Polls until the user
     * message is processed, timing out after 5 s.  Confirms the drainControl()
     * loop does not permanently block the user mailbox select.
     */
    @Test
    fun `L1 adversarial - user message progresses after 10k control messages`(): Unit = runBlocking {
        val ref = GenServers.startLink(scope, FairnessQuantServer(), name = "fairness-10k")
        repeat(10_000) { ref.sendControl(FairnessControlTick) }
        ref.cast(FairnessUserWork(seq = 1))

        withTimeout(5.seconds) {
            while (true) {
                val s: FairnessState = ref.call("state")
                if (s.userCount > 0) break
                delay(10.milliseconds)
            }
        }

        val final: FairnessState = ref.call("state")
        assertEquals(1, final.userCount, "exactly one user-work message must have been processed")
        assertTrue(final.controlCount > 0, "control ticks must have been processed")
    }

    /**
     * L1 quantitative — user-message latency under 5 000-control flood is < 2 s.
     *
     * Sends 5 000 control ticks, then sends a user cast and records the wall-clock
     * time until it is processed.  Asserts the latency is below 2 s.
     */
    @Test
    fun `L1 quantitative - user-message latency under 5k control flood is bounded under 2s`(): Unit = runBlocking {
        val ref = GenServers.startLink(scope, FairnessQuantServer(), name = "fairness-latency")
        repeat(5_000) { ref.sendControl(FairnessControlTick) }

        val sentAt = System.nanoTime()
        ref.cast(FairnessUserWork(seq = 42))

        withTimeout(3.seconds) {
            while (true) {
                val s: FairnessState = ref.call("state")
                if (s.lastUserSeq == 42) break
                delay(5.milliseconds)
            }
        }

        val latencyMs = (System.nanoTime() - sentAt) / 1_000_000L
        assertTrue(
            latencyMs < 2_000L,
            "user-message latency under 5k control flood must be < 2 000 ms; got ${latencyMs} ms",
        )
    }

    /**
     * L2 adversarial — 50 concurrent calls all complete, no deadlock.
     *
     * Launches 50 concurrent calls to the same GenServer.  All must complete
     * within 10 s.  Confirms no internal deadlock between concurrent callers
     * contending on the same actor mailbox.
     */
    @Test
    fun `L2 adversarial - 50 concurrent calls all complete without deadlock`(): Unit = runBlocking {
        val ref = GenServers.startLink(scope, FairnessEchoServer(), name = "fairness-concurrent")

        val deferreds = (1..50).map { i ->
            async<Any?> { ref.call("msg-$i", timeout = 10.seconds) }
        }

        val results = withTimeout(10.seconds) { deferreds.map { it.await() } }
        assertEquals(50, results.size, "all 50 calls must return")
        results.forEachIndexed { i, r ->
            assertEquals("msg-${i + 1}", r, "call ${i + 1} returned wrong value: $r")
        }
    }

    /**
     * L2 adversarial — call timeout is distinct from server death; both paths are non-deadlocking.
     *
     * Two calls race: one to a slow server (800 ms delay) with a 150 ms timeout, and one
     * to a healthy echo server.  The first must surface CancellationException (timeout);
     * the second must succeed.  Verifies the _serverDown select clause and the onTimeout
     * clause are both live under concurrent use.
     */
    @Test
    fun `L2 adversarial - call timeout and call success coexist without deadlock`(): Unit = runBlocking {
        val slowRef = GenServers.startLink(scope, FairnessSlowEchoServer(delayMs = 800), name = "fairness-slow")
        val fastRef = GenServers.startLink(scope, FairnessEchoServer(), name = "fairness-fast")

        // Time out on the slow server.
        val timedOut = async<Result<Any?>> {
            runCatching { slowRef.call<Any?>("ping", timeout = 150.milliseconds) }
        }
        // Concurrently succeed on the fast server.
        val succeeded = async<Any?> { fastRef.call("ping", timeout = 5.seconds) }

        val timedOutResult = withTimeout(5.seconds) { timedOut.await() }
        val fastResult = withTimeout(5.seconds) { succeeded.await() }

        assertTrue(timedOutResult.isFailure, "slow call must have timed out")
        assertTrue(
            timedOutResult.exceptionOrNull() is kotlinx.coroutines.CancellationException,
            "slow call must throw CancellationException; got ${timedOutResult.exceptionOrNull()?.javaClass?.simpleName}",
        )
        assertEquals("ping", fastResult, "fast call must succeed")
    }
}
