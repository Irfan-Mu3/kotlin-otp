package org.otpstudy.supervisor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InfoMsg
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyHandle
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.ServerDownException
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * CBR adversarial tests for claim `genserver+supervisor.restart_deferred_reply`.
 *
 * Claim: a deferred reply in flight when a supervised GenServer crashes is correctly
 * handled — the caller receives [ServerDownException], no reply leak, no interference
 * with the restarted child.
 *
 * Evidence type: AdversarialTest / High
 */

private data object HoldReply : InfoMsg
private data object CrashNow : InfoMsg
private data object Ping : InfoMsg

/**
 * State is simply the pending ReplyHandle (nullable), kept as Any? to avoid deeply
 * nested generic type parameters that the Kotlin compiler cannot infer.
 *
 * On [HoldReply] call: parks caller via DeferReply, stores handle in state.
 * On [CrashNow] cast: throws RuntimeException — simulates abnormal exit with an in-flight reply.
 * On [Ping] call: returns "pong" — used to confirm the (restarted) server is healthy.
 */
private class DeferAndCrashServer : GenServer<Any?> {

    override suspend fun init(self: GenServerRef<Any?>) = InitResult.Ok<Any?>(null)

    override suspend fun handleCall(request: Any, state: Any?): ReplyResult<Any?> =
        when (request) {
            Ping -> ReplyResult.Reply("pong", state)
            else -> ReplyResult.Reply("unknown-request", state)
        }

    override suspend fun handleCallFrom(
        request: Any,
        state: Any?,
        from: ReplyHandle<Any?>,
    ): ReplyResult<Any?> =
        when (request) {
            HoldReply ->
                // Park caller: do NOT call from.reply(). Caller will hang until server
                // crashes and the death-watch fires ServerDownException.
                ReplyResult.DeferReply(from, from)
            else -> handleCall(request, state)
        }

    override suspend fun handleCast(request: Any, state: Any?): NoreplyResult<Any?> =
        when (request) {
            CrashNow -> throw RuntimeException("deliberate crash with deferred reply in flight")
            else -> NoreplyResult.Noreply(state)
        }
}

class DeferredReplyUnderRestartContractTest {

    /**
     * Adversarial: crash the server while a single deferred reply is in flight.
     * Caller must receive [ServerDownException] — not hang, not get a stale reply.
     */
    @Test
    fun `adversarial - deferred reply caller receives ServerDownException when server crashes`(): Unit =
        runBlocking {
            // Server scope: owns the GenServer job.
            val serverScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
            val ref = GenServers.startLink(serverScope, DeferAndCrashServer(), name = "deferred-crash")

            // runCatching inside async so the exception is captured, not propagated to runBlocking parent.
            val deferredCaller = async {
                runCatching { ref.call<Any?>(HoldReply, timeout = 5.seconds) }
            }
            delay(80) // give the call time to reach the server and be parked

            ref.cast(CrashNow)

            val result = withTimeout(2.seconds) { deferredCaller.await() }
            assertTrue(result.isFailure, "caller should have failed")
            assertTrue(
                result.exceptionOrNull() is ServerDownException,
                "expected ServerDownException, got ${result.exceptionOrNull()?.javaClass?.simpleName}",
            )
            assertTrue(!ref.job.isActive, "server job should be inactive after crash")

            serverScope.cancel()
        }

    /**
     * Adversarial: N concurrent callers all hold deferred replies; server crashes.
     * Every caller must receive [ServerDownException] — no hangers, no reply leaks.
     */
    @Test
    fun `adversarial - multiple concurrent deferred callers all receive ServerDownException on crash`(): Unit =
        runBlocking {
            // Server scope separate from runBlocking so callers (in runBlocking scope) survive server death.
            val serverScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
            val ref = GenServers.startLink(serverScope, DeferAndCrashServer(), name = "deferred-crash-multi")

            val concurrency = 20
            // Callers launched in the runBlocking scope — they are not children of serverScope.
            val callers = (1..concurrency).map {
                async { runCatching { ref.call<Any?>(HoldReply, timeout = 5.seconds) } }
            }
            delay(150) // let all callers reach the server and park

            ref.cast(CrashNow)

            val results = withTimeout(3.seconds) { callers.map { it.await() } }
            val failures = results.count { it.isFailure }
            val serverDowns = results.count { it.exceptionOrNull() is ServerDownException }

            assertTrue(failures == concurrency, "all $concurrency callers should fail; got $failures")
            assertTrue(serverDowns == concurrency, "all should be ServerDownException; got $serverDowns/$concurrency")

            serverScope.cancel()
        }

    /**
     * Adversarial: crash a supervised child with a deferred reply in flight; confirm
     * the supervisor restarts the child and the fresh instance responds to Ping.
     * Verifies no stale state leaks into the restarted child.
     */
    @Test
    fun `adversarial - supervisor restarts healthy child after crash with deferred reply in flight`(): Unit =
        runBlocking {
            val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
            val startCount = java.util.concurrent.atomic.AtomicInteger(0)

            // Expose the latest child ref via an atomic so the test can reach it.
            val latestRef = java.util.concurrent.atomic.AtomicReference<GenServerRef<*>?>()

            val flags = SupervisorFlags(
                strategy = SupervisorStrategy.OneForOne,
                intensity = 3,
                period = 10.seconds,
            )
            val childSpec = ChildSpec(
                id = "deferred-sup-child",
                restart = Restart.Permanent,
                shutdown = Shutdown.Timeout(2.seconds),
            ) {
                val ref = GenServers.startLink(this, DeferAndCrashServer(), name = "deferred-sup-gs")
                latestRef.set(ref)
                startCount.incrementAndGet()
                ref.job.join()
            }

            val supRef = Supervisor.startLink(scope, flags, listOf(childSpec))
            delay(150) // wait for first start

            val firstRef = latestRef.get()
            checkNotNull(firstRef) { "first child ref must be set" }

            // Park a caller on the first child.
            val caller = async { runCatching { firstRef.call<Any?>(HoldReply, timeout = 5.seconds) } }
            delay(80)

            // Crash the first child — supervisor should restart it.
            firstRef.cast(CrashNow)

            // Caller must fail with ServerDownException.
            val callResult = withTimeout(2.seconds) { caller.await() }
            assertTrue(callResult.isFailure, "caller should fail after crash")
            assertTrue(callResult.exceptionOrNull() is ServerDownException, "should be ServerDownException")

            // Wait for supervisor to restart.
            delay(300)
            val secondRef = latestRef.get()
            checkNotNull(secondRef) { "second (restarted) child ref must be set" }
            assertTrue(secondRef !== firstRef, "restarted child must be a different ref")

            // Confirm the restarted child is healthy.
            val pong: Any? = withTimeout(2.seconds) { secondRef.call(Ping) }
            assertTrue(pong == "pong", "restarted child must respond to Ping; got $pong")

            supRef.shutdown()
            scope.cancel()
        }
}
