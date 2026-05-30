package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * CBR adversarial tests for claim `genserver+mailbox.control_priority_under_shutdown`.
 *
 * Claim: a GenServer draining the sys/control channel does not prevent its own orderly
 * Supervisor-initiated shutdown — the Job cancels promptly even under a rapid control flood.
 *
 * Evidence type: AdversarialTest / High
 */

private data object ControlFloodMsg : InfoMsg

private class ControlFloodServer(private val controlsProcessed: AtomicInteger) : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int) = ReplyResult.Reply(state, state)
    override suspend fun handleCast(request: Any, state: Int) = NoreplyResult.Noreply(state)
    override suspend fun handleInfo(msg: InfoMsg, state: Int): NoreplyResult<Int> {
        if (msg == ControlFloodMsg) controlsProcessed.incrementAndGet()
        return NoreplyResult.Noreply(state)
    }
}

class ControlPriorityUnderShutdownContractTest {

    /**
     * Adversarial: flood the control channel then call stop().
     *
     * The server must stop within a tight deadline even while control messages
     * are queued. Verifies that drainControl()'s non-blocking tryReceive loop
     * does not trap CancellationException delivery.
     */
    @Test
    fun `adversarial - control flood does not delay stop`(): Unit = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val processed = AtomicInteger(0)
        val ref = GenServers.startLink(scope, ControlFloodServer(processed), name = "ctrl-flood-stop")

        // Saturate the control channel with 10 000 messages before signalling stop.
        repeat(10_000) { ref.sendControl(ControlFloodMsg) }

        val stopStart = System.nanoTime()
        ref.stop()
        val stopMs = (System.nanoTime() - stopStart) / 1_000_000L

        assertTrue(!ref.job.isActive, "server job must be inactive after stop()")
        // Stop must complete well within 2 seconds regardless of the control backlog.
        assertTrue(stopMs < 2_000, "stop() took ${stopMs}ms — expected < 2000ms with control flood queued")

        scope.cancel()
    }

    /**
     * Adversarial: send control flood concurrently while the server is processing
     * calls, then cancel the scope (simulating supervisor shutdown).
     *
     * The scope cancel must complete within a tight deadline — the server must not
     * get stuck draining control messages indefinitely.
     */
    @Test
    fun `adversarial - concurrent control flood does not block scope cancellation`(): Unit = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val processed = AtomicInteger(0)
        val ref = GenServers.startLink(scope, ControlFloodServer(processed), name = "ctrl-flood-cancel")

        // Continuously flood control channel in background.
        val flooder = scope.launch {
            while (true) {
                ref.sendControl(ControlFloodMsg)
                // No delay — maximum pressure.
            }
        }

        // Let the flood build for a moment.
        delay(50)

        val cancelStart = System.nanoTime()
        scope.cancel()
        // Await the actor job directly — scope.cancel() is fire-and-forget.
        withTimeout(3.seconds) { ref.job.join() }
        val cancelMs = (System.nanoTime() - cancelStart) / 1_000_000L

        assertTrue(!ref.job.isActive, "server job must be inactive after scope cancel")
        assertTrue(cancelMs < 3_000, "job did not stop within 3s under continuous control flood (${cancelMs}ms)")

        flooder.cancel()
    }

    /**
     * Adversarial: interleave control flood with sys channel messages during shutdown.
     *
     * Both the sys channel (highest priority) and the control channel drain before
     * each user message. A rapid flood on both must not prevent timely shutdown.
     */
    @Test
    fun `adversarial - sys and control flood together do not delay shutdown`(): Unit = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val processed = AtomicInteger(0)
        val ref = GenServers.startLink(scope, ControlFloodServer(processed), name = "ctrl-sys-flood")

        // Flood both control and sys channels.
        repeat(5_000) { ref.sendControl(ControlFloodMsg) }
        // sys channel: use sysGetState requests — each is a real SysMsg.
        // Queue a batch asynchronously without waiting for replies.
        val sysFlooders = (1..50).map {
            launch { runCatching { ref.sysGetState() } }
        }

        delay(20)

        val stopStart = System.nanoTime()
        ref.stop()
        val stopMs = (System.nanoTime() - stopStart) / 1_000_000L

        assertTrue(!ref.job.isActive, "server job must be inactive after stop()")
        assertTrue(stopMs < 2_000, "stop() took ${stopMs}ms with sys+control flood (expected < 2000ms)")

        sysFlooders.forEach { it.cancel() }
        scope.cancel()
    }

    /**
     * Adversarial: verify that a server under control flood still processes
     * a user call before shutdown — control messages must not permanently
     * starve the user mailbox under finite load.
     *
     * This tests the interaction between drainControl (non-blocking) and the
     * select{} block that awaits both sys and user mailbox.
     */
    @Test
    fun `adversarial - user call completes despite prior control flood`(): Unit = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val processed = AtomicInteger(0)
        val ref = GenServers.startLink(scope, ControlFloodServer(processed), name = "ctrl-flood-call")

        // Queue 5 000 control messages.
        repeat(5_000) { ref.sendControl(ControlFloodMsg) }

        // User call must still complete within a reasonable deadline.
        val result: Int = withTimeout(5.seconds) { ref.call("snapshot") }

        // result is the state (Int), which is valid as long as the call completed.
        assertTrue(result >= 0, "call should return valid state; got $result")
        assertTrue(!ref.job.isActive.not(), "server should still be alive after call") // double-neg: it IS active

        scope.cancel()
    }
}
