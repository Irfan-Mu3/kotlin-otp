package org.otpstudy.genserver

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.OtpDispatchers
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Proves that OtpDispatchers.loom() provides a concrete scalability benefit over
 * Dispatchers.IO when many actors perform concurrent blocking operations.
 *
 * **The property being tested:**
 * - IO dispatcher: N actors blocking simultaneously on a pool capped at P threads
 *   complete in ceil(N/P) batches → total time ≈ ceil(N/P) × blockingMs
 * - Loom dispatcher: virtual threads unmount from carriers when blocking → all N
 *   actors run concurrently → total time ≈ blockingMs regardless of N
 *
 * This is the JVM analog of OTP's dirty scheduler: Loom virtual threads allow
 * blocking I/O without stalling the carrier thread pool, just as OTP dirty schedulers
 * allow blocking NIFs without stalling the normal schedulers.
 *
 * **Real-world equivalent:** replace [Thread.sleep] with a JDBC query or HTTP call.
 * The benefit is identical: Loom's N concurrent blocking actors complete in ~1 DB
 * round-trip time rather than ceil(N/poolSize) round-trips.
 */
class LoomBenefitTest {

    /**
     * A GenServer that blocks its carrier for [blockingMs] milliseconds per call,
     * simulating a JDBC query or HTTP client call.
     */
    private inner class BlockingServer(private val blockingMs: Long) : GenServer<Unit> {
        override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)

        override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> {
            // Thread.sleep on a Loom virtual thread unmounts the VT from its carrier
            // (JDK 21+), freeing the carrier for other actors.
            // Thread.sleep on a platform thread (IO pool) holds the thread for blockingMs.
            Thread.sleep(blockingMs)
            return ReplyResult.Reply(Unit, Unit)
        }

        override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
    }

    @Test
    fun `Loom completes N concurrent blocking actors in 1 batch — constrained IO requires N over poolSize batches`() =
        runBlocking {
            val poolSize = 8
            val actorCount = poolSize * 4  // 32 actors — 4× the pool limit
            val blockingMs = 80L

            // Constrained IO dispatcher: max [poolSize] concurrent platform threads.
            // With 32 actors blocking for 80ms on 8 threads: ceil(32/8) × 80ms ≈ 320ms.
            val constrainedIO = Dispatchers.IO.limitedParallelism(poolSize)
            val ioWallMs = measureTimeMillis {
                coroutineScope {
                    (0 until actorCount).map { i ->
                        async {
                            val ref = GenServers.startLink(this, BlockingServer(blockingMs), context = constrainedIO, name = "io-$i")
                            ref.call<Unit>("block")
                            ref.stop()
                        }
                    }.forEach { it.await() }
                }
            }

            // Loom dispatcher: one virtual thread per actor. All 32 virtual threads
            // unmount when Thread.sleep fires → all complete in ~80ms regardless of N.
            val loomDispatcher = OtpDispatchers.loom() as kotlinx.coroutines.ExecutorCoroutineDispatcher
            val loomWallMs = try {
                measureTimeMillis {
                    coroutineScope {
                        (0 until actorCount).map { i ->
                            async {
                                val ref = GenServers.startLink(this, BlockingServer(blockingMs), context = loomDispatcher, name = "loom-$i")
                                ref.call<Unit>("block")
                                ref.stop()
                            }
                        }.forEach { it.await() }
                    }
                }
            } finally {
                loomDispatcher.close()
            }

            // Loom should complete in roughly 1 batch (≈ blockingMs).
            // Constrained IO should take roughly N/poolSize batches (≈ 4 × blockingMs).
            // Allow generous margins for scheduling jitter and CI variance.
            val speedup = ioWallMs.toDouble() / loomWallMs
            assertTrue(
                speedup >= 2.0,
                "Loom ($loomWallMs ms) should be at least 2× faster than constrained IO " +
                    "($ioWallMs ms) for $actorCount actors × ${blockingMs}ms blocking on a " +
                    "$poolSize-thread pool. Actual speedup: ${"%.1f".format(speedup)}×"
            )
        }
}
