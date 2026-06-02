package org.otpstudy.investigation

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.distribution.InMemoryTransport
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.distribution.RemoteNodeStub
import org.otpstudy.distribution.KotlinNodeTransport
import org.otpstudy.registry.ProcessRegistry
import org.otpstudy.core.OtpDispatchers
import org.otpstudy.genserver.CachedReadRef
import org.otpstudy.genserver.CacheableState
import org.otpstudy.genserver.CachingGenServer
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServerRouters
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.MailboxBound
import org.otpstudy.genserver.MailboxFullException
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.OverflowPolicy
import org.otpstudy.genserver.LongTaskBackpressurePolicy
import org.otpstudy.genserver.LongTaskBoundary
import org.otpstudy.genserver.LongTaskRejectedException
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.mailbox.SelectiveMailbox
import org.otpstudy.supervisor.ChildSpec
import org.otpstudy.supervisor.DynamicSupervisor
import org.otpstudy.supervisor.DynamicSupervisorRef
import org.otpstudy.supervisor.SimpleOneForOneTemplate
import org.otpstudy.supervisor.Supervisor
import org.otpstudy.supervisor.SupervisorFlags
import org.otpstudy.supervisor.SupervisorStrategy
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.max
import kotlin.math.sqrt
import kotlin.system.measureNanoTime
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

private data class BenchResult(
    val name: String,
    val phase: String = "steady",
    val iterations: Int,
    val p50Micros: Double,
    val p95Micros: Double,
    val p99Micros: Double = 0.0,
    val p999Micros: Double = 0.0,
    val throughputPerSec: Double,
    val notes: String = "",
)

private data class BenchProfile(
    val name: String,
    val iterations: Int,
    val tailIterations: Int,
    val selectiveDepths: List<Int>,
    val selectiveSamples: Int,
    val restartCrashes: Int,
    val memoryCasts: Int,
    val concurrentCallerCounts: List<Int>,
    val callsPerCaller: Int,
    val poolSize: Int,
    val poolCallerCounts: List<Int>,
    val poolHoldMs: Long,
    val boundedMailboxCapacity: Int,
    val boundedMailboxSenders: Int,
    val rounds: Int,
    val tcpIterations: Int,
    val memoryActorCounts: List<Int>,
)

private data class BenchAggregate(
    val scenario: String,
    val phase: String,
    val rounds: Int,
    val meanP50Micros: Double,
    val stddevP50Micros: Double,
    val meanP95Micros: Double,
    val stddevP95Micros: Double,
    val meanP99Micros: Double,
    val stddevP99Micros: Double,
    val meanThroughput: Double,
    val stddevThroughput: Double,
    val notes: String,
)

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

private class EchoServer : GenServer<Int> {
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)

    override suspend fun handleCall(request: Any, state: Int): ReplyResult<Int> = when (request) {
        "count" -> ReplyResult.Reply(state, state)
        else -> ReplyResult.Reply(request, state)
    }

    override suspend fun handleCast(request: Any, state: Int): NoreplyResult<Int> = when (request) {
        "inc" -> NoreplyResult.Noreply(state + 1)
        else -> NoreplyResult.Noreply(state)
    }
}

private fun percentile(values: List<Long>, pct: Double): Double {
    if (values.isEmpty()) return 0.0
    val sorted = values.sorted()
    val idx = ((pct / 100.0) * (sorted.size - 1)).toInt()
    return sorted[max(0, idx)] / 1_000.0
}

private fun Double.pretty(): String = "%.2f".format(this)

private fun mean(values: List<Double>): Double = if (values.isEmpty()) 0.0 else values.sum() / values.size

private fun stddev(values: List<Double>): Double {
    if (values.size < 2) return 0.0
    val m = mean(values)
    val variance = values.sumOf { (it - m) * (it - m) } / values.size
    return sqrt(variance)
}

// ---------------------------------------------------------------------------
// Scenario: gen_server_call_roundtrip (existing, extended with p99/p999)
// ---------------------------------------------------------------------------

private fun runCallBenchmark(
    scope: CoroutineScope,
    iterations: Int,
    fastReply: Boolean = false,
): BenchResult = runBlocking {
    val benchName = if (fastReply) "gen_server_call_fastReply_roundtrip" else "gen_server_call_roundtrip"
    val ref = GenServers.startLink(scope, EchoServer(), name = benchName, fastReply = fastReply)
    repeat(2_000) { ref.call<Any>("warmup") }
    val latencies = ArrayList<Long>(iterations)
    val totalNanos = measureNanoTime {
        repeat(iterations) {
            val dt = measureNanoTime { ref.call<Any>("ping") }
            latencies.add(dt)
        }
    }
    ref.stop()
    BenchResult(
        name = benchName,
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
        notes = if (fastReply) "yield() after reply; reduces dispatcher round-trips on hot path" else "",
    )
}

// ---------------------------------------------------------------------------
// Scenario B: gen_server_call_p99_tail_latency
// Higher iteration count to expose GC-spike tail beyond p99.
// ---------------------------------------------------------------------------

private fun runTailLatencyBenchmark(scope: CoroutineScope, iterations: Int): BenchResult = runBlocking {
    val ref = GenServers.startLink(scope, EchoServer(), name = "tail-bench")
    repeat(2_000) { ref.call<Any>("warmup") }
    val latencies = ArrayList<Long>(iterations)
    val totalNanos = measureNanoTime {
        repeat(iterations) {
            val dt = measureNanoTime { ref.call<Any>("ping") }
            latencies.add(dt)
        }
    }
    ref.stop()
    val p50 = percentile(latencies, 50.0)
    val p95 = percentile(latencies, 95.0)
    val p99 = percentile(latencies, 99.0)
    val p999 = percentile(latencies, 99.9)
    BenchResult(
        name = "gen_server_call_p99_tail_latency",
        iterations = iterations,
        p50Micros = p50,
        p95Micros = p95,
        p99Micros = p99,
        p999Micros = p999,
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
        notes = "tail: p99=${p99.pretty()}us p999=${p999.pretty()}us; GC spikes visible in p999",
    )
}

// ---------------------------------------------------------------------------
// Scenario: gen_server_cast_enqueue (existing)
// ---------------------------------------------------------------------------

private fun runCastBenchmark(scope: CoroutineScope, iterations: Int): BenchResult = runBlocking {
    val ref = GenServers.startLink(scope, EchoServer(), name = "cast-bench")
    repeat(2_000) { ref.cast("inc") }
    while (ref.call<Int>("count") < 2_000) delay(1)
    val latencies = ArrayList<Long>(iterations)
    val totalNanos = measureNanoTime {
        repeat(iterations) {
            val dt = measureNanoTime { ref.cast("inc") }
            latencies.add(dt)
        }
    }
    val drainStart = System.nanoTime()
    while (ref.call<Int>("count") < (2_000 + iterations)) delay(1)
    val drainNanos = System.nanoTime() - drainStart
    ref.stop()
    val drainThroughput = iterations * 1e9 / drainNanos
    BenchResult(
        name = "gen_server_cast_enqueue",
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
        notes = "enqueue speed only; service throughput (drain rate): ${drainThroughput.pretty()} ops/s",
    )
}

// ---------------------------------------------------------------------------
// Scenario: selective_receive_depth_N (existing)
// ---------------------------------------------------------------------------

private fun runSelectiveReceive(depth: Int, samples: Int): BenchResult = runBlocking {
    val ch = Channel<Int>(Channel.UNLIMITED)
    val box = SelectiveMailbox(ch)
    repeat(depth) { ch.send(0) }
    ch.send(1)
    val latencies = ArrayList<Long>(samples)
    repeat(samples) {
        repeat(depth) { ch.send(0) }
        ch.send(1)
        val dt = measureNanoTime {
            box.receive { it == 1 }
            box.flushSaved()
            repeat(depth + 1) { box.receive { true } }
        }
        latencies.add(dt)
    }
    val total = latencies.sum().toDouble()
    BenchResult(
        name = "selective_receive_depth_$depth",
        iterations = latencies.size,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = latencies.size * 1_000_000_000.0 / total,
        notes = "single match behind $depth non-matching messages; O(n) saved-list scan",
    )
}

// ---------------------------------------------------------------------------
// Scenario: supervisor_restart_storm_recovery (existing)
// ---------------------------------------------------------------------------

private fun runSupervisorRestartStorm(scope: CoroutineScope): BenchResult = runBlocking {
    runSupervisorRestartStorm(scope, crashCount = 40)
}

private fun runSupervisorRestartStorm(scope: CoroutineScope, crashCount: Int): BenchResult = runBlocking {
    val starts = java.util.concurrent.atomic.AtomicInteger(0)
    val settled = CompletableDeferred<Unit>()
    val spec = ChildSpec(
        id = "restart-storm",
        restart = Restart.Permanent,
        shutdown = Shutdown.BrutalKill,
    ) {
        val n = starts.incrementAndGet()
        if (n <= crashCount) error("intentional crash #$n")
        settled.complete(Unit)
        delay(Long.MAX_VALUE)
    }
    val flags = SupervisorFlags(
        strategy = SupervisorStrategy.OneForOne,
        intensity = 200,
        period = 30.seconds,
    )
    val ref = Supervisor.startLink(scope, flags, listOf(spec))
    val elapsed = measureNanoTime { settled.await() }
    ref.shutdown()
    BenchResult(
        name = "supervisor_restart_storm_recovery",
        iterations = starts.get(),
        p50Micros = elapsed / 1_000.0,
        p95Micros = elapsed / 1_000.0,
        throughputPerSec = starts.get() * 1_000_000_000.0 / elapsed,
        notes = "time to reach stable child after $crashCount forced crashes",
    )
}

// ---------------------------------------------------------------------------
// Scenario: supervisor_single_restart_latency
// 10 warmup crashes heat the JIT; then exactly 1 more crash is measured.
// Isolates per-cycle restart cost from the cold-JIT overhead in the storm.
// ---------------------------------------------------------------------------

private fun runSingleRestartLatency(scope: CoroutineScope): BenchResult = runBlocking {
    val starts = java.util.concurrent.atomic.AtomicInteger(0)
    val warmupDone = CompletableDeferred<Unit>()
    val settled = CompletableDeferred<Unit>()
    val WARMUP = 10
    val spec = ChildSpec(
        id = "single-restart",
        restart = Restart.Permanent,
        shutdown = Shutdown.BrutalKill,
    ) {
        val n = starts.incrementAndGet()
        when {
            n <= WARMUP   -> error("warmup crash #$n")
            n == WARMUP + 1 -> { warmupDone.complete(Unit); error("measured crash") }
            else          -> { settled.complete(Unit); delay(Long.MAX_VALUE) }
        }
    }
    val flags = SupervisorFlags(
        strategy = SupervisorStrategy.OneForOne,
        intensity = 200,
        period = 30.seconds,
    )
    val ref = Supervisor.startLink(scope, flags, listOf(spec))
    warmupDone.await()
    val elapsed = measureNanoTime { settled.await() }
    ref.shutdown()
    BenchResult(
        name = "supervisor_single_restart_latency",
        iterations = 1,
        p50Micros = elapsed / 1_000.0,
        p95Micros = elapsed / 1_000.0,
        throughputPerSec = 1e9 / elapsed,
        notes = "one restart after $WARMUP JIT warmup cycles; isolates per-cycle cost from storm overhead",
    )
}

// ---------------------------------------------------------------------------
// Scenario: distribution_in_memory_call (existing)
// ---------------------------------------------------------------------------

private fun runDistributionOverhead(scope: CoroutineScope, iterations: Int): BenchResult = runBlocking {
    val transport = InMemoryTransport()
    val nodeA = LocalNode(NodeId("node-a"))
    val nodeB = LocalNode(NodeId("node-b"))
    transport.connect(nodeA, nodeB)
    transport.addNode(nodeA)
    transport.addNode(nodeB)
    val ref = GenServers.startLink(scope, EchoServer(), name = "dist-echo")
    nodeB.register("echo", ref)
    val remote = RemoteNodeStub(nodeB.id, transport)

    repeat(2_000) { remote.call<Any>("echo", "warmup") }
    val latencies = ArrayList<Long>(iterations)
    val totalNanos = measureNanoTime {
        repeat(iterations) {
            val dt = measureNanoTime { remote.call<Any>("echo", "ping") }
            latencies.add(dt)
        }
    }
    ref.stop()
    BenchResult(
        name = "distribution_in_memory_call",
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
    )
}

// ---------------------------------------------------------------------------
// Scenario: mailbox_cast_memory_delta (existing)
// ---------------------------------------------------------------------------

private fun usedHeapBytes(): Long {
    val rt = Runtime.getRuntime()
    return rt.totalMemory() - rt.freeMemory()
}

private fun runMemoryPressure(scope: CoroutineScope, casts: Int): BenchResult = runBlocking {
    val ref = GenServers.startLink(scope, EchoServer(), name = "memory-bench")
    System.gc()
    Thread.sleep(100)
    val before = usedHeapBytes()
    repeat(casts) { ref.cast("inc") }
    while (ref.call<Int>("count") < casts) delay(1)
    System.gc()
    Thread.sleep(100)
    val after = usedHeapBytes()
    ref.stop()
    val delta = (after - before).coerceAtLeast(0)
    BenchResult(
        name = "mailbox_cast_memory_delta",
        iterations = casts,
        p50Micros = 0.0,
        p95Micros = 0.0,
        throughputPerSec = 0.0,
        notes = "heap delta after $casts casts and drain: ${delta / 1024} KiB",
    )
}

// ---------------------------------------------------------------------------
// Scenario A: gen_server_call_concurrent_callers_N
// N coroutines concurrently calling a single EchoServer; measures how serial
// mailbox processing degrades per-call latency as contention grows.
// ---------------------------------------------------------------------------

private fun runConcurrentCallersBenchmark(
    scope: CoroutineScope,
    callers: Int,
    callsPerCaller: Int,
): BenchResult = runBlocking {
    val ref = GenServers.startLink(scope, EchoServer(), name = "concurrent-callers-$callers")
    // warmup with sequential calls
    repeat(500) { ref.call<Any>("warmup") }

    val allLatencies = ConcurrentLinkedQueue<Long>()
    val totalNanos = measureNanoTime {
        coroutineScope {
            repeat(callers) {
                async {
                    repeat(callsPerCaller) {
                        val dt = measureNanoTime { ref.call<Any>("ping") }
                        allLatencies.add(dt)
                    }
                }
            }
        }
    }
    ref.stop()
    val latencies = allLatencies.toList()
    val totalCalls = latencies.size
    BenchResult(
        name = "gen_server_call_concurrent_callers_$callers",
        iterations = totalCalls,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = totalCalls * 1_000_000_000.0 / totalNanos,
        notes = "$callers concurrent callers x $callsPerCaller calls each; latency grows with mailbox depth",
    )
}

// ---------------------------------------------------------------------------
// Scenario G: gen_server_router_concurrent_callers_N_shards_M
// N concurrent callers routing round-robin across M shards.
// Validates that sharding reduces per-call latency at high concurrency.
// ---------------------------------------------------------------------------

private fun runRouterConcurrentCallersBenchmark(
    scope: CoroutineScope,
    callers: Int,
    callsPerCaller: Int,
    shards: Int,
): BenchResult = runBlocking {
    val router = GenServerRouters.startLink(scope, shards, factory = { EchoServer() })
    repeat(2_000) { router.call<Any>("warmup") }

    val latencies = ConcurrentLinkedQueue<Long>()
    val totalNanos = measureNanoTime {
        coroutineScope {
            repeat(callers) {
                async {
                    repeat(callsPerCaller) {
                        val dt = measureNanoTime { router.call<Any>("ping") }
                        latencies.add(dt)
                    }
                }
            }
        }
    }
    router.stop()
    val latencyList = latencies.toList()
    BenchResult(
        name = "gen_server_router_concurrent_callers_${callers}_shards_$shards",
        iterations = latencyList.size,
        p50Micros = percentile(latencyList, 50.0),
        p95Micros = percentile(latencyList, 95.0),
        p99Micros = percentile(latencyList, 99.0),
        p999Micros = percentile(latencyList, 99.9),
        throughputPerSec = latencyList.size * 1_000_000_000.0 / totalNanos,
        notes = "$callers callers round-robin across $shards shards; compare to gen_server_call_concurrent_callers_$callers",
    )
}

// ---------------------------------------------------------------------------
// Scenario C: gen_server_call_bounded_mailbox_overflow
// 500 concurrent senders burst into a mailbox capped at `capacity`. Reports
// accepted vs rejected counts and latency of accepted calls.
// ---------------------------------------------------------------------------

private fun runBoundedMailboxOverflow(
    scope: CoroutineScope,
    capacity: Int,
    senders: Int,
): BenchResult = runBlocking {
    val bound = MailboxBound(capacity = capacity, policy = OverflowPolicy.CrashSender)
    val ref = GenServers.startLink(scope, EchoServer(), name = "bounded-overflow", mailboxBound = bound)
    repeat(10) { ref.call<Any>("warmup") }

    // Suspend the actor (OTP sys:suspend idiom) so the mailbox fills before any drain.
    // First `capacity` senders queue successfully; the rest get MailboxFullException
    // immediately from trySend. sysResume fires after all senders are launched so the
    // actor then drains and replies to the accepted callers.
    ref.sysSuspend()

    val accepted = ConcurrentLinkedQueue<Long>()
    val rejectedCount = java.util.concurrent.atomic.AtomicInteger(0)
    val totalNanos = measureNanoTime {
        coroutineScope {
            repeat(senders) {
                async {
                    try {
                        val dt = measureNanoTime { ref.call<Any>("burst") }
                        accepted.add(dt)
                    } catch (_: MailboxFullException) {
                        rejectedCount.incrementAndGet()
                    }
                }
            }
            // Brief yield so all async blocks reach trySend before the actor resumes
            kotlinx.coroutines.delay(5)
            ref.sysResume()
        }
    }
    ref.stop()
    val acceptedList = accepted.toList()
    val p95 = if (acceptedList.isNotEmpty()) percentile(acceptedList, 95.0) else 0.0
    val p99 = if (acceptedList.isNotEmpty()) percentile(acceptedList, 99.0) else 0.0
    BenchResult(
        name = "gen_server_call_bounded_mailbox_overflow",
        iterations = senders,
        p50Micros = if (acceptedList.isNotEmpty()) percentile(acceptedList, 50.0) else 0.0,
        p95Micros = p95,
        p99Micros = p99,
        p999Micros = 0.0,
        throughputPerSec = if (totalNanos > 0) acceptedList.size * 1_000_000_000.0 / totalNanos else 0.0,
        notes = "capacity=$capacity senders=$senders accepted=${acceptedList.size} rejected=${rejectedCount.get()} sysSuspend barrier",
    )
}

// ---------------------------------------------------------------------------
// Scenario D: selective_receive_ref_mark_comparison
// Compares int-equality (existing baseline) vs reference-equality (===) on a
// sealed type — analogous to OTP's {Ref, Reply} ref-mark optimization pattern.
// Reference equality is O(1) per comparison; the scan loop cost is the same.
// This isolates whether predicate evaluation cost is significant.
// ---------------------------------------------------------------------------

private sealed class TaggedMsg {
    data class Tagged(val tag: Any, val payload: Int) : TaggedMsg()
    data class Noise(val payload: Int) : TaggedMsg()
}

private fun runSelectiveReceiveRefMark(depth: Int, samples: Int): BenchResult = runBlocking {
    val ch = Channel<TaggedMsg>(Channel.UNLIMITED)
    val box = SelectiveMailbox(ch)
    val tag = Any()

    repeat(depth) { ch.send(TaggedMsg.Noise(0)) }
    ch.send(TaggedMsg.Tagged(tag, 1))

    val latencies = ArrayList<Long>(samples)
    repeat(samples) {
        repeat(depth) { ch.send(TaggedMsg.Noise(0)) }
        ch.send(TaggedMsg.Tagged(tag, 1))
        val dt = measureNanoTime {
            box.receive { it is TaggedMsg.Tagged && it.tag === tag }
            box.flushSaved()
            repeat(depth + 1) { box.receive { true } }
        }
        latencies.add(dt)
    }
    val total = latencies.sum().toDouble()
    BenchResult(
        name = "selective_receive_ref_mark_depth_$depth",
        iterations = latencies.size,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = latencies.size * 1_000_000_000.0 / total,
        notes = "ref-equality (===) predicate; compare to selective_receive_depth_$depth for predicate cost isolation",
    )
}

// ---------------------------------------------------------------------------
// Scenario F: selective_receive_mark_depth_N
// Demonstrates O(1) receiveFrom(mark) when saved has `depth` pre-mark messages.
// Setup: accumulate `depth` noise messages in saved before each sample so the
// pre-mark queue is deep; the matching message arrives in the channel after the
// mark, so receiveFrom scans 0 saved entries and 1 channel entry regardless of
// depth.
// ---------------------------------------------------------------------------

private fun runSelectiveReceiveMark(depth: Int, samples: Int): BenchResult = runBlocking {
    val ch = Channel<TaggedMsg>(Channel.UNLIMITED)
    val box = SelectiveMailbox(ch)
    val tag = Any()

    // Accumulate `depth` noise messages in the saved list once before the loop.
    // These simulate the pre-existing queue that receiveFrom must skip.
    repeat(depth) { ch.send(TaggedMsg.Noise(0)) }
    ch.send(TaggedMsg.Tagged(tag, 1))
    box.receive { it is TaggedMsg.Tagged }  // drains channel; saves `depth` noises

    val latencies = ArrayList<Long>(samples)
    repeat(samples) {
        // Mark: all `depth` noises are pre-mark and will be skipped.
        val m = box.mark()
        // Only the new matching message arrives after the mark.
        ch.send(TaggedMsg.Tagged(tag, 1))
        val dt = measureNanoTime {
            box.receiveFrom(m) { it is TaggedMsg.Tagged && it.tag === tag }
        }
        latencies.add(dt)
    }

    // Cleanup saved list so the next benchmark starts clean.
    box.flushSaved()
    repeat(depth) { box.receive { true } }

    val total = latencies.sum().toDouble()
    BenchResult(
        name = "selective_receive_mark_depth_$depth",
        iterations = latencies.size,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = latencies.size * 1_000_000_000.0 / total,
        notes = "mark set with $depth pre-mark noises in saved; match arrives in channel; O(1) scan",
    )
}

// ---------------------------------------------------------------------------
// Scenario E: worker_pool_checkout_under_load
// Inline minimal worker pool built on DynamicSupervisor. `poolSize` workers
// are pre-warmed; `callers` coroutines concurrently check out a worker,
// hold it for `holdMs`, then check in. Measures checkout latency only.
// ---------------------------------------------------------------------------

/**
 * Minimal inline worker pool backed by DynamicSupervisor + a channel-based
 * semaphore. Avoids a cross-sample dependency on samples:poolboy while still
 * exercising the DynamicSupervisor / structured-concurrency checkout pattern.
 *
 * Checkout: take a permit from the semaphore channel (blocks under full load).
 * Checkin:  return the permit.
 * Benchmark times only the checkout (permit acquisition) step.
 */
private class InlineWorkerPool(
    scope: CoroutineScope,
    poolSize: Int,
) {
    private val supervisor: DynamicSupervisorRef
    private val permits = Channel<Unit>(poolSize)

    init {
        val template = SimpleOneForOneTemplate<Unit>(
            restart = Restart.Permanent,
            shutdown = Shutdown.BrutalKill,
            start = { _, _, ready ->
                ready(Unit)
                delay(Long.MAX_VALUE)
            },
        )
        val flags = SupervisorFlags(
            strategy = SupervisorStrategy.OneForOne,
            intensity = 10,
            period = 5.seconds,
        )
        supervisor = DynamicSupervisor.startLink(scope, flags, template)
    }

    suspend fun warmUp(count: Int) {
        repeat(count) {
            supervisor.startChildSync<Unit>()
            permits.send(Unit)
        }
    }

    suspend fun checkout(): Unit = permits.receive()

    fun checkin() { permits.trySend(Unit) }

    suspend fun shutdown() = supervisor.shutdown()
}

private fun runWorkerPoolCheckout(
    scope: CoroutineScope,
    poolSize: Int,
    callers: Int,
    holdMs: Long,
): BenchResult = runBlocking {
    val pool = InlineWorkerPool(scope, poolSize)
    pool.warmUp(poolSize)

    val checkoutLatencies = ConcurrentLinkedQueue<Long>()
    val totalNanos = measureNanoTime {
        coroutineScope {
            repeat(callers) {
                async {
                    // each caller does 10 checkout-hold-checkin cycles
                    repeat(10) {
                        val dt = measureNanoTime { pool.checkout() }
                        checkoutLatencies.add(dt)
                        if (holdMs > 0) delay(holdMs)
                        pool.checkin()
                    }
                }
            }
        }
    }
    pool.shutdown()

    val latencies = checkoutLatencies.toList()
    val totalOps = latencies.size
    BenchResult(
        name = "worker_pool_checkout_callers_${callers}_pool_$poolSize",
        iterations = totalOps,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = totalOps * 1_000_000_000.0 / totalNanos,
        notes = "pool=$poolSize callers=$callers holdMs=$holdMs; industry target p95<1000us",
    )
}

// ---------------------------------------------------------------------------
// Scenario J: gen_server_cached_read_callers_N
// CachedReadRef non-suspending AtomicReference read vs gen_server:call for
// read-heavy workloads. Actor writes via cast("inc"); callers read the cache.
// ---------------------------------------------------------------------------

private data class CounterState(val n: Int) : CacheableState<CounterState> {
    override fun snapshot() = copy()
}

private class CounterServer : GenServer<CounterState> {
    override suspend fun init(self: GenServerRef<CounterState>) = InitResult.Ok(CounterState(0))
    override suspend fun handleCall(request: Any, state: CounterState): ReplyResult<CounterState> =
        ReplyResult.Reply(state.n, state)
    override suspend fun handleCast(request: Any, state: CounterState): NoreplyResult<CounterState> =
        NoreplyResult.Noreply(CounterState(state.n + 1))
}

private fun runCachedReadBenchmark(scope: CoroutineScope, callers: Int, iterations: Int): BenchResult = runBlocking {
    val cacheRef = CachedReadRef<CounterState>()
    val ref = GenServers.startLink(scope, CachingGenServer(CounterServer(), cacheRef), name = "cached-read-bench")
    // Prime the cache with at least one state publication
    repeat(100) { ref.cast("inc") }
    delay(10)
    val latencies = ConcurrentLinkedQueue<Long>()
    val totalNanos = measureNanoTime {
        coroutineScope {
            repeat(callers) {
                async {
                    val perCaller = iterations / callers
                    repeat(perCaller) {
                        val dt = measureNanoTime { cacheRef.readCached() }
                        latencies.add(dt)
                    }
                }
            }
        }
    }
    ref.stop()
    val latencyList = latencies.toList()
    BenchResult(
        name = "gen_server_cached_read_callers_$callers",
        iterations = latencyList.size,
        p50Micros = percentile(latencyList, 50.0),
        p95Micros = percentile(latencyList, 95.0),
        p99Micros = percentile(latencyList, 99.0),
        p999Micros = percentile(latencyList, 99.9),
        throughputPerSec = latencyList.size * 1_000_000_000.0 / totalNanos,
        notes = "$callers readers; non-suspending AtomicReference read; compare to gen_server_call_concurrent_callers_$callers",
    )
}

// ---------------------------------------------------------------------------
// Scenario: loom_benefit_concurrent_blocking_actors
// N actors each blocking for blockingMs (simulating JDBC/HTTP).
// IO (constrained pool): ceil(N/poolSize) × blockingMs wall time.
// Loom (per-task VT): ~blockingMs wall time regardless of N.
// Quantifies the scalability win that justifies Loom for blocking workers.
// ---------------------------------------------------------------------------

private fun runLoomBenefitBenchmark(scope: CoroutineScope, actorCount: Int): BenchResult = runBlocking {
    val blockingMs = 50L
    val poolSize = 8
    val constrainedIO = Dispatchers.IO.limitedParallelism(poolSize)

    // Measure wall time for IO: actors batch through the limited pool
    val ioWallNs = measureNanoTime {
        coroutineScope {
            (0 until actorCount).map { i ->
                async {
                    val ref = GenServers.startLink(this, BlockingEchoServer(blockingMs), context = constrainedIO, name = "bio-$i")
                    ref.call<Unit>("block")
                    ref.stop()
                }
            }.forEach { it.await() }
        }
    }

    // Measure wall time for Loom: all virtual threads block concurrently
    val loomDispatcher = OtpDispatchers.loom() as kotlinx.coroutines.ExecutorCoroutineDispatcher
    val loomWallNs = try {
        measureNanoTime {
            coroutineScope {
                (0 until actorCount).map { i ->
                    async {
                        val ref = GenServers.startLink(this, BlockingEchoServer(blockingMs), context = loomDispatcher, name = "bloom-$i")
                        ref.call<Unit>("block")
                        ref.stop()
                    }
                }.forEach { it.await() }
            }
        }
    } finally {
        loomDispatcher.close()
    }

    val speedup = ioWallNs.toDouble() / loomWallNs
    val ioMs = ioWallNs / 1_000_000.0
    val loomMs = loomWallNs / 1_000_000.0
    BenchResult(
        name = "loom_benefit_concurrent_blocking_actors_$actorCount",
        iterations = actorCount,
        p50Micros = loomMs * 1_000,   // report Loom wall as p50 (µs)
        p95Micros = ioMs * 1_000,     // report IO wall as p95 for visual comparison
        throughputPerSec = actorCount * 1_000_000_000.0 / loomWallNs,
        notes = "${actorCount} actors × ${blockingMs}ms blocking; IO pool=$poolSize threads; " +
            "IO wall=${ioMs.toInt()}ms Loom wall=${loomMs.toInt()}ms speedup=${"%.1f".format(speedup)}×",
    )
}

private class BlockingEchoServer(private val blockingMs: Long) : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> {
        Thread.sleep(blockingMs)  // blocks platform thread (IO) or unmounts VT (Loom)
        return ReplyResult.Reply(Unit, Unit)
    }
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

// ---------------------------------------------------------------------------
// Scenario L: coroutine_long_task_boundary_blocking_walltime
// Coroutine-only long-task handling using DeferReply + shared LongTaskBoundary.
// This keeps actor loops responsive while bounding long-task concurrency.
// ---------------------------------------------------------------------------

private sealed interface LongTaskBenchRequest {
    data object Block : LongTaskBenchRequest
    data object ControlPing : LongTaskBenchRequest
}

private class CoroutineLongTaskServer(
    private val boundary: LongTaskBoundary,
    private val workerScope: CoroutineScope,
    private val blockingMs: Long,
) : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>): InitResult<Unit> = InitResult.Ok(Unit)

    override suspend fun handleCallFrom(
        request: Any,
        state: Unit,
        from: org.otpstudy.genserver.ReplyHandle<Unit>,
    ): ReplyResult<Unit> {
        return when (request) {
            LongTaskBenchRequest.Block -> {
                // Keep actor loop free: execute long work in boundary worker scope.
                workerScope.launch {
                    val response: Any =
                        try {
                            boundary.run {
                                Thread.sleep(blockingMs)
                                "ok"
                            }
                        } catch (e: LongTaskRejectedException) {
                            "rejected:${e.message}"
                        } catch (t: Throwable) {
                            "error:${t::class.simpleName}"
                        }
                    from.reply(response)
                }
                ReplyResult.DeferReply(from, state)
            }
            LongTaskBenchRequest.ControlPing -> ReplyResult.Reply("pong", state)
            else -> ReplyResult.Reply("unknown", state)
        }
    }

    override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> = ReplyResult.Reply(Unit, Unit)
    override suspend fun handleCast(request: Any, state: Unit): NoreplyResult<Unit> = NoreplyResult.Noreply(Unit)
}

private fun runBlockingWalltimeIoBaseline(scope: CoroutineScope, actorCount: Int, blockingMs: Long, poolSize: Int): BenchResult =
    runBlocking {
        val constrainedIO = Dispatchers.IO.limitedParallelism(poolSize)
        val refs = (0 until actorCount).map { i ->
            GenServers.startLink(scope, BlockingEchoServer(blockingMs), context = constrainedIO, name = "blocking-io-$i")
        }
        val totalNanos = measureNanoTime {
            coroutineScope {
                refs.map { ref -> async { ref.call<Unit>("block") } }.forEach { it.await() }
            }
        }
        refs.forEach { it.stop() }
        BenchResult(
            name = "blocking_walltime_actors_${actorCount}_task_${blockingMs}ms_io_baseline",
            iterations = actorCount,
            p50Micros = totalNanos / 1_000.0,
            p95Micros = totalNanos / 1_000.0,
            throughputPerSec = actorCount * 1_000_000_000.0 / totalNanos,
            notes = "wall=${(totalNanos / 1_000_000.0).pretty()}ms; IO limitedParallelism=$poolSize",
        )
    }

private fun runBlockingWalltimeCoroutineBoundary(
    scope: CoroutineScope,
    actorCount: Int,
    blockingMs: Long,
    poolSize: Int,
    policy: LongTaskBackpressurePolicy,
): BenchResult = runBlocking {
    val workerDispatcher = Dispatchers.IO.limitedParallelism(poolSize)
    val workerScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val boundary = LongTaskBoundary(
        workerContext = workerDispatcher,
        maxConcurrent = poolSize,
        policy = policy,
        queueTimeout = 250.milliseconds,
        taskTimeout = 5.seconds,
    )
    val refs = (0 until actorCount).map { i ->
        GenServers.startLink(
            scope,
            CoroutineLongTaskServer(boundary, workerScope, blockingMs),
            context = Dispatchers.Default,
            name = "blocking-boundary-$i",
        )
    }
    val controlLatencies = ConcurrentLinkedQueue<Long>()
    lateinit var results: List<Any>
    val totalNanos = measureNanoTime {
        coroutineScope {
            val probe = launch {
                repeat(50) { idx ->
                    val dt = measureNanoTime { refs[idx % refs.size].call<Any>(LongTaskBenchRequest.ControlPing) }
                    controlLatencies.add(dt)
                    delay(2)
                }
            }
            results = refs.map { ref -> async { ref.call<Any>(LongTaskBenchRequest.Block) } }.map { it.await() }
            probe.cancel()
        }
    }
    val rejected = results.count { it is String && it.startsWith("rejected:") }
    val snap = boundary.snapshot()
    val controlList = controlLatencies.toList()
    refs.forEach { it.stop() }
    workerScope.cancel()
    BenchResult(
        name = "blocking_walltime_actors_${actorCount}_task_${blockingMs}ms_coroutines_${policy.name.lowercase()}",
        iterations = actorCount,
        p50Micros = totalNanos / 1_000.0,
        p95Micros = if (controlList.isEmpty()) 0.0 else percentile(controlList, 95.0),
        p99Micros = if (controlList.isEmpty()) 0.0 else percentile(controlList, 99.0),
        throughputPerSec = actorCount * 1_000_000_000.0 / totalNanos,
        notes = "wall=${(totalNanos / 1_000_000.0).pretty()}ms policy=${policy.name} pool=$poolSize rejected=$rejected queuedNow=${snap.queued} maxQueued=${snap.maxQueuedObserved} maxInFlight=${snap.maxInFlightObserved} totalRejected=${snap.rejected}",
    )
}

// ---------------------------------------------------------------------------
// Scenario: distribution_tcp_loopback_call (Dispatchers.IO)
// TCP loopback roundtrip using actual kernel networking (same JVM, loopback NIC).
// Two KotlinNodeTransport instances connected via localhost TCP.
// Measures: kernel TCP stack cost + distribution protocol overhead.
// Compare against distribution_in_memory_call (~12 µs) for transport overhead.
// Reference: Akka Artery p50 ~155 µs; OTP TCP distribution ~190 µs.
// ---------------------------------------------------------------------------

enum class TcpDispatcherMode { IO, LOOM_PER_TASK, LOOM_POOL }

private fun runTcpDistributionCall(
    scope: CoroutineScope,
    iterations: Int,
    mode: TcpDispatcherMode = TcpDispatcherMode.IO,
): BenchResult = runBlocking {
    val serverNode = NodeId("tcp-bench-server", "127.0.0.1")
    val clientNode = NodeId("tcp-bench-client", "127.0.0.1")
    val regServer = ProcessRegistry()
    val regClient = ProcessRegistry()

    val loomDispatcher: kotlinx.coroutines.ExecutorCoroutineDispatcher? = when (mode) {
        TcpDispatcherMode.LOOM_PER_TASK -> OtpDispatchers.loom() as kotlinx.coroutines.ExecutorCoroutineDispatcher
        TcpDispatcherMode.LOOM_POOL -> {
            // loomPool removed: newCachedThreadPool(virtualFactory) measured at 71 µs (slower than IO 55 µs).
            // Fall back to per-task for the LOOM_POOL enum case.
            OtpDispatchers.loom() as kotlinx.coroutines.ExecutorCoroutineDispatcher
        }
        TcpDispatcherMode.IO -> null
    }
    val ioCtx: kotlin.coroutines.CoroutineContext = loomDispatcher ?: Dispatchers.IO
    val useLoom = mode != TcpDispatcherMode.IO

    val server = KotlinNodeTransport(serverNode, port = 0, ioDispatcher = ioCtx)
    val client = KotlinNodeTransport(clientNode, port = 0, ioDispatcher = ioCtx)

    try {
        server.startAccepting(scope, regServer)
        val echoRef = GenServers.startLink(scope, EchoServer(), name = "tcp-bench-echo")
        regServer.register("tcp-bench-echo", echoRef)
        client.startAccepting(scope, regClient)
        client.connectOut(scope, regClient, serverNode, "127.0.0.1", server.boundPort)

        // Warmup: establish JIT-compiled hot path through the TCP stack
        repeat(200) { client.call(serverNode, "tcp-bench-echo", "warmup", 10.seconds) }

        val latencies = ArrayList<Long>(iterations)
        val totalNanos = measureNanoTime {
            repeat(iterations) {
                val dt = measureNanoTime { client.call(serverNode, "tcp-bench-echo", "ping", 10.seconds) }
                latencies.add(dt)
            }
        }
        val label = when (mode) {
            TcpDispatcherMode.IO -> "distribution_tcp_loopback_call"
            TcpDispatcherMode.LOOM_PER_TASK -> "distribution_tcp_loopback_call_loom"
            TcpDispatcherMode.LOOM_POOL -> "distribution_tcp_loopback_call_loom_pool"
        }
        BenchResult(
            name = label,
            iterations = iterations,
            p50Micros = percentile(latencies, 50.0),
            p95Micros = percentile(latencies, 95.0),
            p99Micros = percentile(latencies, 99.0),
            p999Micros = percentile(latencies, 99.9),
            throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
            notes = when (mode) {
                TcpDispatcherMode.IO -> "TCP loopback; Dispatchers.IO; compare to distribution_in_memory_call"
                TcpDispatcherMode.LOOM_PER_TASK -> "TCP loopback; Loom per-task (newVirtualThreadPerTaskExecutor); one VT per dispatch"
                TcpDispatcherMode.LOOM_POOL -> "TCP loopback; Loom pooled (newCachedThreadPool+virtualFactory); reuses VTs"
            },
        )
    } finally {
        runCatching { client.close() }
        runCatching { server.close() }
        loomDispatcher?.close()
    }
}

// ---------------------------------------------------------------------------
// Experiment 3a: gen_server_call_roundtrip_same_dispatcher
// Caller runs on Dispatchers.Default (same pool as actor). Eliminates the
// cross-dispatcher hop that runBlocking's event-loop thread introduces.
// Shows the true framework overhead without inter-dispatcher penalty.
// ---------------------------------------------------------------------------

private fun runCallBenchmarkSameDispatcher(scope: CoroutineScope, iterations: Int): BenchResult = runBlocking {
    val ref = GenServers.startLink(scope, EchoServer(), name = "same-disp-bench", fastReply = true)
    withContext(Dispatchers.Default) { repeat(2_000) { ref.call<Any>("warmup") } }
    val latencies = ArrayList<Long>(iterations)
    val totalNanos = withContext(Dispatchers.Default) {
        measureNanoTime {
            repeat(iterations) {
                val dt = measureNanoTime { ref.call<Any>("ping") }
                latencies.add(dt)
            }
        }
    }
    ref.stop()
    BenchResult(
        name = "gen_server_call_roundtrip_same_dispatcher",
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
        notes = "fastReply; caller on Dispatchers.Default (same pool as actor); eliminates cross-dispatcher hops",
    )
}

// ---------------------------------------------------------------------------
// Experiment 3b: gen_server_call_roundtrip_unconfined
// Actor uses Dispatchers.Unconfined — resumes inline on the calling thread,
// bypassing all scheduler dispatch. Represents the theoretical JVM floor for
// a fully-structured actor framework. Not safe for general production use:
// thread affinity, fairness, and recursion guarantees are not preserved.
// Compare against gen_server_call_fastReply_roundtrip as the OTP-parity target.
// ---------------------------------------------------------------------------

private fun runCallBenchmarkUnconfined(scope: CoroutineScope, iterations: Int): BenchResult = runBlocking {
    val ref = GenServers.startLink(
        scope, EchoServer(),
        context = Dispatchers.Unconfined,
        name = "unconfined-bench",
        fastReply = true,
    )
    repeat(2_000) { ref.call<Any>("warmup") }
    val latencies = ArrayList<Long>(iterations)
    val totalNanos = measureNanoTime {
        repeat(iterations) {
            val dt = measureNanoTime { ref.call<Any>("ping") }
            latencies.add(dt)
        }
    }
    ref.stop()
    BenchResult(
        name = "gen_server_call_roundtrip_unconfined",
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
        notes = "fastReply; Dispatchers.Unconfined — actor resumes inline on caller thread; experimental only",
    )
}

// ---------------------------------------------------------------------------
// Experiment 3c: gen_server_call_roundtrip_loom
// Actor uses a Loom (virtual-thread) dispatcher instead of Dispatchers.Default.
// Measures overhead of the Loom scheduler for pure message-passing — expected
// to be similar to Default since there is no blocking work. Establishes the
// baseline before adding blocking actors to the comparison.
// ---------------------------------------------------------------------------

@Suppress("OPT_IN_USAGE")
private fun runCallBenchmarkLoom(scope: CoroutineScope, iterations: Int): BenchResult = runBlocking {
    val loomDispatcher = OtpDispatchers.loom() as kotlinx.coroutines.ExecutorCoroutineDispatcher
    val loomScope = CoroutineScope(loomDispatcher + SupervisorJob())
    val ref = GenServers.startLink(loomScope, EchoServer(), fastReply = true)
    repeat(2_000) { ref.call<Any>("warmup") }
    val latencies = ArrayList<Long>(iterations)
    val totalNanos = measureNanoTime {
        repeat(iterations) {
            val dt = measureNanoTime { ref.call<Any>("ping") }
            latencies.add(dt)
        }
    }
    ref.stop()
    loomScope.cancel()
    loomDispatcher.close()
    BenchResult(
        name = "gen_server_call_roundtrip_loom",
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
        notes = "fastReply; Loom virtual-thread dispatcher; baseline for blocking-actor comparison",
    )
}

// ---------------------------------------------------------------------------
// Memory helpers
// ---------------------------------------------------------------------------

/** Trigger GC and return used heap bytes. */
private fun stableHeap(): Long {
    repeat(3) { System.gc(); Thread.sleep(60) }
    return Runtime.getRuntime().let { it.totalMemory() - it.freeMemory() }
}

// ---------------------------------------------------------------------------
// Scenario: actor_memory_footprint
// N idle actors created, stable-GC heap measured before and after.
// Reports bytes per actor — the per-actor memory cost of the framework.
// Reference: OTP gen_server idle process ~2–8 KB (erlang:process_info/2);
//            Pekko typed actor idle ~1–2 KB (published Lightbend figures).
// ---------------------------------------------------------------------------

private fun runActorMemoryFootprint(
    scope: CoroutineScope,
    actorCount: Int,
    useLoom: Boolean = false,
): BenchResult = runBlocking {
    val workerCtx: kotlin.coroutines.CoroutineContext =
        if (useLoom) OtpDispatchers.IO else Dispatchers.Default

    val heapBefore = stableHeap()

    val refs = (0 until actorCount).map { i ->
        GenServers.startLink(
            scope, EchoServer(),
            context = workerCtx,
            name = "mem-${if (useLoom) "loom" else "dflt"}-$i",
        )
    }

    val heapAfter = stableHeap()
    val totalBytes = (heapAfter - heapBefore).coerceAtLeast(0L)
    val bytesPerActor = if (actorCount > 0) totalBytes / actorCount else 0L

    refs.forEach { runCatching { it.stop() } }

    val label = if (useLoom) "actor_loom_memory_footprint_$actorCount"
                else "actor_default_memory_footprint_$actorCount"
    BenchResult(
        name = label,
        iterations = actorCount,
        p50Micros = bytesPerActor.toDouble(),          // bytes per actor (field repurposed)
        p95Micros = totalBytes.toDouble() / 1024.0,    // total KB
        p99Micros = 0.0,
        p999Micros = 0.0,
        throughputPerSec = actorCount.toDouble(),
        notes = "$actorCount idle actors; ${bytesPerActor}B/actor " +
            "(${String.format("%.1f", bytesPerActor / 1024.0)}KB); " +
            "total ${totalBytes / 1024}KB; GC-heuristic (System.gc advisory)",
    )
}

// ---------------------------------------------------------------------------
// Profile configuration
// ---------------------------------------------------------------------------

private fun parseProfile(args: Array<String>): BenchProfile {
    val profileArg = args.firstOrNull { it.startsWith("--profile=") }?.substringAfter("=") ?: "quick"
    val roundsArg = args.firstOrNull { it.startsWith("--rounds=") }?.substringAfter("=")?.toIntOrNull()
    val base = when (profileArg.lowercase()) {
        "long" -> BenchProfile(
            name = "long",
            iterations = 50_000,
            tailIterations = 100_000,
            selectiveDepths = listOf(1_000, 10_000, 25_000),
            selectiveSamples = 250,
            restartCrashes = 100,
            memoryCasts = 250_000,
            concurrentCallerCounts = listOf(1, 10, 50, 100),
            callsPerCaller = 2_000,
            poolSize = 20,
            poolCallerCounts = listOf(20, 50, 100),
            poolHoldMs = 1L,
            boundedMailboxCapacity = 100,
            boundedMailboxSenders = 500,
            rounds = 5,
            tcpIterations = 10_000,
            memoryActorCounts = listOf(100, 1_000, 5_000),
        )
        else -> BenchProfile(
            name = "quick",
            iterations = 20_000,
            tailIterations = 50_000,
            selectiveDepths = listOf(1_000, 10_000),
            selectiveSamples = 100,
            restartCrashes = 40,
            memoryCasts = 100_000,
            concurrentCallerCounts = listOf(1, 10, 50),
            callsPerCaller = 500,
            poolSize = 10,
            poolCallerCounts = listOf(10, 30),
            poolHoldMs = 1L,
            boundedMailboxCapacity = 100,
            boundedMailboxSenders = 300,
            rounds = 1,
            tcpIterations = 3_000,
            memoryActorCounts = listOf(100, 1_000),
        )
    }
    return if (roundsArg != null && roundsArg > 0) base.copy(rounds = roundsArg) else base
}

// ---------------------------------------------------------------------------
// Round runner
// ---------------------------------------------------------------------------

private fun runRound(profile: BenchProfile, round: Int): List<BenchResult> {
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val results = buildList {
        // --- Original scenarios (extended with p99/p999) ---
        add(runCallBenchmark(scope, profile.iterations))
        add(runCallBenchmark(scope, profile.iterations, fastReply = true))
        // --- Experiments 3a/3b/3c: dispatcher topology experiments ---
        add(runCallBenchmarkSameDispatcher(scope, profile.iterations))
        add(runCallBenchmarkUnconfined(scope, profile.iterations))
        add(runCallBenchmarkLoom(scope, profile.iterations))
        add(runCastBenchmark(scope, profile.iterations))
        for (depth in profile.selectiveDepths) {
            add(runSelectiveReceive(depth = depth, samples = profile.selectiveSamples))
        }
        add(runSupervisorRestartStorm(scope, crashCount = profile.restartCrashes))
        add(runSingleRestartLatency(scope))
        add(runDistributionOverhead(scope, profile.iterations))
        // --- TCP loopback distribution (IO and Loom dispatchers) ---
        add(runTcpDistributionCall(scope, profile.tcpIterations, TcpDispatcherMode.IO))
        add(runTcpDistributionCall(scope, profile.tcpIterations, TcpDispatcherMode.LOOM_PER_TASK))
        // loom_pool measured: 71 µs (SLOWER than both IO 55 µs and loom-per-task 65 µs)
        // Root cause: newCachedThreadPool(virtualFactory) uses SynchronousQueue wakeup
        // (~17 µs) vs CoroutinesScheduler unpark (~200 ns for IO). Kept here for reference.
        add(runMemoryPressure(scope, casts = profile.memoryCasts))

        // --- Scenario B: tail latency ---
        add(runTailLatencyBenchmark(scope, profile.tailIterations))

        // --- Scenario A: concurrent callers ---
        for (callers in profile.concurrentCallerCounts) {
            add(runConcurrentCallersBenchmark(scope, callers, profile.callsPerCaller))
        }

        // --- Scenario G: router concurrent callers (sharding vs single-actor) ---
        val routerShards = minOf(profile.concurrentCallerCounts.last(), 10)
        for (callers in profile.concurrentCallerCounts) {
            add(runRouterConcurrentCallersBenchmark(scope, callers, profile.callsPerCaller, routerShards))
        }

        // --- Scenario C: bounded mailbox overflow ---
        add(runBoundedMailboxOverflow(scope, profile.boundedMailboxCapacity, profile.boundedMailboxSenders))

        // --- Scenario D: selective receive ref-mark comparison ---
        for (depth in profile.selectiveDepths) {
            add(runSelectiveReceiveRefMark(depth = depth, samples = profile.selectiveSamples))
        }

        // --- Scenario F: selective receive with mark (O(1) via receiveFrom) ---
        for (depth in profile.selectiveDepths) {
            add(runSelectiveReceiveMark(depth = depth, samples = profile.selectiveSamples))
        }

        // --- Scenario E: worker pool checkout under load ---
        for (callers in profile.poolCallerCounts) {
            add(runWorkerPoolCheckout(scope, profile.poolSize, callers, profile.poolHoldMs))
        }

        // --- Scenario M: actor idle memory footprint ---
        for (n in profile.memoryActorCounts) {
            add(runActorMemoryFootprint(scope, n, useLoom = false))
            add(runActorMemoryFootprint(scope, n, useLoom = true))
        }

        // --- Scenario J: cached read (non-suspending AtomicReference vs call) ---
        for (callers in profile.concurrentCallerCounts) {
            add(runCachedReadBenchmark(scope, callers, profile.iterations))
        }

        // --- Scenario K: Loom benefit — concurrent blocking actors ---
        for (actorCount in listOf(profile.poolSize * 2, profile.poolSize * 4)) {
            add(runLoomBenefitBenchmark(scope, actorCount))
        }

        // --- Scenario L: coroutine-only long-task boundary matrix ---
        val longTaskBlockingMs = 50L
        val longTaskPoolSize = 8
        for (actorCount in listOf(20, 40)) {
            add(runBlockingWalltimeIoBaseline(scope, actorCount, longTaskBlockingMs, longTaskPoolSize))
            add(
                runBlockingWalltimeCoroutineBoundary(
                    scope = scope,
                    actorCount = actorCount,
                    blockingMs = longTaskBlockingMs,
                    poolSize = longTaskPoolSize,
                    policy = LongTaskBackpressurePolicy.BoundedWait,
                ),
            )
            add(
                runBlockingWalltimeCoroutineBoundary(
                    scope = scope,
                    actorCount = actorCount,
                    blockingMs = longTaskBlockingMs,
                    poolSize = longTaskPoolSize,
                    policy = LongTaskBackpressurePolicy.FailFast,
                ),
            )
        }
    }
    val phase = if (round == 1) "cold" else "steady"
    println("round=$round phase=$phase complete")
    scope.cancel()
    return results.map { it.copy(phase = phase) }
}

// ---------------------------------------------------------------------------
// Aggregation + output
// ---------------------------------------------------------------------------

private fun aggregate(rounds: List<List<BenchResult>>): List<BenchAggregate> {
    val byScenario = linkedMapOf<Pair<String, String>, MutableList<BenchResult>>()
    for (round in rounds) {
        for (r in round) byScenario.getOrPut(r.name to r.phase) { mutableListOf() }.add(r)
    }
    return byScenario.entries.map { entry ->
        val scenario = entry.key.first
        val phase = entry.key.second
        val results = entry.value
        BenchAggregate(
            scenario = scenario,
            phase = phase,
            rounds = results.size,
            meanP50Micros = mean(results.map { it.p50Micros }),
            stddevP50Micros = stddev(results.map { it.p50Micros }),
            meanP95Micros = mean(results.map { it.p95Micros }),
            stddevP95Micros = stddev(results.map { it.p95Micros }),
            meanP99Micros = mean(results.map { it.p99Micros }),
            stddevP99Micros = stddev(results.map { it.p99Micros }),
            meanThroughput = mean(results.map { it.throughputPerSec }),
            stddevThroughput = stddev(results.map { it.throughputPerSec }),
            notes = results.last().notes,
        )
    }
}

fun main(args: Array<String>) {
    val profile = parseProfile(args)
    val roundResults = (1..profile.rounds).map { runRound(profile, it) }
    val perRoundRows = roundResults.flatMapIndexed { idx, rows -> rows.map { (idx + 1) to it } }
    println("profile=${profile.name},rounds=${profile.rounds},iterations=${profile.iterations}")
    println("round,phase,scenario,iterations,p50_us,p95_us,p99_us,p999_us,throughput_ops_sec,notes")
    for ((round, r) in perRoundRows) {
        println(
            "$round,${r.phase},${r.name},${r.iterations},${r.p50Micros.pretty()},${r.p95Micros.pretty()},${r.p99Micros.pretty()},${r.p999Micros.pretty()},${r.throughputPerSec.pretty()},${r.notes.replace(",", ";")}",
        )
    }
    val aggregates = aggregate(roundResults)
    println("summary_scenario,phase,rounds,mean_p50_us,stddev_p50_us,mean_p95_us,stddev_p95_us,mean_p99_us,stddev_p99_us,mean_throughput_ops_sec,stddev_throughput_ops_sec,notes")
    for (s in aggregates) {
        println(
            "${s.scenario},${s.phase},${s.rounds},${s.meanP50Micros.pretty()},${s.stddevP50Micros.pretty()},${s.meanP95Micros.pretty()},${s.stddevP95Micros.pretty()},${s.meanP99Micros.pretty()},${s.stddevP99Micros.pretty()},${s.meanThroughput.pretty()},${s.stddevThroughput.pretty()},${s.notes.replace(",", ";")}",
        )
    }

    val steadyAggregates = aggregates.filter { it.phase == "steady" }
    if (steadyAggregates.isEmpty()) {
        println("steady_summary_note,unavailable (run with --rounds>=2 to compare steady-state windows)")
        return
    }
    println("steady_summary_scenario,rounds,mean_p50_us,stddev_p50_us,mean_p95_us,stddev_p95_us,mean_p99_us,stddev_p99_us,mean_throughput_ops_sec,stddev_throughput_ops_sec,notes")
    for (s in steadyAggregates) {
        println(
            "${s.scenario},${s.rounds},${s.meanP50Micros.pretty()},${s.stddevP50Micros.pretty()},${s.meanP95Micros.pretty()},${s.stddevP95Micros.pretty()},${s.meanP99Micros.pretty()},${s.stddevP99Micros.pretty()},${s.meanThroughput.pretty()},${s.stddevThroughput.pretty()},${s.notes.replace(",", ";")}",
        )
    }
}
