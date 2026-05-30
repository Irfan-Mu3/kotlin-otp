package org.otpstudy.investigation

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.distribution.InMemoryTransport
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.distribution.RemoteNodeStub
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.mailbox.SelectiveMailbox
import org.otpstudy.supervisor.ChildSpec
import org.otpstudy.supervisor.Supervisor
import org.otpstudy.supervisor.SupervisorFlags
import org.otpstudy.supervisor.SupervisorStrategy
import kotlinx.coroutines.channels.Channel
import kotlin.math.max
import kotlin.math.sqrt
import kotlin.system.measureNanoTime
import kotlin.time.Duration.Companion.seconds

private data class BenchResult(
    val name: String,
    val iterations: Int,
    val p50Micros: Double,
    val p95Micros: Double,
    val throughputPerSec: Double,
    val notes: String = "",
)

private data class BenchProfile(
    val name: String,
    val iterations: Int,
    val selectiveDepths: List<Int>,
    val selectiveSamples: Int,
    val restartCrashes: Int,
    val memoryCasts: Int,
    val rounds: Int,
)

private data class BenchAggregate(
    val scenario: String,
    val rounds: Int,
    val meanP50Micros: Double,
    val stddevP50Micros: Double,
    val meanP95Micros: Double,
    val stddevP95Micros: Double,
    val meanThroughput: Double,
    val stddevThroughput: Double,
    val notes: String,
)

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

private fun runCallBenchmark(scope: CoroutineScope, iterations: Int): BenchResult = runBlocking {
    val ref = GenServers.startLink(scope, EchoServer(), name = "call-bench")
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
        name = "gen_server_call_roundtrip",
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
    )
}

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
    while (ref.call<Int>("count") < (2_000 + iterations)) delay(1)
    ref.stop()
    BenchResult(
        name = "gen_server_cast_enqueue",
        iterations = iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
        notes = "throughput measures enqueue speed; processing drained via count call",
    )
}

private fun runSelectiveReceive(depth: Int, samples: Int): BenchResult = runBlocking {
    val ch = Channel<Int>(Channel.UNLIMITED)
    val box = SelectiveMailbox(ch)
    repeat(depth) { ch.send(0) }
    ch.send(1)
    val latencies = ArrayList<Long>(samples)
    repeat(samples) {
        // refill to keep scan cost shape stable across iterations
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
        throughputPerSec = latencies.size * 1_000_000_000.0 / total,
        notes = "single matching message behind $depth non-matching messages",
    )
}

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
        throughputPerSec = iterations * 1_000_000_000.0 / totalNanos,
    )
}

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

private fun Double.pretty(): String = "%.2f".format(this)

private fun mean(values: List<Double>): Double = if (values.isEmpty()) 0.0 else values.sum() / values.size

private fun stddev(values: List<Double>): Double {
    if (values.size < 2) return 0.0
    val m = mean(values)
    val variance = values.sumOf { (it - m) * (it - m) } / values.size
    return sqrt(variance)
}

private fun parseProfile(args: Array<String>): BenchProfile {
    val profileArg = args.firstOrNull { it.startsWith("--profile=") }?.substringAfter("=") ?: "quick"
    val roundsArg = args.firstOrNull { it.startsWith("--rounds=") }?.substringAfter("=")?.toIntOrNull()
    val base = when (profileArg.lowercase()) {
        "long" -> BenchProfile(
            name = "long",
            iterations = 50_000,
            selectiveDepths = listOf(1_000, 10_000, 25_000),
            selectiveSamples = 250,
            restartCrashes = 100,
            memoryCasts = 250_000,
            rounds = 5,
        )
        else -> BenchProfile(
            name = "quick",
            iterations = 20_000,
            selectiveDepths = listOf(1_000, 10_000),
            selectiveSamples = 100,
            restartCrashes = 40,
            memoryCasts = 100_000,
            rounds = 1,
        )
    }
    return if (roundsArg != null && roundsArg > 0) base.copy(rounds = roundsArg) else base
}

private fun runRound(profile: BenchProfile, round: Int): List<BenchResult> {
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val results = buildList {
        add(runCallBenchmark(scope, profile.iterations))
        add(runCastBenchmark(scope, profile.iterations))
        for (depth in profile.selectiveDepths) {
            add(runSelectiveReceive(depth = depth, samples = profile.selectiveSamples))
        }
        add(runSupervisorRestartStorm(scope, crashCount = profile.restartCrashes))
        add(runDistributionOverhead(scope, profile.iterations))
        add(runMemoryPressure(scope, casts = profile.memoryCasts))
    }
    println("round=$round complete")
    scope.cancel()
    return results
}

private fun aggregate(rounds: List<List<BenchResult>>): List<BenchAggregate> {
    val byScenario = linkedMapOf<String, MutableList<BenchResult>>()
    for (round in rounds) {
        for (r in round) byScenario.getOrPut(r.name) { mutableListOf() }.add(r)
    }
    return byScenario.entries.map { (scenario, results) ->
        BenchAggregate(
            scenario = scenario,
            rounds = results.size,
            meanP50Micros = mean(results.map { it.p50Micros }),
            stddevP50Micros = stddev(results.map { it.p50Micros }),
            meanP95Micros = mean(results.map { it.p95Micros }),
            stddevP95Micros = stddev(results.map { it.p95Micros }),
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
    println("round,scenario,iterations,p50_us,p95_us,throughput_ops_sec,notes")
    for ((round, r) in perRoundRows) {
        println(
            "$round,${r.name},${r.iterations},${r.p50Micros.pretty()},${r.p95Micros.pretty()},${r.throughputPerSec.pretty()},${r.notes.replace(",", ";")}"
        )
    }
    println("summary_scenario,rounds,mean_p50_us,stddev_p50_us,mean_p95_us,stddev_p95_us,mean_throughput_ops_sec,stddev_throughput_ops_sec,notes")
    for (s in aggregate(roundResults)) {
        println(
            "${s.scenario},${s.rounds},${s.meanP50Micros.pretty()},${s.stddevP50Micros.pretty()},${s.meanP95Micros.pretty()},${s.stddevP95Micros.pretty()},${s.meanThroughput.pretty()},${s.stddevThroughput.pretty()},${s.notes.replace(",", ";")}"
        )
    }
}
