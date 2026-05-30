package org.otpstudy.benchmarks

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.SupervisorStrategy
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

// ---------------------------------------------------------------------------
// Typed message protocol
// ---------------------------------------------------------------------------

sealed interface PekkoMsg
data class Ping(val replyTo: ActorRef<Pong>) : PekkoMsg
data class Pong(val token: String = "pong")
data class PekkoCast(val payload: String) : PekkoMsg
object PekkoStop : PekkoMsg
data class PekkoCountQuery(val replyTo: ActorRef<PekkoCount>) : PekkoMsg
data class PekkoCount(val n: Int)

/** Supervisor restart protocol */
sealed interface SuperMsg
object Crash : SuperMsg
data class AskStable(val replyTo: ActorRef<Stable>) : SuperMsg
object Stable

// ---------------------------------------------------------------------------
// Echo actor behavior — handles Ping with immediate Pong reply
// ---------------------------------------------------------------------------

private class EchoActor(ctx: ActorContext<PekkoMsg>) : AbstractBehavior<PekkoMsg>(ctx) {
    private var castCount = 0

    override fun createReceive(): Receive<PekkoMsg> = newReceiveBuilder()
        .onMessage(Ping::class.java) { msg ->
            msg.replyTo.tell(Pong())
            Behaviors.same()
        }
        .onMessage(PekkoCast::class.java) {
            castCount++
            Behaviors.same()
        }
        .onMessage(PekkoCountQuery::class.java) { msg ->
            msg.replyTo.tell(PekkoCount(castCount))
            Behaviors.same()
        }
        .onMessage(PekkoStop::class.java) {
            Behaviors.stopped()
        }
        .build()

    companion object {
        fun create(): Behavior<PekkoMsg> = Behaviors.setup { EchoActor(it) }
    }
}

// ---------------------------------------------------------------------------
// Crashable child for supervisor restart benchmark
// ---------------------------------------------------------------------------
// Removed — supervisor restart benchmark uses an inline Behaviors.receive lambda.

// ---------------------------------------------------------------------------
// Helper: ask() wrapper that blocks the calling thread
// ---------------------------------------------------------------------------

private fun <Req, Resp> ActorSystem<*>.ask(
    actor: ActorRef<Req>,
    timeoutMs: Long = 5_000,
    msgFactory: (ActorRef<Resp>) -> Req,
): Resp {
    val timeout = Duration.ofMillis(timeoutMs)
    return AskPattern.ask(actor, { replyTo: ActorRef<Resp> -> msgFactory(replyTo) }, timeout, scheduler())
        .toCompletableFuture()
        .get(timeoutMs, TimeUnit.MILLISECONDS)
}

// ---------------------------------------------------------------------------
// Benchmark: call roundtrip
// ---------------------------------------------------------------------------

fun runPekkaCallRoundtrip(profile: BenchProfile): BenchResult {
    val system = ActorSystem.create(EchoActor.create(), "pekko-call")
    val actor = system as ActorSystem<PekkoMsg>

    // Warm up
    repeat(profile.warmupIterations) {
        actor.ask<PekkoMsg, Pong>(actor) { replyTo -> Ping(replyTo) }
    }

    val latencies = mutableListOf<Long>()
    val totalNanos = measureNanoTime {
        repeat(profile.iterations) {
            val ns = measureNanoTime {
                actor.ask<PekkoMsg, Pong>(actor) { replyTo -> Ping(replyTo) }
            }
            latencies.add(ns)
        }
    }
    system.terminate()
    system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS)

    return BenchResult(
        library = "pekko",
        scenario = "call_roundtrip",
        iterations = profile.iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = profile.iterations * 1_000_000_000.0 / totalNanos,
        notes = "Typed ActorSystem; AskPattern.ask; single sequential caller",
    )
}

// ---------------------------------------------------------------------------
// Benchmark: cast enqueue throughput
// ---------------------------------------------------------------------------

fun runPekkaCastEnqueue(profile: BenchProfile): BenchResult {
    val system = ActorSystem.create(EchoActor.create(), "pekko-cast")
    val actor = system as ActorSystem<PekkoMsg>

    // Warm up
    repeat(profile.warmupIterations) { actor.tell(PekkoCast("w")) }

    val latencies = mutableListOf<Long>()
    val totalNanos = measureNanoTime {
        repeat(profile.iterations) {
            val ns = measureNanoTime { actor.tell(PekkoCast("x")) }
            latencies.add(ns)
        }
    }
    // Wait for drain before terminating
    actor.ask<PekkoMsg, PekkoCount>(actor) { replyTo -> PekkoCountQuery(replyTo) }
    system.terminate()
    system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS)

    return BenchResult(
        library = "pekko",
        scenario = "cast_enqueue",
        iterations = profile.iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = profile.iterations * 1_000_000_000.0 / totalNanos,
        notes = "actor.tell() enqueue-only cost; drain confirmed via CountQuery",
    )
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent callers
// ---------------------------------------------------------------------------

fun runPekkaConcurrentCallers(profile: BenchProfile): List<BenchResult> {
    return profile.concurrentCallerCounts.map { callerCount ->
        val system = ActorSystem.create(EchoActor.create(), "pekko-concurrent-$callerCount")
        val actor = system as ActorSystem<PekkoMsg>

        // Warm up single-threaded
        repeat(profile.warmupIterations) {
            actor.ask<PekkoMsg, Pong>(actor) { replyTo -> Ping(replyTo) }
        }

        val latencies = ConcurrentLinkedQueue<Long>()
        val latch = CountDownLatch(callerCount)
        val totalNanos = measureNanoTime {
            val threads = (1..callerCount).map {
                Thread {
                    latch.countDown()
                    latch.await()
                    repeat(profile.callsPerCaller) {
                        val ns = measureNanoTime {
                            actor.ask<PekkoMsg, Pong>(actor) { replyTo -> Ping(replyTo) }
                        }
                        latencies.add(ns)
                    }
                }.also { t -> t.start() }
            }
            threads.forEach { it.join() }
        }
        system.terminate()
        system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS)

        val all = latencies.toList()
        BenchResult(
            library = "pekko",
            scenario = "concurrent_callers_$callerCount",
            iterations = all.size,
            p50Micros = percentile(all, 50.0),
            p95Micros = percentile(all, 95.0),
            p99Micros = percentile(all, 99.0),
            p999Micros = percentile(all, 99.9),
            throughputPerSec = all.size * 1_000_000_000.0 / totalNanos,
            notes = "$callerCount concurrent Java threads; CountDownLatch barrier",
        )
    }
}

// ---------------------------------------------------------------------------
// Benchmark: supervisor single restart (warm JIT)
//
// Design: supervised actor receives a Crash message → throws → supervisor
// restarts it → actor replies to a post-restart AskStable probe.
// We measure the time from sending Crash to receiving the Stable reply.
// ---------------------------------------------------------------------------

fun runPekkaSupervisorRestart(profile: BenchProfile): BenchResult {
    val warmupCrashes = 10

    fun oneRestartNanos(): Long {
        val stable = CompletableFuture<Unit>()
        // Wrap the crashable behavior in a supervisor that restarts on RuntimeException
        val supervised: Behavior<SuperMsg> = Behaviors.supervise(
            Behaviors.setup<SuperMsg> { ctx ->
                var restarted = false
                Behaviors.receive { _, msg ->
                    when (msg) {
                        is Crash -> {
                            if (!restarted) {
                                restarted = true
                                throw RuntimeException("benchmark crash")
                            }
                            Behaviors.same()
                        }
                        is AskStable -> {
                            msg.replyTo.tell(Stable)
                            Behaviors.same()
                        }
                        else -> Behaviors.same()
                    }
                }
            }
        ).onFailure(RuntimeException::class.java, SupervisorStrategy.restart())

        val system = ActorSystem.create(supervised, "pekko-sup-restart")
        val t0 = System.nanoTime()
        system.tell(Crash)
        // Poll until a post-restart AskStable succeeds
        val timeout = Duration.ofMillis(5_000)
        AskPattern.ask(
            system as ActorRef<SuperMsg>,
            { replyTo: ActorRef<Stable> -> AskStable(replyTo) },
            timeout,
            system.scheduler(),
        ).toCompletableFuture().get(5, TimeUnit.SECONDS)
        val t1 = System.nanoTime()
        system.terminate()
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS)
        return t1 - t0
    }

    // Warm-up phase: discard timing
    repeat(warmupCrashes) { oneRestartNanos() }

    // Measurement: single restart
    val ns = oneRestartNanos()

    return BenchResult(
        library = "pekko",
        scenario = "supervisor_single_restart",
        iterations = 1,
        p50Micros = ns / 1_000.0,
        p95Micros = ns / 1_000.0,
        p99Micros = ns / 1_000.0,
        p999Micros = ns / 1_000.0,
        throughputPerSec = 1_000_000_000.0 / ns,
        notes = "Crash msg → RuntimeException → restart → AskStable probe; $warmupCrashes JIT warm-up restarts",
    )
}
