package org.otpstudy.benchmarks

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.AskPattern
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Receive
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

sealed interface AkkaMsg
data class AkkaPing(val replyTo: ActorRef<AkkaPong>) : AkkaMsg
data class AkkaPong(val token: String = "pong")
data class AkkaCast(val payload: String) : AkkaMsg
object AkkaStop : AkkaMsg
data class AkkaCountQuery(val replyTo: ActorRef<AkkaCount>) : AkkaMsg
data class AkkaCount(val n: Int)

sealed interface AkkaSuperMsg
object AkkaCrash : AkkaSuperMsg
data class AkkaAskStable(val replyTo: ActorRef<AkkaStable>) : AkkaSuperMsg
object AkkaStable

private class AkkaEchoActor(ctx: ActorContext<AkkaMsg>) : AbstractBehavior<AkkaMsg>(ctx) {
    private var castCount = 0

    override fun createReceive(): Receive<AkkaMsg> = newReceiveBuilder()
        .onMessage(AkkaPing::class.java) { msg ->
            msg.replyTo.tell(AkkaPong())
            Behaviors.same()
        }
        .onMessage(AkkaCast::class.java) {
            castCount++
            Behaviors.same()
        }
        .onMessage(AkkaCountQuery::class.java) { msg ->
            msg.replyTo.tell(AkkaCount(castCount))
            Behaviors.same()
        }
        .onMessage(AkkaStop::class.java) {
            Behaviors.stopped()
        }
        .build()

    companion object {
        fun create(): Behavior<AkkaMsg> = Behaviors.setup { AkkaEchoActor(it) }
    }
}

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

fun runAkkaCallRoundtrip(profile: BenchProfile): BenchResult {
    val system = ActorSystem.create(AkkaEchoActor.create(), "akka-call")
    val actor = system as ActorSystem<AkkaMsg>

    repeat(profile.warmupIterations) {
        actor.ask<AkkaMsg, AkkaPong>(actor) { replyTo -> AkkaPing(replyTo) }
    }

    val latencies = mutableListOf<Long>()
    val totalNanos = measureNanoTime {
        repeat(profile.iterations) {
            val ns = measureNanoTime {
                actor.ask<AkkaMsg, AkkaPong>(actor) { replyTo -> AkkaPing(replyTo) }
            }
            latencies.add(ns)
        }
    }
    system.terminate()
    system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS)

    return BenchResult(
        library = "akka",
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

fun runAkkaCastEnqueue(profile: BenchProfile): BenchResult {
    val system = ActorSystem.create(AkkaEchoActor.create(), "akka-cast")
    val actor = system as ActorSystem<AkkaMsg>

    repeat(profile.warmupIterations) { actor.tell(AkkaCast("w")) }

    val latencies = mutableListOf<Long>()
    val totalNanos = measureNanoTime {
        repeat(profile.iterations) {
            val ns = measureNanoTime { actor.tell(AkkaCast("x")) }
            latencies.add(ns)
        }
    }
    actor.ask<AkkaMsg, AkkaCount>(actor) { replyTo -> AkkaCountQuery(replyTo) }
    system.terminate()
    system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS)

    return BenchResult(
        library = "akka",
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

fun runAkkaConcurrentCallers(profile: BenchProfile): List<BenchResult> {
    return profile.concurrentCallerCounts.map { callerCount ->
        val system = ActorSystem.create(AkkaEchoActor.create(), "akka-concurrent-$callerCount")
        val actor = system as ActorSystem<AkkaMsg>

        repeat(profile.warmupIterations) {
            actor.ask<AkkaMsg, AkkaPong>(actor) { replyTo -> AkkaPing(replyTo) }
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
                            actor.ask<AkkaMsg, AkkaPong>(actor) { replyTo -> AkkaPing(replyTo) }
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
            library = "akka",
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

fun runAkkaSupervisorRestart(profile: BenchProfile): BenchResult {
    val warmupCrashes = 10

    fun oneRestartNanos(): Long {
        val supervised: Behavior<AkkaSuperMsg> = Behaviors.supervise(
            Behaviors.setup<AkkaSuperMsg> {
                var restarted = false
                Behaviors.receive { _, msg ->
                    when (msg) {
                        is AkkaCrash -> {
                            if (!restarted) {
                                restarted = true
                                throw RuntimeException("benchmark crash")
                            }
                            Behaviors.same()
                        }
                        is AkkaAskStable -> {
                            msg.replyTo.tell(AkkaStable)
                            Behaviors.same()
                        }
                    }
                }
            }
        ).onFailure(RuntimeException::class.java, SupervisorStrategy.restart())

        val system = ActorSystem.create(supervised, "akka-sup-restart")
        val t0 = System.nanoTime()
        system.tell(AkkaCrash)
        val timeout = Duration.ofMillis(5_000)
        AskPattern.ask(
            system as ActorRef<AkkaSuperMsg>,
            { replyTo: ActorRef<AkkaStable> -> AkkaAskStable(replyTo) },
            timeout,
            system.scheduler(),
        ).toCompletableFuture().get(5, TimeUnit.SECONDS)
        val t1 = System.nanoTime()
        system.terminate()
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS)
        return t1 - t0
    }

    repeat(warmupCrashes) { oneRestartNanos() }
    val ns = oneRestartNanos()

    return BenchResult(
        library = "akka",
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
