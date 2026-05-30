package org.otpstudy.benchmarks

import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.eventbus.Message
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

private const val BENCH_ADDRESS = "bench.echo"
private const val CAST_ADDRESS = "bench.cast"

// ---------------------------------------------------------------------------
// Bench verticle — registers an EventBus consumer that echoes requests
// ---------------------------------------------------------------------------

private class EchoVerticle : AbstractVerticle() {
    override fun start(startPromise: Promise<Void>) {
        vertx.eventBus().consumer<String>(BENCH_ADDRESS) { msg: Message<String> ->
            msg.reply(msg.body())
        }
        startPromise.complete()
    }
}

// ---------------------------------------------------------------------------
// Shared Vert.x setup / teardown helpers
// ---------------------------------------------------------------------------

private fun startVertx(): Vertx {
    val vertx = Vertx.vertx(VertxOptions().setEventLoopPoolSize(1))
    val latch = CompletableFuture<Void>()
    vertx.deployVerticle(EchoVerticle())
        .onSuccess { latch.complete(null) }
        .onFailure { latch.completeExceptionally(it) }
    latch.get(10, TimeUnit.SECONDS)
    return vertx
}

private fun stopVertx(vertx: Vertx) {
    val done = CompletableFuture<Void>()
    vertx.close()
        .onSuccess { done.complete(null) }
        .onFailure { done.completeExceptionally(it) }
    done.get(10, TimeUnit.SECONDS)
}

/** Block the calling thread until a Vert.x Future<Message<T>> resolves. */
private fun <T> io.vertx.core.Future<Message<T>>.awaitBlocking(timeoutMs: Long = 5_000): Message<T> {
    val f = CompletableFuture<Message<T>>()
    onSuccess { f.complete(it) }
    onFailure { f.completeExceptionally(it) }
    return f.get(timeoutMs, TimeUnit.MILLISECONDS)
}

// ---------------------------------------------------------------------------
// Benchmark: call roundtrip (EventBus.request → reply)
// ---------------------------------------------------------------------------

fun runVertxCallRoundtrip(profile: BenchProfile): BenchResult {
    val vertx = startVertx()
    val eb = vertx.eventBus()

    // Warm up: fire-and-wait using Future API
    repeat(profile.warmupIterations) {
        eb.request<String>(BENCH_ADDRESS, "ping").awaitBlocking()
    }

    val latencies = mutableListOf<Long>()
    val totalNanos = measureNanoTime {
        repeat(profile.iterations) {
            val ns = measureNanoTime {
                eb.request<String>(BENCH_ADDRESS, "ping").awaitBlocking()
            }
            latencies.add(ns)
        }
    }

    stopVertx(vertx)

    return BenchResult(
        library = "vertx",
        scenario = "call_roundtrip",
        iterations = profile.iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = profile.iterations * 1_000_000_000.0 / totalNanos,
        notes = "EventBus.request(); Future API; single event loop; caller blocks on CompletableFuture.get()",
    )
}

// ---------------------------------------------------------------------------
// Benchmark: cast enqueue (EventBus.send, no reply)
// ---------------------------------------------------------------------------

fun runVertxCastEnqueue(profile: BenchProfile): BenchResult {
    val vertx = Vertx.vertx(VertxOptions().setEventLoopPoolSize(1))

    val counter = java.util.concurrent.atomic.AtomicLong(0)
    val deployed = CompletableFuture<Void>()
    vertx.runOnContext {
        vertx.eventBus().consumer<String>(CAST_ADDRESS) { counter.incrementAndGet() }
        deployed.complete(null)
    }
    deployed.get(5, TimeUnit.SECONDS)

    // Warm up
    repeat(profile.warmupIterations) { vertx.eventBus().send(CAST_ADDRESS, "w") }

    val latencies = mutableListOf<Long>()
    val totalNanos = measureNanoTime {
        repeat(profile.iterations) {
            val ns = measureNanoTime { vertx.eventBus().send(CAST_ADDRESS, "x") }
            latencies.add(ns)
        }
    }
    // Drain: wait until all messages have been processed
    val deadline = System.currentTimeMillis() + 5_000
    val expected = (profile.warmupIterations + profile.iterations).toLong()
    while (counter.get() < expected && System.currentTimeMillis() < deadline) Thread.sleep(10)

    stopVertx(vertx)

    return BenchResult(
        library = "vertx",
        scenario = "cast_enqueue",
        iterations = profile.iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = profile.iterations * 1_000_000_000.0 / totalNanos,
        notes = "EventBus.send() fire-and-forget; drain confirmed via atomic counter",
    )
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent callers
// ---------------------------------------------------------------------------

fun runVertxConcurrentCallers(profile: BenchProfile): List<BenchResult> {
    return profile.concurrentCallerCounts.map { callerCount ->
        val vertx = startVertx()
        val eb = vertx.eventBus()

        // Warm up single-threaded
        repeat(profile.warmupIterations) {
            eb.request<String>(BENCH_ADDRESS, "w").awaitBlocking()
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
                            eb.request<String>(BENCH_ADDRESS, "ping").awaitBlocking()
                        }
                        latencies.add(ns)
                    }
                }.also { t -> t.start() }
            }
            threads.forEach { it.join() }
        }

        stopVertx(vertx)

        val all = latencies.toList()
        BenchResult(
            library = "vertx",
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
