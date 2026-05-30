package org.otpstudy.benchmarks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.system.measureNanoTime

/**
 * Raw kotlinx.coroutines Channel benchmarks.
 *
 * These measure the absolute JVM floor for coroutine-based message passing —
 * no actor framework overhead, no supervision, just Channel.send + Channel.receive
 * in a producer/consumer loop.  This is the theoretical minimum cost that any
 * coroutine-based actor library (including kotlin-otp) must pay per message.
 */

// ---------------------------------------------------------------------------
// Message type for request/reply over a raw channel
// ---------------------------------------------------------------------------

private data class RawCall(val payload: String, val reply: CompletableDeferred<String>)

// ---------------------------------------------------------------------------
// Benchmark: call roundtrip (send RawCall, receive Pong via CompletableDeferred)
// ---------------------------------------------------------------------------

fun runChannelCallRoundtrip(profile: BenchProfile): BenchResult {
    val mailbox = Channel<RawCall>(Channel.UNLIMITED)

    val latencies = mutableListOf<Long>()
    var totalNanos = 0L

    runBlocking(Dispatchers.Default) {
        // Actor loop
        val actor = launch {
            for (msg in mailbox) {
                msg.reply.complete("pong")
            }
        }

        // Warm up
        repeat(profile.warmupIterations) {
            val deferred = CompletableDeferred<String>()
            mailbox.send(RawCall("ping", deferred))
            deferred.await()
        }

        totalNanos = measureNanoTime {
            repeat(profile.iterations) {
                val deferred = CompletableDeferred<String>()
                val ns = measureNanoTime {
                    mailbox.send(RawCall("ping", deferred))
                    deferred.await()
                }
                latencies.add(ns)
            }
        }

        mailbox.close()
        actor.join()
    }

    return BenchResult(
        library = "kotlin-channel",
        scenario = "call_roundtrip",
        iterations = profile.iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = profile.iterations * 1_000_000_000.0 / totalNanos,
        notes = "UNLIMITED Channel + CompletableDeferred; Dispatchers.Default; no actor overhead — JVM floor",
    )
}

// ---------------------------------------------------------------------------
// Benchmark: cast enqueue throughput (Channel.trySend, no reply)
// ---------------------------------------------------------------------------

fun runChannelCastEnqueue(profile: BenchProfile): BenchResult {
    val mailbox = Channel<String>(Channel.UNLIMITED)
    var castCount = 0

    val latencies = mutableListOf<Long>()
    var totalNanos = 0L

    runBlocking(Dispatchers.Default) {
        val actor = launch {
            for (msg in mailbox) {
                castCount++
            }
        }

        // Warm up
        repeat(profile.warmupIterations) { mailbox.trySend("w") }

        totalNanos = measureNanoTime {
            repeat(profile.iterations) {
                val ns = measureNanoTime { mailbox.trySend("x") }
                latencies.add(ns)
            }
        }
        mailbox.close()
        actor.join()
    }

    return BenchResult(
        library = "kotlin-channel",
        scenario = "cast_enqueue",
        iterations = profile.iterations,
        p50Micros = percentile(latencies, 50.0),
        p95Micros = percentile(latencies, 95.0),
        p99Micros = percentile(latencies, 99.0),
        p999Micros = percentile(latencies, 99.9),
        throughputPerSec = profile.iterations * 1_000_000_000.0 / totalNanos,
        notes = "Channel.trySend non-blocking; drain confirmed via actor join; JVM floor for cast",
    )
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent callers
// ---------------------------------------------------------------------------

fun runChannelConcurrentCallers(profile: BenchProfile): List<BenchResult> {
    return profile.concurrentCallerCounts.map { callerCount ->
        val mailbox = Channel<RawCall>(Channel.UNLIMITED)
        val latencies = ConcurrentLinkedQueue<Long>()
        var totalNanos = 0L

        runBlocking(Dispatchers.Default) {
            // Actor loop
            val actor = launch {
                for (msg in mailbox) {
                    msg.reply.complete("pong")
                }
            }

            // Warm up single-caller
            repeat(profile.warmupIterations) {
                val d = CompletableDeferred<String>()
                mailbox.send(RawCall("ping", d))
                d.await()
            }

            totalNanos = measureNanoTime {
                coroutineScope {
                    repeat(callerCount) {
                        async {
                            repeat(profile.callsPerCaller) {
                                val d = CompletableDeferred<String>()
                                val ns = measureNanoTime {
                                    mailbox.send(RawCall("ping", d))
                                    d.await()
                                }
                                latencies.add(ns)
                            }
                        }
                    }
                }
            }

            mailbox.close()
            actor.join()
        }

        val all = latencies.toList()
        BenchResult(
            library = "kotlin-channel",
            scenario = "concurrent_callers_$callerCount",
            iterations = all.size,
            p50Micros = percentile(all, 50.0),
            p95Micros = percentile(all, 95.0),
            p99Micros = percentile(all, 99.0),
            p999Micros = percentile(all, 99.9),
            throughputPerSec = all.size * 1_000_000_000.0 / totalNanos,
            notes = "$callerCount coroutines on Dispatchers.Default; no actor overhead — JVM floor for concurrency",
        )
    }
}
