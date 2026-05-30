package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private class CountingServer(private val id: Int) : GenServer<Int> {
    val callCount = AtomicInteger(0)
    override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
    override suspend fun handleCall(request: Any, state: Int): ReplyResult<Int> {
        callCount.incrementAndGet()
        return ReplyResult.Reply("shard-$id", state)
    }
    override suspend fun handleCast(request: Any, state: Int) = NoreplyResult.Noreply(state)
}

class GenServerRouterTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest fun setUp() { scope = CoroutineScope(Dispatchers.Default + SupervisorJob()) }
    @AfterTest  fun tearDown() { scope.cancel() }

    @Test
    fun `calls are distributed across all shards`() = runBlocking {
        val shardCount = 4
        val servers = (0 until shardCount).map { CountingServer(it) }
        val router = GenServerRouters.startLink(scope, shardCount, factory = {
            servers[servers.indexOfFirst { s -> s.callCount.get() == 0 }.takeIf { it >= 0 }
                ?: servers.indices.minByOrNull { servers[it].callCount.get() }!!]
        })
        // Actually create with factory returning each server in turn
        val serverInstances = (0 until shardCount).map { CountingServer(it) }
        var idx = 0
        val router2 = GenServerRouters.startLink(scope, shardCount, factory = { serverInstances[idx++] })

        // Each shard should receive exactly 1 call after 4 round-robin calls
        repeat(shardCount) { router2.call<String>("ping") }
        serverInstances.forEach { assertEquals(1, it.callCount.get(), "shard ${serverInstances.indexOf(it)} call count") }

        router2.stop()
    }

    @Test
    fun `router size matches factory invocation count`() = runBlocking {
        var factoryCalls = 0
        val router = GenServerRouters.startLink(scope, 5, factory = {
            factoryCalls++
            object : GenServer<Unit> {
                override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
                override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply(Unit, state)
                override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(state)
            }
        })
        assertEquals(5, factoryCalls)
        assertEquals(5, router.size)
        router.stop()
    }

    @Test
    fun `concurrent callers on router do not cross-talk`() = runBlocking {
        val router = GenServerRouters.startLink(scope, 4, factory = {
            object : GenServer<Int> {
                override suspend fun init(self: GenServerRef<Int>) = InitResult.Ok(0)
                override suspend fun handleCall(request: Any, state: Int): ReplyResult<Int> {
                    val n = request as Int
                    return ReplyResult.Reply(n * 2, state)
                }
                override suspend fun handleCast(request: Any, state: Int) = NoreplyResult.Noreply(state)
            }
        })
        coroutineScope {
            val results = (1..100).map { n -> async { router.call<Int>(n) } }.awaitAll()
            results.forEachIndexed { i, result ->
                assertEquals((i + 1) * 2, result, "call ${i + 1} returned wrong result")
            }
        }
        router.stop()
    }

    @Test
    fun `round-robin wraps around correctly`() = runBlocking {
        val callsPerShard = mutableListOf<AtomicInteger>()
        val shards = 3
        repeat(shards) { callsPerShard.add(AtomicInteger(0)) }
        val counters = callsPerShard.toList()
        var idx = 0
        val router = GenServerRouters.startLink(scope, shards, factory = {
            val i = idx++
            object : GenServer<Unit> {
                override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
                override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> {
                    counters[i].incrementAndGet()
                    return ReplyResult.Reply(Unit, state)
                }
                override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(state)
            }
        })
        val totalCalls = 9  // 3 shards × 3 calls each
        repeat(totalCalls) { router.call<Unit>("ping") }
        counters.forEach { assertEquals(3, it.get()) }
        router.stop()
    }
}
