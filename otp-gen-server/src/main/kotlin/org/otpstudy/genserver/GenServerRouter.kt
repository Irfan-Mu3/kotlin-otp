package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Round-robin router over [size] identical GenServer shards.
 *
 * Each [call] and [cast] is dispatched to the next shard in round-robin order.
 * Shards share no state — use this for stateless or independently-stateful servers
 * where call ordering across shards is not required.
 *
 * Analogous to the OTP poolboy / worker_pool pattern for distributing coordinator load.
 */
class GenServerRouterRef<S> internal constructor(
    internal val shards: List<GenServerRef<S>>,
) {
    private val counter = AtomicInteger(0)

    /** Route a call to the next shard in round-robin order. */
    suspend fun <R> call(request: Any, timeout: Duration = 5.seconds): R =
        nextShard().call(request, timeout)

    /** Route a cast to the next shard in round-robin order. */
    fun cast(request: Any): Unit = nextShard().cast(request)

    /** Stop all shards. */
    suspend fun stop() {
        for (shard in shards) shard.stop()
    }

    /** Number of shards. */
    val size: Int get() = shards.size

    private fun nextShard(): GenServerRef<S> =
        shards[Math.floorMod(counter.getAndIncrement(), shards.size)]
}

/** Factory for [GenServerRouterRef]. */
object GenServerRouters {
    /**
     * Start [size] GenServer shards and return a round-robin router over them.
     *
     * [factory] is called [size] times; each invocation must return a fresh [GenServer] instance.
     * Shards are named `"$name-0"` … `"$name-N"` when [name] is provided.
     *
     * OTP analogy: `supervisor:start_link` with N identical `simple_one_for_one` children,
     * plus a dispatcher that routes messages round-robin.
     */
    fun <S> startLink(
        parent: CoroutineScope,
        size: Int,
        factory: () -> GenServer<S>,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
        mailboxBound: MailboxBound? = null,
        fastReply: Boolean = false,
        hibernateAfter: Duration? = null,
    ): GenServerRouterRef<S> {
        require(size > 0) { "router size must be > 0, got $size" }
        val shards = (0 until size).map { i ->
            GenServers.startLink(
                parent = parent,
                server = factory(),
                context = context,
                name = name?.let { "$it-$i" },
                mailboxBound = mailboxBound,
                fastReply = fastReply,
                hibernateAfter = hibernateAfter,
            )
        }
        return GenServerRouterRef(shards)
    }
}
