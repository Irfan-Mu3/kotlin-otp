package org.otpstudy.genserver

import java.util.concurrent.atomic.AtomicReference

/**
 * Marker interface for GenServer state types that support atomic snapshot reads.
 *
 * Implement this on your state class to use [CachedReadRef]. [snapshot] must return
 * an immutable copy — callers that read it concurrently with actor mutations must
 * not see partially-updated state.
 *
 * OTP analogy: ETS `protected` table — reads bypass the owning process but may be
 * stale relative to the latest write.
 */
interface CacheableState<S> {
    /**
     * Return an immutable snapshot of this state for concurrent reads.
     * Called by the actor after every state-mutating callback.
     */
    fun snapshot(): S
}

/**
 * Thread-safe, non-suspending snapshot store updated by a [CachingGenServer] wrapper.
 *
 * [readCached] returns the latest published snapshot or `null` before the first
 * state-mutating callback fires.
 *
 * **Consistency guarantee:** eventually consistent. The snapshot may lag behind the
 * actor's current state by one or more messages. Do not use where linearizable reads
 * are required — use [GenServerRef.call] instead.
 *
 * **Hibernate limitation:** state produced by [NoreplyResult.Hibernate.onWake] lambdas
 * is not captured because `onWake` is invoked directly by the actor loop, not through
 * the [GenServer] interface. Do not use [CachingGenServer] with actors that mutate
 * state inside `onWake` callbacks.
 */
class CachedReadRef<S : CacheableState<S>> {
    private val _cache = AtomicReference<S?>(null)

    /** Publish a new snapshot. Called internally by [CachingGenServer] after each mutation. */
    internal fun update(state: S) {
        _cache.set(state.snapshot())
    }

    /**
     * Return the latest published snapshot, or `null` if no state-mutating callback
     * has fired yet.
     *
     * Non-suspending. Backed by [AtomicReference.get] (~50 ns).
     */
    fun readCached(): S? = _cache.get()
}

/**
 * [GenServer] delegation wrapper that publishes state snapshots to a [CachedReadRef]
 * after every state-mutating callback.
 *
 * Intercepts all paths that can produce a new actor state:
 * - [handleCallFrom] (covers both [handleCall] and async [ReplyResult.DeferReply] paths)
 * - [handleCast]
 * - [handleInfo]
 *
 * Usage:
 * ```kotlin
 * val cache = CachedReadRef<MyState>()
 * val ref = GenServers.startLink(scope, CachingGenServer(MyServer(), cache))
 * // Later, non-suspending read:
 * val snapshot = cache.readCached()
 * ```
 */
class CachingGenServer<S : CacheableState<S>>(
    private val inner: GenServer<S>,
    val cache: CachedReadRef<S>,
) : GenServer<S> by inner {

    /**
     * Intercepts [handleCallFrom] rather than [handleCall] so that async-reply paths
     * ([ReplyResult.DeferReply]) also publish the new state to the cache.
     */
    override suspend fun handleCallFrom(
        request: Any,
        state: S,
        from: ReplyHandle<S>,
    ): ReplyResult<S> = inner.handleCallFrom(request, state, from).also { result ->
        when (result) {
            is ReplyResult.Reply     -> cache.update(result.newState)
            is ReplyResult.DeferReply -> cache.update(result.newState)
            is ReplyResult.Stop      -> Unit  // do not publish post-stop state
        }
    }

    override suspend fun handleCast(request: Any, state: S): NoreplyResult<S> =
        inner.handleCast(request, state).also { publishNoreply(it) }

    override suspend fun handleInfo(msg: InfoMsg, state: S): NoreplyResult<S> =
        inner.handleInfo(msg, state).also { publishNoreply(it) }

    private fun publishNoreply(result: NoreplyResult<S>) {
        when (result) {
            is NoreplyResult.Noreply   -> cache.update(result.newState)
            is NoreplyResult.Hibernate -> cache.update(result.newState)  // snapshot at hibernation; onWake state not captured — see class KDoc
            is NoreplyResult.Stop      -> Unit
        }
    }
}
