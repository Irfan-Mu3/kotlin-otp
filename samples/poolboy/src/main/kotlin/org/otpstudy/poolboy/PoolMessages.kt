package org.otpstudy.poolboy

import kotlinx.coroutines.Job
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.InfoMsg

/**
 * Public configuration for a pool. Mirrors the `PoolArgs` proplist accepted by
 * [`poolboy:start_link/1,2`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl).
 *
 * - [size]: number of permanent workers (`{size, N}`).
 * - [maxOverflow]: extra ephemeral workers spawned on demand and dismissed on checkin
 *   (`{max_overflow, N}`).
 * - [strategy]: [Strategy.Lifo] (default, matches OTP) or [Strategy.Fifo].
 * - [name]: optional registry name (`{name, {local, Name}}` analogue, registered in
 *   [org.otpstudy.registry.GlobalProcessRegistry]).
 */
data class PoolConfig(
    val size: Int,
    val maxOverflow: Int = 10,
    val strategy: Strategy = Strategy.Lifo,
    val name: String? = null,
) {
    init {
        require(size >= 0) { "size must be >= 0 (got $size)" }
        require(maxOverflow >= 0) { "maxOverflow must be >= 0 (got $maxOverflow)" }
    }

    enum class Strategy { Lifo, Fifo }
}

/**
 * Pool state names returned by [`status/1`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl#L228-L237).
 */
enum class PoolStateName { Ready, Overflow, Full }

/** Snapshot returned by [PoolRef.status]. Tuple `{StateName, AvailableLen, Overflow, Monitors}` in OTP. */
data class PoolStatus(
    val state: PoolStateName,
    val available: Int,
    val overflow: Int,
    val monitors: Int,
)

/**
 * Caller-supplied identity used to monitor the borrower. Equivalent to BEAM's `From`
 * pid in `gen_server:call`. On the JVM we don't get the caller's [Job] for free,
 * so [PoolRef.checkout] takes it explicitly (defaulted to the current coroutine's job).
 */
internal typealias CheckoutRef = Long

/**
 * Pool gen_server protocol — typed alternatives to passing raw `Any` over the
 * mailbox. All requests carry the [CheckoutRef] that poolboy uses to correlate
 * `cancel_waiting` casts back to a specific blocked checkout.
 */
internal sealed class PoolRequest {
    /** `{checkout, CRef, Block}` in poolboy. [borrower] null means use [org.otpstudy.genserver.ReplyHandle.callerJob]. */
    data class Checkout(
        val cref: CheckoutRef,
        val block: Boolean,
        val borrower: Job?,
    ) : PoolRequest()

    /** `status` call. */
    data object Status : PoolRequest()

    /** `stop` call. */
    data object Stop : PoolRequest()
}

internal sealed class PoolCast {
    /** `{checkin, Pid}` in poolboy. */
    data class Checkin(val worker: GenServerRef<*>) : PoolCast()

    /** `{cancel_waiting, CRef}` in poolboy. */
    data class CancelWaiting(val cref: CheckoutRef) : PoolCast()
}

/**
 * Internal info-msg routed into the pool gen_server's mailbox when a borrower's
 * coroutine completes (analogue of BEAM `'DOWN'` for the borrower pid).
 *
 * We use a dedicated message rather than [org.otpstudy.genserver.Down] because
 * we need the checkout [cref] (not just the pool's own monitored [Job]) and we
 * already track the cref ↔ borrower mapping inside the pool actor.
 */
internal data class BorrowerDown(val cref: CheckoutRef) : InfoMsg

/**
 * Internal info-msg routed into the pool gen_server's mailbox when a worker
 * (`GenServerRef`) job completes — analogue of BEAM `'EXIT'` from a linked
 * worker, via [org.otpstudy.core.ProcessMonitor] on the worker's [Job].
 */
internal data class WorkerDown(val worker: GenServerRef<*>) : InfoMsg
