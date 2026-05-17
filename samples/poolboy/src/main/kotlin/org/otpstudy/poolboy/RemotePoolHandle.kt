package org.otpstudy.poolboy

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonElement
import org.otpstudy.distribution.DistributionWire
import org.otpstudy.distribution.NodeId
import org.otpstudy.distribution.NodeTransport
import org.otpstudy.genserver.ServerDownException

/**
 * Pool reached on another [node] via [transport]. Uses [@Serializable] [WirePoolRequest] payloads
 * and returns [RemotePooledWorker] handles ([WorkerToken] + [PoolRequest.ForwardCall]).
 */
class RemotePoolHandle<W>(
    private val transport: NodeTransport,
    private val node: NodeId,
    override val poolName: String,
) : PoolHandle<W> {
    private val stopped = AtomicBoolean(false)

    override suspend fun checkout(
        block: Boolean,
        timeout: Duration,
        borrower: Job?,
    ): PooledWorker<W>? {
        val cref = nextCheckoutRef()
        val wire: WirePoolRequest = WirePoolRequest.Checkout(cref, block)
        return try {
            decodeCheckoutWorker(
                transport.call(node, poolName, DistributionWire.encodeSerializable(wire), timeout),
            )
        } catch (t: Throwable) {
            throwIfPoolGone(t)
            runBlocking {
                val cast: WirePoolCast = WirePoolCast.CancelWaiting(cref)
                transport.send(node, poolName, DistributionWire.encodeSerializable(cast))
            }
            throw t
        }
    }

    override fun checkin(worker: PooledWorker<W>) {
        worker.checkin()
    }

    override suspend fun status(): PoolStatus =
        try {
            decodeStatus(
                transport.call(
                    node,
                    poolName,
                    DistributionWire.encodeSerializable<WirePoolRequest>(WirePoolRequest.Status),
                    5.seconds,
                ),
            )
        } catch (t: Throwable) {
            throwIfPoolGone(t)
        }

    override suspend fun stop() {
        if (!stopped.compareAndSet(false, true)) return
        try {
            transport.call(
                node,
                poolName,
                DistributionWire.encodeSerializable<WirePoolRequest>(WirePoolRequest.Stop),
                5.seconds,
            )
        } catch (_: Throwable) {
        }
    }

    private fun decodeStatus(raw: Any?): PoolStatus =
        when (raw) {
            is WirePoolStatus -> PoolWire.fromWire(raw)
            is PoolStatus -> raw
            is JsonElement -> PoolWire.fromWire(DistributionWire.decodeSerializable<WirePoolStatus>(raw))
            else -> error("unexpected status reply: $raw")
        }

    private fun decodeCheckoutWorker(raw: Any?): PooledWorker<W>? {
        if (raw is String && raw.startsWith("error:")) {
            error(raw.removePrefix("error:"))
        }
        val result =
            when (raw) {
                is WireCheckoutResult -> raw
                is JsonElement -> DistributionWire.decodeSerializable<WireCheckoutResult>(raw)
                else -> null
            }
        return when (result) {
            is WireCheckoutResult.Worker -> RemotePooledWorker(transport, node, poolName, result.token)
            WireCheckoutResult.Full -> null
            null -> null
        }
    }

    private fun throwIfPoolGone(t: Throwable): Nothing {
        when (t) {
            is ServerDownException -> throw PoolStoppedException(poolName, t)
            is IllegalStateException ->
                if (t.message?.contains("no process registered") == true) {
                    throw PoolStoppedException(poolName, t)
                }
        }
        throw t
    }

    private companion object {
        private val crefSeq = AtomicLong(0L)

        private fun nextCheckoutRef(): CheckoutRef = crefSeq.incrementAndGet()
    }
}
