package org.otpstudy.poolboy

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonElement
import org.otpstudy.distribution.DistributionWire
import org.otpstudy.distribution.NodeId
import org.otpstudy.distribution.NodeTransport
import org.otpstudy.genserver.GenServerRef

/**
 * Borrowed pool worker handle. Same JVM uses a real [GenServerRef]; cross-JVM uses
 * [WorkerToken] + [PoolRequest.ForwardCall] on the home pool.
 */
interface PooledWorker<W> {
    suspend fun <R> call(request: Any, timeout: Duration = 5.seconds): R

    fun cast(request: Any)

    /** Return this worker to its pool (token or local ref). */
    fun checkin()
}

class LocalPooledWorker<W>(
    val ref: GenServerRef<W>,
    private val onCheckin: (GenServerRef<W>) -> Unit,
) : PooledWorker<W> {
    override suspend fun <R> call(request: Any, timeout: Duration): R = ref.call(request, timeout)

    override fun cast(request: Any) {
        ref.cast(request)
    }

    override fun checkin() {
        onCheckin(ref)
    }
}

class RemotePooledWorker<W>(
    private val transport: NodeTransport,
    private val node: NodeId,
    private val poolName: String,
    private val token: WorkerToken,
) : PooledWorker<W> {
    override suspend fun <R> call(request: Any, timeout: Duration): R {
        val wire: WirePoolRequest =
            WirePoolRequest.ForwardCall(
                token.checkoutCref,
                token.workerId,
                DistributionWire.encodePayload(request),
            )
        val raw = transport.call(node, poolName, DistributionWire.encodeSerializable(wire), timeout)
        val decoded =
            when (raw) {
                is JsonElement -> DistributionWire.decodeGenServerPayload(raw)
                else -> raw
            }
        @Suppress("UNCHECKED_CAST")
        return when (decoded) {
            is Int -> decoded as R
            is Long -> decoded.toInt() as R
            else -> decoded as R
        }
    }

    override fun cast(request: Any) {
        runBlocking {
            val wire: WirePoolRequest =
                WirePoolRequest.ForwardCall(
                    token.checkoutCref,
                    token.workerId,
                    DistributionWire.encodePayload(request),
                )
            transport.send(node, poolName, DistributionWire.encodeSerializable(wire))
        }
    }

    override fun checkin() {
        runBlocking {
            val cast: WirePoolCast =
                WirePoolCast.CheckinToken(token.workerId, token.checkoutCref)
            transport.send(node, poolName, DistributionWire.encodeSerializable(cast))
        }
    }
}
