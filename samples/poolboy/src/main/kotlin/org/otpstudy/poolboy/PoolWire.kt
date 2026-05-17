package org.otpstudy.poolboy

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import org.otpstudy.distribution.DistributionWire

@Serializable
data class WorkerToken(
    val workerId: Long,
    val checkoutCref: Long,
)

@Serializable
sealed class WirePoolRequest {
    @Serializable
    @SerialName("checkout")
    data class Checkout(
        val cref: Long,
        val block: Boolean,
    ) : WirePoolRequest()

    @Serializable
    @SerialName("status")
    data object Status : WirePoolRequest()

    @Serializable
    @SerialName("stop")
    data object Stop : WirePoolRequest()

    @Serializable
    @SerialName("forward")
    data class ForwardCall(
        val checkoutCref: Long,
        val workerId: Long,
        val requestJson: JsonElement,
    ) : WirePoolRequest()
}

@Serializable
sealed class WirePoolCast {
    @Serializable
    @SerialName("checkin_token")
    data class CheckinToken(
        val workerId: Long,
        val checkoutCref: Long,
    ) : WirePoolCast()

    @Serializable
    @SerialName("cancel_waiting")
    data class CancelWaiting(val cref: Long) : WirePoolCast()
}

@Serializable
data class WirePoolStatus(
    val state: String,
    val available: Int,
    val overflow: Int,
    val monitors: Int,
)

@Serializable
sealed class WireCheckoutResult {
    @Serializable
    @SerialName("worker")
    data class Worker(val token: WorkerToken) : WireCheckoutResult()

    @Serializable
    @SerialName("full")
    data object Full : WireCheckoutResult()
}

@Serializable
data class WireForwardCall(
    val workerId: Long,
    val requestJson: JsonElement,
)

internal object PoolWire {
    fun toWire(req: PoolRequest): WirePoolRequest =
        when (req) {
            is PoolRequest.Checkout ->
                WirePoolRequest.Checkout(req.cref, req.block)
            PoolRequest.Status -> WirePoolRequest.Status
            PoolRequest.Stop -> WirePoolRequest.Stop
            is PoolRequest.ForwardCall ->
                WirePoolRequest.ForwardCall(
                    req.checkoutCref,
                    req.workerId,
                    DistributionWire.encodePayload(req.request),
                )
        }

    fun fromWire(req: WirePoolRequest): PoolRequest =
        when (req) {
            is WirePoolRequest.Checkout ->
                PoolRequest.Checkout(req.cref, req.block, borrower = null)
            WirePoolRequest.Status -> PoolRequest.Status
            WirePoolRequest.Stop -> PoolRequest.Stop
            is WirePoolRequest.ForwardCall ->
                PoolRequest.ForwardCall(
                    req.checkoutCref,
                    req.workerId,
                    DistributionWire.decodeGenServerPayload(req.requestJson),
                )
        }

    fun toWire(cast: PoolCast): WirePoolCast =
        when (cast) {
            is PoolCast.Checkin ->
                error("use WirePoolCast.CheckinToken for remote checkin")
            is PoolCast.CheckinToken ->
                WirePoolCast.CheckinToken(cast.workerId, cast.checkoutCref)
            is PoolCast.CancelWaiting ->
                WirePoolCast.CancelWaiting(cast.cref)
        }

    fun fromWire(cast: WirePoolCast): PoolCast =
        when (cast) {
            is WirePoolCast.CheckinToken ->
                PoolCast.CheckinToken(cast.workerId, cast.checkoutCref)
            is WirePoolCast.CancelWaiting ->
                PoolCast.CancelWaiting(cast.cref)
        }

    fun toWire(status: PoolStatus): WirePoolStatus =
        WirePoolStatus(
            state = status.state.name,
            available = status.available,
            overflow = status.overflow,
            monitors = status.monitors,
        )

    fun fromWire(status: WirePoolStatus): PoolStatus =
        PoolStatus(
            state = PoolStateName.valueOf(status.state),
            available = status.available,
            overflow = status.overflow,
            monitors = status.monitors,
        )

    fun encodeRequest(req: PoolRequest): JsonElement =
        DistributionWire.encodeSerializable<WirePoolRequest>(toWire(req))

    fun decodeRequest(el: JsonElement): PoolRequest =
        fromWire(DistributionWire.decodeSerializable<WirePoolRequest>(el))

    fun encodeCast(cast: PoolCast): JsonElement =
        DistributionWire.encodeSerializable<WirePoolCast>(toWire(cast))

    fun decodeCast(el: JsonElement): PoolCast =
        fromWire(DistributionWire.decodeSerializable<WirePoolCast>(el))
}
