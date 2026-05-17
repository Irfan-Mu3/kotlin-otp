package org.otpstudy.distribution

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Cluster-wide name registration events (OTP `global` semantics, kotlin-otp wire only).
 */
@Serializable
sealed class GlobalDistMsg {
    @Serializable
    @SerialName("register")
    data class Register(
        val name: String,
        val homeNode: String,
        val localName: String,
        val processId: Long,
    ) : GlobalDistMsg()

    @Serializable
    @SerialName("unregister")
    data class Unregister(
        val name: String,
        val homeNode: String,
        val processId: Long,
    ) : GlobalDistMsg()

    /** Full snapshot exchanged on [GlobalReplicationBus.syncPeers]. */
    @Serializable
    @SerialName("sync")
    data class SyncSnapshot(
        val entries: List<Register>,
    ) : GlobalDistMsg()
}

/** Node id wire form: `name@host`. */
fun NodeId.toWire(): String = "${name}@${host}"

fun nodeIdFromWire(wire: String): NodeId = parseNodeId(wire)
