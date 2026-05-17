package org.otpstudy.distribution

/**
 * Hook for [DistMsg.Global] frames received by [KotlinNodeTransport].
 * [org.otpstudy.global.GlobalRegistry] installs a handler during `install`.
 */
fun interface GlobalReplicationHandler {
    fun onMessage(msg: GlobalDistMsg, fromNode: NodeId)
}

object GlobalReplicationBus {
    @Volatile
    var handler: GlobalReplicationHandler? = null

    /** Invoked when a distribution link to [peer] comes up (after handshake). */
    @Volatile
    var onPeerConnected: ((NodeId) -> Unit)? = null
}
