package org.otpstudy.distribution

import org.otpstudy.genserver.InfoMsg

/**
 * Lifecycle event delivered to actors that called [NodeMonitor.monitorNode].
 *
 * Analogous to {nodeup, Node} / {nodedown, Node} messages in Erlang.
 *
 * OTP source: lib/kernel/src/net_kernel.erl — nodeup/nodedown delivery
 */
sealed class NodeEvent : InfoMsg {
    /** The named node connected or was first seen. */
    data class NodeUp(val nodeId: NodeId) : NodeEvent()

    /** The named node disconnected or stopped. */
    data class NodeDown(val nodeId: NodeId, val reason: String = "disconnected") : NodeEvent()
}
