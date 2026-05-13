package org.otpstudy.distribution

import org.otpstudy.core.OtpStudyDebug
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration

/**
 * In-process transport that routes between two [LocalNode]s in the same JVM.
 *
 * Intended for tests: no serialisation, no network, deterministic delivery.
 * Wire-compatible distribution (EPMD + TCP + Erlang dist protocol) belongs in
 * `otp-jinterface` (see docs/archive/roadmaps/THE_DEEP_END.md §3).
 *
 * [connect] and [disconnect] fire [NodeMonitor] lifecycle events so that actors
 * subscribed via [NodeMonitor.monitorNode] receive [NodeEvent.NodeUp] / [NodeEvent.NodeDown].
 */
class InMemoryTransport : NodeTransport {
    private val nodes = ConcurrentHashMap<NodeId, LocalNode>()

    fun addNode(node: LocalNode) {
        nodes[node.id] = node
    }

    /**
     * Connect two nodes and notify subscribers of both.
     * Analogous to net_kernel establishing a distribution connection.
     */
    fun connect(a: LocalNode, b: LocalNode) {
        nodes[a.id] = a
        nodes[b.id] = b
        NodeMonitor.notifyUp(a.id)
        NodeMonitor.notifyUp(b.id)
    }

    /**
     * Disconnect a node and notify its subscribers.
     * Analogous to erts_do_net_exits signalling nodedown to all monitors.
     */
    fun disconnect(nodeId: NodeId, reason: String = "disconnected") {
        nodes.remove(nodeId)
        NodeMonitor.notifyDown(nodeId, reason)
    }

    override suspend fun send(targetNode: NodeId, targetName: String, message: Any) {
        OtpStudyDebug.trace { "InMemoryTransport.send node=$targetNode name=$targetName" }
        val node = nodes[targetNode]
            ?: throw IllegalStateException("unknown node: $targetNode")
        node.cast(targetName, message)
    }

    override suspend fun call(
        targetNode: NodeId,
        targetName: String,
        request: Any,
        timeout: Duration,
    ): Any? {
        OtpStudyDebug.trace { "InMemoryTransport.call node=$targetNode name=$targetName" }
        val node = nodes[targetNode]
            ?: throw IllegalStateException("unknown node: $targetNode")
        return node.call<Any?>(targetName, request, timeout)
    }
}
