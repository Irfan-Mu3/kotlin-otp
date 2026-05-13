package org.otpstudy.distribution

import org.otpstudy.genserver.GenServerRef
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Subscribe to lifecycle events for remote nodes.
 *
 * Analogous to erlang:monitor_node/2. Registered watchers receive
 * [NodeEvent.NodeUp] and [NodeEvent.NodeDown] via their [GenServer.handleInfo] handler.
 *
 * Watcher registrations are automatically cleaned up when the watcher's job completes.
 *
 * OTP source: lib/kernel/src/net_kernel.erl — monitor_node/2, handle_nodeup/1
 */
object NodeMonitor {
    private val subscribers = ConcurrentHashMap<NodeId, CopyOnWriteArrayList<GenServerRef<*>>>()

    /**
     * Register [watcher] to receive [NodeEvent] notifications for [nodeId].
     * Returns an [AutoCloseable] that removes the subscription.
     */
    fun monitorNode(nodeId: NodeId, watcher: GenServerRef<*>): AutoCloseable {
        subscribers.getOrPut(nodeId) { CopyOnWriteArrayList() }.add(watcher)
        watcher.job.invokeOnCompletion { demonitorNode(nodeId, watcher) }
        return AutoCloseable { demonitorNode(nodeId, watcher) }
    }

    fun demonitorNode(nodeId: NodeId, watcher: GenServerRef<*>) {
        subscribers[nodeId]?.remove(watcher)
    }

    internal fun notifyUp(nodeId: NodeId) {
        subscribers[nodeId]?.forEach { runCatching { it.sendInfo(NodeEvent.NodeUp(nodeId)) } }
    }

    internal fun notifyDown(nodeId: NodeId, reason: String = "disconnected") {
        subscribers[nodeId]?.forEach { runCatching { it.sendInfo(NodeEvent.NodeDown(nodeId, reason)) } }
        subscribers.remove(nodeId)
    }

    fun reset() {
        subscribers.clear()
    }
}
