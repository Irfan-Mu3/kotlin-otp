package org.otpstudy.global

import kotlinx.coroutines.runBlocking
import org.otpstudy.core.OtpProcessId
import org.otpstudy.distribution.GlobalDistMsg
import org.otpstudy.distribution.GlobalReplicationBus
import org.otpstudy.distribution.GlobalReplicationHandler
import org.otpstudy.distribution.InMemoryTransport
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.distribution.NodeTransport
import org.otpstudy.distribution.RemoteGenServerRef
import org.otpstudy.distribution.toWire
import org.otpstudy.genserver.GenServerRef
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Distributed global name registry — OTP's `global` module for kotlin-otp.
 *
 * Call [install] once per logical node (same JVM may host multiple nodes via separate
 * [LocalNode] views). [registerName] replicates to connected peers; [resolveName] returns
 * a local [GenServerRef] or [RemoteGenServerRef].
 */
object GlobalRegistry {
    data class MetricsSnapshot(
        val remoteRegistersApplied: Long,
        val remoteUnregistersApplied: Long,
        val staleMessagesDropped: Long,
        val snapshotsProcessed: Long,
        val syncBroadcasts: Long,
        val lastSyncBroadcastLatencyMillis: Long?,
    )

    private data class NodeView(
        val local: ConcurrentHashMap<String, GenServerRef<*>> = ConcurrentHashMap(),
        val remote: ConcurrentHashMap<String, RemoteEntry> = ConcurrentHashMap(),
        val localVersions: ConcurrentHashMap<String, Long> = ConcurrentHashMap(),
        val observedVersions: ConcurrentHashMap<String, Long> = ConcurrentHashMap(),
    )

    data class RemoteEntry(
        val homeNode: NodeId,
        val localName: String,
        val processId: OtpProcessId,
    )

    private val views = ConcurrentHashMap<NodeId, NodeView>()
    private val remoteRegistersApplied = AtomicLong(0L)
    private val remoteUnregistersApplied = AtomicLong(0L)
    private val staleMessagesDropped = AtomicLong(0L)
    private val snapshotsProcessed = AtomicLong(0L)
    private val syncBroadcasts = AtomicLong(0L)
    private val lastSyncBroadcastAtNanos = AtomicLong(0L)
    private val lastSyncLatencyMillis = AtomicLong(-1L)

    /** Single-JVM default when [install] was not called. */
    private val defaultNode = NodeId("local", "jvm")

    @Volatile
    private var transport: NodeTransport? = null

    @Volatile
    private var activeNode: NodeId? = null

    var conflictResolver: ConflictResolver = ConflictResolver.KeepFirst

    sealed class NameResolution {
        data class LocalRef<S>(val ref: GenServerRef<S>) : NameResolution()

        data class RemoteRef(val stub: RemoteGenServerRef) : NameResolution()
    }

    fun install(transport: NodeTransport, localNode: LocalNode) {
        this.transport = transport
        activeNode = localNode.id
        views.getOrPut(localNode.id) { NodeView() }
        if (transport is InMemoryTransport) {
            transport.replicationSource = localNode.id
        }
        GlobalReplicationBus.handler =
            GlobalReplicationHandler { msg, from -> onDistMessage(msg, from) }
        val previous = GlobalReplicationBus.onPeerConnected
        GlobalReplicationBus.onPeerConnected = { peer ->
            previous?.invoke(peer)
            syncPeers()
        }
    }

    /** Bind [activeNode] for resolve/register in tests (multi-node, same JVM). */
    fun useNode(localNode: LocalNode) {
        activeNode = localNode.id
        views.getOrPut(localNode.id) { NodeView() }
    }

    fun registerName(name: String, ref: GenServerRef<*>): RegisterResult {
        val view = currentView()
        val existing = view.local.putIfAbsent(name, ref)
        if (existing != null) {
            return when (val r = conflictResolver) {
                ConflictResolver.KeepFirst -> RegisterResult.Conflict(existing)
                ConflictResolver.KeepLast -> {
                    view.local[name] = ref
                    broadcastRegister(name, ref)
                    RegisterResult.Ok
                }
                is ConflictResolver.Custom -> {
                    val winner = r.resolve(name, existing, ref)
                    view.local[name] = winner
                    broadcastRegister(name, winner)
                    RegisterResult.Ok
                }
            }
        }
        ref.job.invokeOnCompletion { unregisterName(name) }
        broadcastRegister(name, ref)
        return RegisterResult.Ok
    }

    fun unregisterName(name: String) {
        val view = currentView()
        val removed = view.local.remove(name) ?: return
        broadcastUnregister(name, removed.id)
        for (v in views.values) {
            v.remote.remove(name)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <S> whereisName(name: String): GenServerRef<S>? =
        when (val r = resolveName(name)) {
            is NameResolution.LocalRef<*> -> r.ref as GenServerRef<S>
            else -> null
        }

    fun resolveName(name: String): NameResolution? {
        val view = views[currentNode()] ?: return null
        val local = view.local[name]
        if (local != null) {
            if (!local.job.isActive) {
                view.local.remove(name, local)
                return null
            }
            @Suppress("UNCHECKED_CAST")
            return NameResolution.LocalRef(local)
        }
        val remote = view.remote[name] ?: return null
        val t = transport ?: return null
        return NameResolution.RemoteRef(
            RemoteGenServerRef(remote.homeNode, remote.localName, remote.processId, t),
        )
    }

    fun registeredNames(): Set<String> {
        val view = views[currentNode()] ?: return emptySet()
        return view.local.keys + view.remote.keys
    }

    fun reset() {
        views.clear()
        transport = null
        activeNode = null
        GlobalReplicationBus.handler = null
        GlobalReplicationBus.onPeerConnected = null
        resetMetrics()
    }

    fun metricsSnapshot(): MetricsSnapshot =
        MetricsSnapshot(
            remoteRegistersApplied = remoteRegistersApplied.get(),
            remoteUnregistersApplied = remoteUnregistersApplied.get(),
            staleMessagesDropped = staleMessagesDropped.get(),
            snapshotsProcessed = snapshotsProcessed.get(),
            syncBroadcasts = syncBroadcasts.get(),
            lastSyncBroadcastLatencyMillis = lastSyncLatencyMillis.get().takeIf { it >= 0L },
        )

    fun resetMetrics() {
        remoteRegistersApplied.set(0L)
        remoteUnregistersApplied.set(0L)
        staleMessagesDropped.set(0L)
        snapshotsProcessed.set(0L)
        syncBroadcasts.set(0L)
        lastSyncBroadcastAtNanos.set(0L)
        lastSyncLatencyMillis.set(-1L)
    }

    private fun currentNode(): NodeId = activeNode ?: defaultNode

    private fun currentView(): NodeView = views.getOrPut(currentNode()) { NodeView() }

    private fun nextLocalVersion(name: String): Long {
        val view = currentView()
        val next = (view.localVersions[name] ?: 0L) + 1L
        view.localVersions[name] = next
        return next
    }

    private fun broadcastRegister(name: String, ref: GenServerRef<*>) {
        val node = currentNode()
        if (transport == null) return
        val version = nextLocalVersion(name)
        val msg =
            GlobalDistMsg.Register(
                name = name,
                homeNode = node.toWire(),
                localName = name,
                processId = ref.id.value,
                version = version,
            )
        runBlocking { transport?.broadcastGlobal(msg) }
    }

    private fun broadcastUnregister(name: String, processId: OtpProcessId) {
        val node = currentNode()
        if (transport == null) return
        val version = nextLocalVersion(name)
        val msg =
            GlobalDistMsg.Unregister(
                name = name,
                homeNode = node.toWire(),
                processId = processId.value,
                version = version,
            )
        runBlocking { transport?.broadcastGlobal(msg) }
    }

    private fun onDistMessage(msg: GlobalDistMsg, fromNode: NodeId) {
        when (msg) {
            is GlobalDistMsg.Register -> {
                if (applyRemoteRegister(msg)) {
                    remoteRegistersApplied.incrementAndGet()
                } else {
                    staleMessagesDropped.incrementAndGet()
                }
            }
            is GlobalDistMsg.Unregister -> {
                var applied = false
                for (v in views.values) {
                    val current = v.observedVersions[msg.name] ?: 0L
                    if (msg.version <= current) {
                        continue
                    }
                    v.observedVersions[msg.name] = msg.version
                    v.remote.remove(msg.name)
                    applied = true
                }
                if (applied) {
                    remoteUnregistersApplied.incrementAndGet()
                } else {
                    staleMessagesDropped.incrementAndGet()
                }
            }
            is GlobalDistMsg.SyncSnapshot -> {
                snapshotsProcessed.incrementAndGet()
                val startedAt = lastSyncBroadcastAtNanos.get()
                if (startedAt > 0L) {
                    val millis = (System.nanoTime() - startedAt) / 1_000_000L
                    lastSyncLatencyMillis.set(millis)
                }
                for (entry in msg.entries) {
                    if (applyRemoteRegister(entry)) {
                        remoteRegistersApplied.incrementAndGet()
                    } else {
                        staleMessagesDropped.incrementAndGet()
                    }
                }
            }
        }
    }

    private fun applyRemoteRegister(msg: GlobalDistMsg.Register): Boolean {
        val home = parseHome(msg.homeNode)
        val incoming =
            RemoteEntry(
                homeNode = home,
                localName = msg.localName,
                processId = OtpProcessId(msg.processId),
            )
        var applied = false
        for ((nodeId, view) in views) {
            if (nodeId == home) continue
            val currentVersion = view.observedVersions[msg.name] ?: 0L
            if (msg.version <= currentVersion) continue
            view.observedVersions[msg.name] = msg.version
            val existingLocal = view.local[msg.name]
            if (existingLocal != null) {
                when (conflictResolver) {
                    ConflictResolver.KeepFirst -> continue
                    ConflictResolver.KeepLast -> {
                        view.local.remove(msg.name)
                        view.remote[msg.name] = incoming
                    }
                    is ConflictResolver.Custom -> {
                        view.local.remove(msg.name)
                        view.remote[msg.name] = incoming
                    }
                }
            } else {
                view.remote[msg.name] = incoming
            }
            applied = true
        }
        return applied
    }

    private fun syncPeers() {
        val node = currentNode()
        if (transport == null) return
        val view = views[node] ?: return
        val entries =
            view.local.map { (name, ref) ->
                val version = view.localVersions[name] ?: 0L
                GlobalDistMsg.Register(
                    name = name,
                    homeNode = node.toWire(),
                    localName = name,
                    processId = ref.id.value,
                    version = version,
                )
            }
        if (entries.isEmpty()) return
        syncBroadcasts.incrementAndGet()
        lastSyncBroadcastAtNanos.set(System.nanoTime())
        runBlocking { transport?.broadcastGlobal(GlobalDistMsg.SyncSnapshot(entries)) }
    }

    private fun parseHome(wire: String): NodeId {
        val i = wire.lastIndexOf('@')
        return if (i <= 0) NodeId(wire) else NodeId(wire.substring(0, i), wire.substring(i + 1))
    }

    sealed class RegisterResult {
        data object Ok : RegisterResult()

        data class Conflict(val existing: GenServerRef<*>) : RegisterResult()
    }

    sealed class ConflictResolver {
        data object KeepFirst : ConflictResolver()

        data object KeepLast : ConflictResolver()

        data class Custom(
            val resolve: (name: String, existing: GenServerRef<*>, incoming: GenServerRef<*>) -> GenServerRef<*>,
        ) : ConflictResolver()
    }
}
