package org.otpstudy.distribution

import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.registry.ProcessRegistry
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.net.Socket
import java.net.SocketException
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Length-framed JSON transport between Kotlin peers (see [THE_HADAL_ZONE.md] §2).
 *
 * [ioDispatcher] controls the coroutine context for blocking socket operations.
 * Defaults to [Dispatchers.IO] — the intentional exception to the "Loom first" rule.
 * TCP frame writes (~1–3 µs blocking) are shorter than virtual-thread dispatch
 * overhead (~4–5 µs), so the `CoroutinesScheduler`-backed IO pool is ~9 µs faster
 * per roundtrip than `OtpDispatchers.IO`. All other blocking paths in this library
 * default to `OtpDispatchers.IO` (Loom). Benchmark: `docs/deployment/latency-tuning.md`.
 *
 * **Server:** [startAccepting] then [connect] from peers (or only accept inbound connections).
 * **Client:** call [startAccepting] with the same [ProcessRegistry] if you need to accept return
 * connections, or use [wire] + [connectOut] when you only dial out.
 */
class KotlinNodeTransport(
    val localNode: NodeId,
    val clusterSecret: String = "",
    port: Int = 0,
    private val ioDispatcher: kotlin.coroutines.CoroutineContext = Dispatchers.IO,
) : NodeTransport, AutoCloseable {

    private val json = DistributionWire.json
    private val serverSocket = java.net.ServerSocket(port)
    val boundPort: Int get() = serverSocket.localPort

    private val connections = ConcurrentHashMap<NodeId, KotlinDistConnection>()
    private val pendingCalls = ConcurrentHashMap<String, CompletableDeferred<kotlinx.serialization.json.JsonElement>>()
    private var appScope: CoroutineScope? = null
    private var registry: ProcessRegistry? = null
    private var acceptJob: Job? = null

    /** Associate scope + registry before [connectOut] (client-only setups). */
    fun wire(scope: CoroutineScope, registry: ProcessRegistry) {
        this.appScope = scope
        this.registry = registry
    }

    fun startAccepting(scope: CoroutineScope, registry: ProcessRegistry) {
        wire(scope, registry)
        acceptJob = scope.launch(ioDispatcher) {
            while (!serverSocket.isClosed) {
                val socket = runCatching { serverSocket.accept() }.getOrNull() ?: break
                scope.launch(ioDispatcher) {
                    runCatching { acceptInbound(socket) }
                }
            }
        }
    }

    suspend fun connectOut(
        scope: CoroutineScope,
        registry: ProcessRegistry,
        remoteNode: NodeId,
        host: String,
        port: Int,
    ) {
        wire(scope, registry)
        val socket = withContext(ioDispatcher) { Socket(host, port) }
        val conn = KotlinDistConnection(socket, json, ioDispatcher)
        val confirmed = conn.handshake(localNodeWire(), clusterSecret, initiator = true)
        check(confirmed == remoteNode) { "handshake: expected $remoteNode but got $confirmed" }
        connections[remoteNode] = conn
        NodeMonitor.notifyUp(remoteNode)
        GlobalReplicationBus.onPeerConnected?.invoke(remoteNode)
        startReceiving(checkNotNull(appScope), remoteNode, conn)
    }

    private fun localNodeWire(): String = "${localNode.name}@${localNode.host}"

    private suspend fun acceptInbound(socket: Socket) {
        val scope = checkNotNull(appScope) { "startAccepting before connections arrive" }
        val conn = KotlinDistConnection(socket, json, ioDispatcher)
        val remoteId = conn.handshake(localNodeWire(), clusterSecret, initiator = false)
        connections[remoteId] = conn
        NodeMonitor.notifyUp(remoteId)
        GlobalReplicationBus.onPeerConnected?.invoke(remoteId)
        startReceiving(scope, remoteId, conn)
    }

    private fun startReceiving(scope: CoroutineScope, remote: NodeId, conn: KotlinDistConnection) {
        scope.launch(ioDispatcher) {
            try {
                while (!conn.isClosed) {
                    // recvBlocking() avoids a redundant dispatcher hop: this coroutine is
                    // already on ioDispatcher, so using conn.recv() (which calls withContext
                    // internally) would create an unnecessary virtual thread per message.
                    val msg = conn.recvBlocking() ?: break
                    scope.launch { dispatch(remote, conn, msg) }
                }
            } finally {
                connections.remove(remote, conn)
                conn.closeSocketOnly()
                NodeMonitor.notifyDown(remote, "connection closed")
            }
        }
    }

    private suspend fun dispatch(remote: NodeId, conn: KotlinDistConnection, msg: DistMsg) {
        val reg = registry
        when (msg) {
            is DistMsg.Cast -> {
                if (reg == null) return
                reg.lookup(msg.target)?.cast(DistributionWire.decodeGenServerPayload(msg.msg))
            }
            is DistMsg.Call -> {
                if (reg == null) return
                val ref = reg.lookup(msg.target) ?: return
                scopeLaunchCall(conn, msg, ref)
            }
            is DistMsg.Reply -> pendingCalls.remove(msg.id)?.complete(msg.result)
            is DistMsg.Ping -> conn.send(DistMsg.Pong)
            is DistMsg.Hello -> { /* post-handshake only */ }
            is DistMsg.Pong -> Unit
            is DistMsg.Global -> GlobalReplicationBus.handler?.onMessage(msg.payload, remote)
        }
    }

    /** Broadcast a [GlobalDistMsg] to every connected peer. */
    override suspend fun broadcastGlobal(msg: GlobalDistMsg) {
        val frame = DistMsg.Global(msg)
        for (conn in connections.values) {
            conn.send(frame)
        }
    }

    private suspend fun scopeLaunchCall(conn: KotlinDistConnection, msg: DistMsg.Call, ref: GenServerRef<*>) {
        try {
            val req = DistributionWire.decodeGenServerPayload(msg.req)
            val result = ref.call<Any?>(req, 60.seconds)
            conn.send(DistMsg.Reply(msg.id, DistributionWire.encodePayload(result)))
        } catch (t: Throwable) {
            conn.send(DistMsg.Reply(msg.id, DistributionWire.encodePayload("error:${t.message}")))
        }
    }

    override suspend fun send(targetNode: NodeId, targetName: String, message: Any) {
        val reg = registry
        if (targetNode == localNode) {
            reg?.lookup(targetName)?.cast(message)
            return
        }
        val conn = connections[targetNode] ?: return
        conn.send(DistMsg.Cast(targetName, DistributionWire.encodePayload(message)))
    }

    override suspend fun call(targetNode: NodeId, targetName: String, request: Any, timeout: Duration): Any? {
        val reg = registry ?: error("wire/startAccepting not called")
        if (targetNode == localNode) {
            val ref = reg.lookup(targetName) ?: error("no process registered as '$targetName' on $localNode")
            return ref.call(request, timeout)
        }
        val conn = connections[targetNode] ?: throw NoSuchNodeException(targetNode)
        val id = UUID.randomUUID().toString()
        val reply = CompletableDeferred<kotlinx.serialization.json.JsonElement>()
        pendingCalls[id] = reply
        conn.send(DistMsg.Call(targetName, id, DistributionWire.encodePayload(request)))
        return withTimeout(timeout) {
            DistributionWire.decodePayload(reply.await())
        }
    }

    override fun close() {
        acceptJob?.cancel()
        connections.values.forEach { it.close() }
        serverSocket.close()
    }
}

internal class KotlinDistConnection(
    private val socket: Socket,
    private val json: kotlinx.serialization.json.Json,
    private val ioDispatcher: kotlin.coroutines.CoroutineContext = Dispatchers.IO,
) : AutoCloseable {
    private val out = DataOutputStream(socket.getOutputStream().buffered())
    private val inp = DataInputStream(socket.getInputStream().buffered())

    val isClosed: Boolean get() = socket.isClosed

    suspend fun send(msg: DistMsg) {
        withContext(ioDispatcher) {
            val bytes = json.encodeToString(DistMsg.serializer(), msg).toByteArray(Charsets.UTF_8)
            synchronized(out) {
                out.writeInt(bytes.size)
                out.write(bytes)
                out.flush()
            }
        }
    }

    suspend fun recv(): DistMsg? =
        withContext(ioDispatcher) { recvBlocking() }

    /**
     * Blocking (non-suspending) receive. Use from within a coroutine already
     * dispatched to an appropriate blocking-capable context (IO or Loom virtual thread),
     * avoiding a redundant dispatcher hop per message.
     *
     * With Loom: the virtual thread unmounts from its carrier during the blocking
     * `readInt`/`readFully` calls — this is the intended usage pattern.
     */
    internal fun recvBlocking(): DistMsg? {
        return try {
            val len = inp.readInt()
            if (len < 0 || len > 16 * 1024 * 1024) error("bad frame length $len")
            val bytes = ByteArray(len)
            inp.readFully(bytes)
            json.decodeFromString(DistMsg.serializer(), String(bytes, Charsets.UTF_8))
        } catch (_: EOFException) {
            null
        } catch (_: SocketException) {
            null
        }
    }

    suspend fun handshake(localWire: String, secret: String, initiator: Boolean): NodeId =
        withContext(ioDispatcher) {
            val myDigest = sha256Hex(secret + localWire)
            if (initiator) {
                send(DistMsg.Hello(localWire, myDigest))
                val their = recv() as? DistMsg.Hello ?: error("expected Hello")
                checkDigest(secret, their.node, their.digest)
                parseNodeId(their.node)
            } else {
                val their = recv() as? DistMsg.Hello ?: error("expected Hello")
                checkDigest(secret, their.node, their.digest)
                send(DistMsg.Hello(localWire, myDigest))
                parseNodeId(their.node)
            }
        }

    fun closeSocketOnly() {
        runCatching { socket.close() }
    }

    override fun close() {
        runCatching { socket.close() }
    }
}

private fun checkDigest(secret: String, node: String, digest: String) {
    if (secret.isEmpty()) return
    val expect = sha256Hex(secret + node)
    check(digest == expect) { "handshake digest mismatch" }
}

private fun sha256Hex(input: String): String {
    val digest = MessageDigest.getInstance("SHA-256").digest(input.toByteArray(Charsets.UTF_8))
    return digest.joinToString("") { "%02x".format(it) }
}

internal fun parseNodeId(wire: String): NodeId {
    val i = wire.lastIndexOf('@')
    return if (i <= 0) NodeId(wire) else NodeId(wire.substring(0, i), wire.substring(i + 1))
}
