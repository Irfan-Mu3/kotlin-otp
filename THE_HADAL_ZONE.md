# The Hadal Zone: Kotlin-native distribution, primitives, and production tooling

> **Status: study implementation complete; production hardening is the next phase (2026).**  
> **§1–§11 below** are the original design spec. Most behaviour is implemented — see [TRACK_B_PROGRESS.md](TRACK_B_PROGRESS.md) for module-level mapping. **Production** checklist and non-goals: **[PRODUCTION_DEEP.md](PRODUCTION_DEEP.md)** (former §12).

This is the eighth roadmap. Three ideas anchor it.

**Why Erlang/OTP still matters:** fault isolation, supervision, selective receive, distributed patterns (`abcast`, leader election, timers), and the operational mindset (`sys`, crash triage, topology visibility, profiling). Those ideas work.

**Why companies rarely standardize on it:** a comparatively small hiring pool for BEAM languages, different runtime and ops idioms, and weaker overlap with mainstream backend stacks — not because the technology is weak.

**What kotlin-otp is for:** carry the *semantics* of OTP into **Kotlin on the JVM** (coroutines, typed APIs, existing JVM observability) as a **Kotlin-only** system. This roadmap does **not** integrate with Erlang or Elixir nodes, BEAM wire protocols, or Erlang-specific crash files. OTP remains the **behavioural reference** where it helps users; the deployable runtime boundary is the JVM.

**Where this document sits now:** §1–§11 were the **design spec** for Hadal-era features. The **implementation** of that spec is largely complete (see [TRACK_B_PROGRESS.md](TRACK_B_PROGRESS.md)): TCP transport, scatter-gather, leader helper, global timer, ports, `sys` aliases, post-mortem capture, supervision tree rendering, and `ProfiledGenServer`. The prose below remains the **behavioural reference**; where module names in older paragraphs say `otp-timer` / `otp-postmortem` / `otp-gen-leader`, the shipped code may live under **`otp-gen-server`**, **`otp-observer`**, or **`otp-distribution`** instead — same responsibilities, fewer Gradle projects.

**Next phase:** [PRODUCTION_DEEP.md](PRODUCTION_DEEP.md) turns “study-quality code” into **production posture** without a big-bang rewrite: cheap checks, clear operational boundaries, and honest limits.

---

## 1. Out of scope — BEAM term codecs and Erlang cluster wire

kotlin-otp targets **Kotlin-only** clusters. **Erlang External Term Format (ETF)** and compatibility with `term_to_binary`, DETS, BEAM distribution, or **jinterface** bridges are **out of scope** for THE_HADAL_ZONE. Message payloads on the wire use **JSON** and `kotlinx.serialization` (§2). If a compact binary transport is ever needed, define a **Kotlin-internal** codec (for example CBOR or MessagePack), not ETF.

---

## 2. Kotlin-native TCP transport (`otp-distribution` extension)

### The real gap

`InMemoryTransport` runs inside a single JVM process. Moving to a real distributed system requires TCP and a **Kotlin-native** wire protocol.

Heavyweight cluster handshakes built for heterogeneous nodes and long-lived capability negotiation are unnecessary when every peer runs this library. The right default is the smallest protocol that establishes identity, authenticates the connection when a secret is configured, and routes messages to named actors. **Length-framed JSON** with `kotlinx.serialization` is that protocol.

The architecture keeps **`NodeTransport` pluggable** so alternative transports (different framing, encryption, or binary codecs) can be added without rewriting supervision or `GenServers`. The default implementation is aimed at **Kotlin-to-Kotlin** clusters.

### Wire format

```
┌─────────────────────────────────────────────────────┐
│ Frame: 4-byte big-endian payload length + JSON bytes │
│                                                       │
│ Handshake (both sides):                               │
│   → { "type": "hello", "node": "a@host",              │
│       "digest": sha256(secret + node_name) }          │
│   ← { "type": "hello", "node": "b@host",              │
│       "digest": sha256(secret + node_name) }          │
│                                                       │
│ Messages (after handshake):                           │
│   { "type": "cast",  "target": "myserver", "msg": … } │
│   { "type": "call",  "target": "myserver", "id": …,   │
│     "req": … }                                        │
│   { "type": "reply", "id": "…", "result": … }         │
│   { "type": "ping" } / { "type": "pong" }             │
└─────────────────────────────────────────────────────┘
```

### `DistMsg` frame model

```kotlin
@Serializable
sealed class DistMsg {
    @Serializable @SerialName("hello")
    data class Hello(val node: String, val digest: String) : DistMsg()

    @Serializable @SerialName("cast")
    data class Cast(val target: String, val msg: kotlinx.serialization.json.JsonElement) : DistMsg()

    @Serializable @SerialName("call")
    data class Call(val target: String, val id: String,
                    val req: kotlinx.serialization.json.JsonElement) : DistMsg()

    @Serializable @SerialName("reply")
    data class Reply(val id: String, val result: kotlinx.serialization.json.JsonElement) : DistMsg()

    @Serializable @SerialName("ping") data object Ping : DistMsg()
    @Serializable @SerialName("pong") data object Pong : DistMsg()
}
```

### `KotlinNodeTransport`

```kotlin
/**
 * Kotlin-native TCP transport for otp-distribution.
 *
 * Frames are 4-byte-length-prefixed JSON (kotlinx.serialization).
 * Handshake is a single-round identity + digest exchange (see implementation).
 * Kotlin peers only; not an OTP/Erlang distribution client.
 *
 * Plugs into NodeTransport; NodeMonitor receives nodeup/nodedown on
 * connect and disconnect.
 *
 * OTP reference (behaviour): net_kernel + dist_util — linking and routing ideas;
 * wire format is defined here.
 */
class KotlinNodeTransport(
    val localNode: NodeId,
    val clusterSecret: String = "",  // empty = unauthenticated (dev/test mode)
    port: Int = 0,
) : NodeTransport, AutoCloseable {

    private val json = Json { classDiscriminator = "type" }
    private val serverSocket = java.net.ServerSocket(port)
    val boundPort: Int get() = serverSocket.localPort

    private val connections = ConcurrentHashMap<NodeId, KotlinDistConnection>()
    private val pendingCalls = ConcurrentHashMap<String, CompletableDeferred<JsonElement>>()
    private lateinit var registry: ProcessRegistry

    fun startAccepting(scope: CoroutineScope, registry: ProcessRegistry) {
        this.registry = registry
        scope.launch(Dispatchers.IO) {
            while (!serverSocket.isClosed) {
                val socket = runCatching { serverSocket.accept() }.getOrNull() ?: break
                launch { acceptInbound(socket, scope) }
            }
        }
    }

    suspend fun connect(
        remoteNode: NodeId,
        host: String,
        port: Int,
        scope: CoroutineScope,
    ) {
        val socket = withContext(Dispatchers.IO) { java.net.Socket(host, port) }
        val conn = KotlinDistConnection(socket, json)
        val confirmed = conn.handshake(localNode, clusterSecret, initiator = true)
        check(confirmed == remoteNode) { "expected $remoteNode but connected to $confirmed" }
        connections[remoteNode] = conn
        NodeMonitor.notifyUp(remoteNode)
        conn.startReceiving(scope, remoteNode, ::dispatch)
    }

    private suspend fun acceptInbound(socket: java.net.Socket, scope: CoroutineScope) {
        val conn = KotlinDistConnection(socket, json)
        val remoteId = conn.handshake(localNode, clusterSecret, initiator = false)
        connections[remoteId] = conn
        NodeMonitor.notifyUp(remoteId)
        conn.startReceiving(scope, remoteId, ::dispatch)
    }

    override fun sendCast(node: NodeId, name: String, message: Any) {
        val conn = connections[node] ?: return
        val encoded = encodeAny(message)
        conn.send(DistMsg.Cast(name, encoded))
    }

    override suspend fun <R> sendCall(node: NodeId, name: String, request: Any): R {
        val conn = connections[node] ?: throw NoSuchNodeException(node)
        val id = java.util.UUID.randomUUID().toString()
        val reply = CompletableDeferred<JsonElement>()
        pendingCalls[id] = reply
        conn.send(DistMsg.Call(name, id, encodeAny(request)))
        @Suppress("UNCHECKED_CAST")
        return json.decodeFromJsonElement(reply.await()) as R
    }

    private fun dispatch(remote: NodeId, msg: DistMsg) {
        when (msg) {
            is DistMsg.Cast  -> registry.lookup<Any>(msg.target)?.cast(decodeAny(msg.msg))
            is DistMsg.Call  -> {
                val ref = registry.lookup<Any>(msg.target) ?: return
                // Run the call in a separate coroutine; reply goes back over the wire
                // (omitted for brevity — uses ref.call + conn.send(Reply(...)))
            }
            is DistMsg.Reply -> pendingCalls.remove(msg.id)?.complete(msg.result)
            is DistMsg.Ping  -> connections[remote]?.send(DistMsg.Pong)
            else             -> Unit
        }
    }

    override fun close() {
        connections.values.forEach { it.close() }
        serverSocket.close()
    }
}
```

### `KotlinDistConnection` — framing and handshake

```kotlin
internal class KotlinDistConnection(
    private val socket: java.net.Socket,
    private val json: Json,
) : AutoCloseable {
    private val out = java.io.DataOutputStream(socket.getOutputStream().buffered())
    private val inp = java.io.DataInputStream(socket.getInputStream().buffered())

    fun send(msg: DistMsg) {
        val bytes = json.encodeToString(DistMsg.serializer(), msg).toByteArray()
        synchronized(out) { out.writeInt(bytes.size); out.write(bytes); out.flush() }
    }

    private fun recv(): DistMsg {
        val len = inp.readInt()
        val bytes = ByteArray(len).also { inp.readFully(it) }
        return json.decodeFromString(DistMsg.serializer(), String(bytes))
    }

    suspend fun handshake(local: NodeId, secret: String, initiator: Boolean): NodeId =
        withContext(Dispatchers.IO) {
            val myDigest = sha256(secret + local.name)
            if (initiator) {
                send(DistMsg.Hello(local.name, myDigest))
                val their = recv() as? DistMsg.Hello
                    ?: error("expected Hello during handshake")
                check(their.digest == sha256(secret + their.node)) { "handshake auth failed" }
                NodeId(their.node)
            } else {
                val their = recv() as? DistMsg.Hello
                    ?: error("expected Hello during handshake")
                check(their.digest == sha256(secret + their.node)) { "handshake auth failed" }
                send(DistMsg.Hello(local.name, myDigest))
                NodeId(their.node)
            }
        }

    fun startReceiving(
        scope: CoroutineScope,
        remote: NodeId,
        dispatch: (NodeId, DistMsg) -> Unit,
    ) {
        scope.launch(Dispatchers.IO) {
            runCatching {
                while (!socket.isClosed) dispatch(remote, recv())
            }
            NodeMonitor.notifyDown(remote, "connection closed")
        }
    }

    private fun sha256(input: String): String {
        val digest = java.security.MessageDigest.getInstance("SHA-256").digest(input.toByteArray())
        return digest.joinToString("") { "%02x".format(it) }
    }

    override fun close() { socket.close() }
}
```

### What this enables vs. what it intentionally skips

This enables real **Kotlin-to-Kotlin** distribution: `pg.broadcast`, `GlobalRegistry`, `NodeMonitor`, `abcast`/`multi_call` (§3), and `gen_leader` (§4) across JVM processes. It does **not** implement OTP distribution frames or Erlang node identity tokens.

**OTP reference (behaviour):** `lib/kernel/src/dist_util.erl`, `lib/kernel/src/net_kernel.erl` — framing, handshake, and node lifecycle *ideas*; this wire format is Kotlin-specific.

---

## 3. `gen_server:abcast` and `multi_call` — scatter-gather across nodes (`otp-gen-server` extension)

### The real gap

Two multi-node patterns from OTP's `gen_server.erl` are not yet first-class in kotlin-otp (behavioural parity, not Erlang interop):

```erlang
gen_server:abcast([node1@h, node2@h], myserver, kick)
%% ↑ fire-and-forget cast to named process on multiple nodes

{Replies, BadNodes} = gen_server:multi_call([node1, node2], myserver, get_state)
%% ↑ synchronous call to all nodes; returns {successes, timeouts}
```

These are used by distributed caches, configuration broadcast, and any system that needs to scatter work and gather results. Without them, distributed actor code has to roll its own fan-out every time.

### Extension on `GenServers`

```kotlin
object GenServers {
    // ... existing startLink, startLinkSync ...

    /**
     * Async broadcast cast to [name] on all [nodes].
     * Fire-and-forget; callers are not notified of delivery failures.
     *
     * OTP analogue: gen_server:abcast/3
     */
    fun abcast(
        nodes: List<NodeId>,
        name: String,
        message: Any,
        transport: NodeTransport,
    ) {
        for (node in nodes) {
            runCatching { transport.sendCast(node, name, message) }
        }
    }

    /**
     * Synchronous scatter-gather call to [name] on all [nodes].
     * Returns [MultiCallResult] with replies received within [timeout]
     * and [noReplies] for nodes that did not respond.
     *
     * OTP analogue: gen_server:multi_call/3,4
     */
    suspend fun <R> multiCall(
        nodes: List<NodeId>,
        name: String,
        request: Any,
        transport: NodeTransport,
        timeout: Duration = 5.seconds,
    ): MultiCallResult<R> = coroutineScope {
        val deferreds = nodes.map { nodeId ->
            nodeId to async {
                runCatching {
                    withTimeout(timeout) {
                        @Suppress("UNCHECKED_CAST")
                        transport.sendCall(nodeId, name, request) as R
                    }
                }
            }
        }
        val replies   = mutableListOf<Pair<NodeId, R>>()
        val noReplies = mutableListOf<NodeId>()
        for ((nodeId, deferred) in deferreds) {
            deferred.await().fold(
                onSuccess = { replies.add(nodeId to it) },
                onFailure = { noReplies.add(nodeId) },
            )
        }
        MultiCallResult(replies, noReplies)
    }

    data class MultiCallResult<R>(
        val replies: List<Pair<NodeId, R>>,
        val noReplies: List<NodeId>,
    )
}
```

### Wiring `NodeTransport` to `GenServerRef`

`NodeTransport.sendCall` needs to route through to a local `GenServerRef` when the node is local, and through a TCP connection when remote. The routing table is keyed on `NodeId`:

```kotlin
interface NodeTransport {
    fun sendCast(node: NodeId, name: String, message: Any)
    suspend fun <R> sendCall(node: NodeId, name: String, request: Any): R
}

class RoutingNodeTransport(
    private val local: NodeId,
    private val registry: ProcessRegistry,
    /** Remote sends use the same JSON framing as [KotlinNodeTransport] (§2). */
    private val remote: NodeTransport? = null,
) : NodeTransport {

    override fun sendCast(node: NodeId, name: String, message: Any) {
        if (node == local) {
            registry.lookup<Any>(name)?.cast(message) ?: return
        } else {
            remote?.sendCast(node, name, message)
        }
    }

    override suspend fun <R> sendCall(node: NodeId, name: String, request: Any): R {
        if (node == local) {
            @Suppress("UNCHECKED_CAST")
            return registry.lookup<Any>(name)?.call(request)
                ?: throw NoSuchActorException("$name not registered on $local")
        }
        val r = remote ?: throw NoSuchNodeException(node)
        @Suppress("UNCHECKED_CAST")
        return r.sendCall(node, name, request) as R
    }
}
```

**OTP reference (behaviour):** `lib/stdlib/src/gen_server.erl` — `abcast/2,3`, `multi_call/2,3,4`, `do_multi_call/4`.

---

## 4. `gen_leader` — distributed leader election (`otp-gen-leader` module)

### The real gap

`pg` and `global` assume no single leader. Many distributed systems require exactly one process to be active at a time — the one writing to the database, the one accepting new jobs, the one holding the sequence counter. Leader election is how you get there.

OTP doesn't ship a `gen_leader` in stdlib — it was a contributed library, and OTP 25+ added leader election via `ra` (Raft-based). Our implementation uses a simpler bully variant: the node with the lexicographically highest name wins. When the leader's node goes down, `NodeMonitor.NodeDown` triggers a new election among survivors.

### Callbacks interface

```kotlin
/**
 * Callbacks for a leader-aware actor.
 *
 * [elected] is called on the new leader when it wins the election.
 * [surrendered] is called on non-leaders when a leader is established.
 * [handleLeaderCall] handles calls that arrive only at the leader.
 * [handleCall] handles calls that can arrive at any node.
 *
 * OTP analogue: gen_leader contributed library
 */
interface LeaderCallbacks<S> {
    suspend fun init(): S
    suspend fun elected(state: S, leader: NodeId): S           // became leader
    suspend fun surrendered(state: S, leader: NodeId): S       // another node won
    suspend fun handleLeaderCall(request: Any, state: S): Pair<Any, S>
    suspend fun handleCall(request: Any, state: S): Pair<Any, S>
    suspend fun handleCast(request: Any, state: S): S
}
```

### `GenLeaderServer` — the run-loop wrapper

```kotlin
class GenLeaderServer<S>(
    private val callbacks: LeaderCallbacks<S>,
    private val localNode: NodeId,
    private val peers: List<NodeId>,
) : GenServer<GenLeaderServer.LeaderState<S>> {

    data class LeaderState<S>(
        val appState: S,
        val leader: NodeId?,
        val isLeader: Boolean,
    )

    private sealed class LeaderMsg {
        data object Elect : LeaderMsg()
        data class Claim(val candidateNode: NodeId) : LeaderMsg()
    }

    override suspend fun init(): InitResult<LeaderState<S>> {
        val s = callbacks.init()
        return InitResult.Ok(LeaderState(s, leader = null, isLeader = false))
    }

    override suspend fun handleCast(
        request: Any,
        state: LeaderState<S>,
    ): NoreplyResult<LeaderState<S>> = when (request) {
        is LeaderMsg.Elect -> {
            val winner = (peers + localNode).maxByOrNull { it.name } ?: localNode
            val newApp = if (winner == localNode) {
                callbacks.elected(state.appState, localNode)
            } else {
                callbacks.surrendered(state.appState, winner)
            }
            NoreplyResult.Noreply(state.copy(appState = newApp, leader = winner, isLeader = winner == localNode))
        }
        else -> {
            val newApp = callbacks.handleCast(request, state.appState)
            NoreplyResult.Noreply(state.copy(appState = newApp))
        }
    }

    override suspend fun handleCall(
        request: Any,
        state: LeaderState<S>,
    ): ReplyResult<LeaderState<S>> {
        val (reply, newApp) = if (state.isLeader) {
            callbacks.handleLeaderCall(request, state.appState)
        } else {
            callbacks.handleCall(request, state.appState)
        }
        return ReplyResult.Reply(reply, state.copy(appState = newApp))
    }

    override suspend fun handleInfo(
        msg: InfoMsg,
        state: LeaderState<S>,
    ): NoreplyResult<LeaderState<S>> {
        if (msg is NodeEvent.NodeDown && msg.nodeId == state.leader) {
            // Leader died — re-elect from survivors
            val survivors = (peers + localNode).filter { it != msg.nodeId }
            val winner = survivors.maxByOrNull { it.name } ?: localNode
            val newApp = if (winner == localNode) {
                callbacks.elected(state.appState, localNode)
            } else {
                callbacks.surrendered(state.appState, winner)
            }
            return NoreplyResult.Noreply(state.copy(appState = newApp, leader = winner, isLeader = winner == localNode))
        }
        return NoreplyResult.Noreply(state)
    }
}

fun <S> GenServers.startLeader(
    scope: CoroutineScope,
    callbacks: LeaderCallbacks<S>,
    localNode: NodeId,
    peers: List<NodeId>,
    name: String? = null,
): GenServerRef<GenLeaderServer.LeaderState<S>> {
    val server = GenLeaderServer(callbacks, localNode, peers)
    val ref = startLink(scope, server, name = name)
    peers.forEach { NodeMonitor.monitorNode(it, ref) }
    ref.cast(GenLeaderServer.LeaderMsg.Elect)
    return ref
}
```

**OTP reference (behaviour):** gen_leader library (`https://github.com/erlware/gen_leader_revival`); OTP 25+ `ra` application for the Raft-based equivalent; `lib/kernel/src/net_kernel.erl` for `nodedown`-style delivery that triggers re-election.

---

## 5. `timer` module — centralized timer service (`otp-timer` module)

### The real gap

`OtpTimers` in `otp-gen-server` is per-actor: timers are managed inside the actor's run loop and cancelled when the actor stops. That is analogous to `erlang:send_after/3` (a BIF scoped to a process).

OTP's `timer` module is different: it is a *global gen_server* that stores pending timers. The key contract is that `timer:send_after/3` returns a `TRef` that can be passed to `timer:cancel/1` from anywhere, by anyone, even from a different process.

```kotlin
/**
 * Centralized timer service — OTP's timer module.
 *
 * Unlike OtpTimers (per-actor, lifecycle-coupled), OtpTimer is a singleton
 * service. Timers survive the creating actor's death and must be explicitly
 * cancelled.
 *
 * OTP reference: lib/stdlib/src/timer.erl — send_after/3, send_interval/3, cancel/1
 */
object OtpTimer {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val pending = ConcurrentHashMap<TimerRef, Job>()

    /**
     * Deliver [message] to [dest] after [delay].
     * Returns a [TimerRef] that can be passed to [cancel].
     * Analogous to timer:send_after/3.
     */
    fun sendAfter(delay: Duration, dest: GenServerRef<*>, message: InfoMsg): TimerRef {
        val ref = TimerRef()
        pending[ref] = scope.launch {
            kotlinx.coroutines.delay(delay)
            pending.remove(ref)
            if (dest.job.isActive) dest.sendInfo(message)
        }
        return ref
    }

    /**
     * Deliver [message] to [dest] every [interval] until cancelled.
     * Analogous to timer:send_interval/3.
     */
    fun sendInterval(interval: Duration, dest: GenServerRef<*>, message: InfoMsg): TimerRef {
        val ref = TimerRef()
        pending[ref] = scope.launch {
            while (true) {
                kotlinx.coroutines.delay(interval)
                if (!dest.job.isActive) break
                dest.sendInfo(message)
            }
            pending.remove(ref)
        }
        return ref
    }

    /**
     * Apply [block] once after [delay], without sending a message.
     * Analogous to timer:apply_after/4.
     */
    fun applyAfter(delay: Duration, block: () -> Unit): TimerRef {
        val ref = TimerRef()
        pending[ref] = scope.launch {
            kotlinx.coroutines.delay(delay)
            pending.remove(ref)
            block()
        }
        return ref
    }

    /**
     * Cancel [ref]. Returns true if the timer was still pending.
     * Analogous to timer:cancel/1.
     */
    fun cancel(ref: TimerRef): Boolean =
        pending.remove(ref)?.also { it.cancel() } != null

    /** Cancel all pending timers. For test teardown. */
    fun reset() { pending.values.forEach { it.cancel() }; pending.clear() }
}

@JvmInline value class TimerRef(val id: java.util.UUID = java.util.UUID.randomUUID())
```

### Pattern: heartbeat actor

```kotlin
class HeartbeatServer : GenServer<HeartbeatState> {
    private lateinit var timerRef: TimerRef

    override suspend fun init(): InitResult<HeartbeatState> {
        // Start a 30-second repeating heartbeat using the global timer service
        timerRef = OtpTimer.sendInterval(30.seconds, selfRef, HeartbeatTick)
        return InitResult.Ok(HeartbeatState(lastBeat = Instant.now()))
    }

    override suspend fun handleInfo(msg: InfoMsg, state: HeartbeatState): NoreplyResult<HeartbeatState> =
        when (msg) {
            is HeartbeatTick -> {
                sendHeartbeat()
                NoreplyResult.Noreply(state.copy(lastBeat = Instant.now()))
            }
            else -> NoreplyResult.Noreply(state)
        }

    override suspend fun terminate(reason: TerminateReason, state: HeartbeatState) {
        OtpTimer.cancel(timerRef)  // explicit cancel — timer outlives actor otherwise
    }
}
```

**OTP reference (behaviour):** `lib/stdlib/src/timer.erl` — `send_after/3`, `send_interval/3`, `cancel/1`, the internal `do_apply_after/5`.

---

## 6. Port drivers — bridging OS processes as actors (`otp-port` module)

### The real gap

In OTP, `open_port({spawn, "program"}, [{packet, 2}])` creates a linked port process that talks to an OS subprocess via stdin/stdout. The port sends `{Port, {data, Bytes}}` when the subprocess writes; commands go to stdin. That pattern is the **behavioural reference** for wrapping external programs.

On the JVM, `ProcessBuilder` provides the subprocess. The gap is wrapping it as a **kotlin-otp** actor: lifecycle coupled to the owner, data delivered as `InfoMsg`, and a clear API to write stdin.

```kotlin
/**
 * An OTP-style port: an OS subprocess bridged into the actor system.
 *
 * Wraps [ProcessBuilder] as an actor-compatible handle.
 * Subprocess stdout lines arrive as [PortData] InfoMsg to [owner].
 * Casting [PortCommand] sends bytes to stdin.
 * When the subprocess exits, [PortExit] is delivered to [owner].
 *
 * OTP analogue: erlang:open_port({spawn, Cmd}, Options)
 * OTP reference: erts/emulator/beam/io.c — open_port, port_command, port_close
 */
class OtpPort(
    val command: List<String>,
    val owner: GenServerRef<*>,
    scope: CoroutineScope,
) : AutoCloseable {

    private val process: Process = ProcessBuilder(command)
        .redirectErrorStream(true)
        .start()

    val isAlive: Boolean get() = process.isAlive
    val exitCode: Int?  get() = if (!process.isAlive) process.exitValue() else null

    init {
        scope.launch(Dispatchers.IO) {
            val buf = ByteArray(4096)
            val stream = process.inputStream
            while (isAlive) {
                val n = stream.read(buf)
                if (n < 0) break
                if (owner.job.isActive) owner.sendInfo(PortData(this@OtpPort, buf.copyOf(n)))
            }
            val code = process.waitFor()
            if (owner.job.isActive) owner.sendInfo(PortExit(this@OtpPort, code))
        }
    }

    /**
     * Send [bytes] to the subprocess's stdin.
     * Analogous to Port ! {self(), {command, Bytes}}.
     */
    fun command(bytes: ByteArray) {
        process.outputStream.write(bytes)
        process.outputStream.flush()
    }

    /** Send a newline-terminated [line] to stdin. */
    fun commandLine(line: String) = command((line + "\n").toByteArray())

    override fun close() { process.destroyForcibly() }
}

/** Bytes received from [port]'s subprocess stdout. */
data class PortData(val port: OtpPort, val bytes: ByteArray) : InfoMsg

/** Subprocess attached to [port] exited with [exitCode]. */
data class PortExit(val port: OtpPort, val exitCode: Int) : InfoMsg
```

### Pattern: Python worker actor

```kotlin
class PythonWorkerServer(private val scriptPath: String) : GenServer<PythonState> {
    private lateinit var port: OtpPort

    override suspend fun init(): InitResult<PythonState> {
        // port's owner (this actor) receives PortData/PortExit via handleInfo
        port = OtpPort(listOf("python3", "-u", scriptPath), selfRef, scope)
        return InitResult.Ok(PythonState(pending = emptyMap()))
    }

    override suspend fun handleCall(request: Any, state: PythonState): ReplyResult<PythonState> {
        val reply = CompletableDeferred<String>()
        val id = java.util.UUID.randomUUID().toString()
        port.commandLine("$id:${request}")
        return ReplyResult.Reply(reply, state.copy(pending = state.pending + (id to reply)))
    }

    override suspend fun handleInfo(msg: InfoMsg, state: PythonState): NoreplyResult<PythonState> =
        when (msg) {
            is PortData -> {
                val text = String(msg.bytes).trim()
                val (id, result) = text.split(":", limit = 2)
                state.pending[id]?.complete(result)
                NoreplyResult.Noreply(state.copy(pending = state.pending - id))
            }
            is PortExit -> NoreplyResult.Stop(TerminateReason.Normal, state)
            else -> NoreplyResult.Noreply(state)
        }

    override suspend fun terminate(reason: TerminateReason, state: PythonState) {
        port.close()
    }
}
```

**OTP reference (behaviour):** `erts/emulator/beam/io.c` — `open_port/2`, `erts_port_command/3`; `lib/kernel/src/hipe_unified_loader.erl` for driver patterns.

---

## 7. `sys:get_state` and `sys:replace_state` — live server surgery (`otp-gen-server` extension)

### The real gap

Our `SysMsg` handles `Suspend`, `Resume`, and `GetStatus`. OTP's `sys` module has two more operations that are missing for **behavioural parity**:

- `sys:get_state/1,2` — returns the *raw* server state (not formatted status). Production debuggers use this to see what a stuck actor holds.
- `sys:replace_state/2,3` — transforms state *without* going through `handleCall`, so operators can repair live state without a full restart.

Both bypass normal message handling and run inside the actor loop (single-writer semantics).

### Extension to `SysMsg`

```kotlin
sealed class SysMsg : InfoMsg {
    object Suspend : SysMsg()
    object Resume  : SysMsg()
    data class GetStatus(val reply: CompletableDeferred<StatusReport>) : SysMsg()

    /**
     * Return the actor's raw state.
     * Analogous to sys:get_state/1,2.
     */
    data class GetState(val reply: CompletableDeferred<Any?>) : SysMsg()

    /**
     * Apply [transform] to the actor's state; reply with the new state.
     * [transform] runs inside the actor's loop — single-writer guarantee.
     * Analogous to sys:replace_state/2,3.
     */
    data class ReplaceState(
        val transform: (Any?) -> Any?,
        val reply: CompletableDeferred<Any?>,
    ) : SysMsg()
}
```

### Extension to `GenServerRef`

```kotlin
/**
 * Retrieve the actor's current state, bypassing handleCall.
 * The state is returned as [Any?]; caller casts to [S].
 *
 * OTP analogue: sys:get_state/1,2
 */
suspend fun getState(timeout: Duration = 5.seconds): Any? {
    val reply = CompletableDeferred<Any?>()
    sendInfo(SysMsg.GetState(reply))
    return withTimeout(timeout) { reply.await() }
}

/**
 * Apply [transform] to the actor's state atomically.
 * Returns the state after transformation.
 *
 * OTP analogue: sys:replace_state/2,3
 */
suspend fun replaceState(transform: (Any?) -> Any?, timeout: Duration = 5.seconds): Any? {
    val reply = CompletableDeferred<Any?>()
    sendInfo(SysMsg.ReplaceState(transform, reply))
    return withTimeout(timeout) { reply.await() }
}
```

### Run-loop handling (addition to the `handleSysMsg` branch)

```kotlin
is SysMsg.GetState -> {
    msg.reply.complete(state)
}
is SysMsg.ReplaceState -> {
    @Suppress("UNCHECKED_CAST")
    state = msg.transform(state) as S
    msg.reply.complete(state)
}
```

### Pattern: live state repair

```kotlin
// A counter actor got corrupted state — fix it without restart:
val ref = registry.lookup<Long>("counter")!!

val current = ref.getState() as Long
println("current state: $current")  // 9999 (wrong)

val fixed = ref.replaceState { state -> (state as Long).coerceIn(0L, 100L) } as Long
println("fixed state: $fixed")      // 100
// Actor continues running; no restart; no message-queue drain needed.
```

**OTP reference (behaviour):** `lib/stdlib/src/sys.erl` — `get_state/1,2`, `replace_state/2,3`; `lib/stdlib/src/gen_server.erl` — `system_get_state/1`, `system_replace_state/2`.

---

## 8. JVM post-mortem and runtime snapshots (`otp-postmortem` module)

### The real gap

OTP ships `erl_crash.dump` and Observer’s crashdump viewer — artefacts tied to the BEAM. For a **Kotlin-only** runtime, production triage should align with **JVM** practice: thread dumps, heap dumps, structured logs, and optional **JDK Flight Recorder (JFR)** or Micrometer metrics — not parsing Erlang crash files.

kotlin-otp should still offer **first-class introspection of its own world** when things go wrong: registered actors, supervision topology, mailbox depths, and node membership at the moment of a controlled shutdown or fatal error.

### Design: `RuntimeSnapshot` export

On demand or from an uncaught handler, walk `ProcessRegistry`, `Supervisor` trees, and `NodeMonitor` state into a **versioned JSON or text report** written to a configured path (or appended to structured logs). The shape mirrors what OTP crash dumps answer for processes — names, links, queue length, reductions proxy — but uses **kotlin-otp IDs and JVM thread names**, not Erlang pids.

```kotlin
/**
 * Serializable snapshot of kotlin-otp runtime state for post-mortem analysis.
 * Written on fatal error hooks or explicit admin calls — not tied to BEAM.
 */
data class RuntimeSnapshot(
    val capturedAt: java.time.Instant,
    val jvm: JvmInfo,
    val nodes: List<NodeId>,
    val actors: List<ActorSnapshot>,
    val supervisors: List<SupervisorSnapshot>,
)

data class JvmInfo(
    val javaVersion: String,
    val pid: String?,
    val memory: Map<String, Long>,
)

data class ActorSnapshot(
    val id: OtpProcessId,
    val registeredName: String?,
    val serverClass: String,
    val mailboxLen: Int,
    val reductions: Long,
)

data class SupervisorSnapshot(
    val id: OtpProcessId,
    val strategy: RestartStrategy,
    val children: List<String>,
)

object Postmortem {
    /** Capture current runtime; caller writes to disk or ships to logs. */
    suspend fun capture(): RuntimeSnapshot { TODO("walk registry + supervisors") }

    /** Register JVM shutdown hook to write snapshot on uncaught exception path. */
    fun installFatalHook(outputDir: java.nio.file.Path) { TODO() }
}
```

### Integrations (optional layers)

- **Thread dump:** document `jcmd <pid> Thread.print` / `jstack` for operators; optional helper that triggers `Thread.getAllStackTraces()` formatting into the same report directory.
- **Heap dump:** point to `-XX:+HeapDumpOnOutOfMemoryError` and `jcmd GC.heap_dump` — no need to parse HPROF inside kotlin-otp unless you later add a thin reader.
- **JFR / metrics:** hooks that register kotlin-otp events (actor start/stop, mailbox high-water) for JDK 17+ Flight Recorder or Micrometer — same *role* as OTP’s production probes, JVM-native tooling.

**OTP reference (behaviour):** `crashdump_viewer` and `erl_crash.dump` — *questions* operators ask (who had the biggest mailbox, what was linked); **answers** here come from JVM and kotlin-otp state, not BEAM files.

---

## 9. Supervision tree visualization (`otp-supervisor` extension)

### The real gap

In OTP, Observer shows a live supervision tree. kotlin-otp needs the same **inspectability** without coupling to Erlang tooling: operators should dump the hierarchy from running code (ASCII, DOT, or JSON) the way they would from Observer.

The implementation requires two things: a `whichChildren` query on `Supervisor` (same *idea* as `supervisor:which_children/1`), and a recursive walk that assembles the tree. The rendering is pure string manipulation.

### `SupervisorTree`

```kotlin
/**
 * Walk the supervision hierarchy and render as ASCII or Graphviz DOT.
 *
 * OTP reference: lib/stdlib/src/supervisor.erl — which_children/1, count_children/1
 *                lib/observer/src/observer_proc_wx.erl — build_tree/2
 */
object SupervisorTree {

    sealed class Node {
        data class Sup(
            val name: String,
            val id: OtpProcessId,
            val strategy: RestartStrategy,
            val restartCount: Int,
            val children: List<Node>,
        ) : Node()

        data class Worker(
            val name: String,
            val id: OtpProcessId,
            val module: String,
            val restartCount: Int,
            val queueLen: Int,
        ) : Node()
    }

    suspend fun build(supervisor: SupervisorRef): Node.Sup {
        val children = supervisor.whichChildren()
        val childNodes = children.map { child ->
            when {
                child.isSupervisor -> build(child.ref as SupervisorRef)
                else -> Node.Worker(
                    name = child.id,
                    id = child.ref.id,
                    module = child.ref.serverClassName,
                    restartCount = child.restartCount,
                    queueLen = ProcessTable.info(child.ref.id)?.messageQueueLen ?: 0,
                )
            }
        }
        return Node.Sup(
            name = supervisor.name ?: supervisor.id.toString(),
            id = supervisor.id,
            strategy = supervisor.strategy,
            restartCount = supervisor.restartCount,
            children = childNodes,
        )
    }

    /** ASCII tree — suitable for logging and terminal output. */
    fun toAscii(node: Node, prefix: String = "", isLast: Boolean = true): String = buildString {
        val branch = if (isLast) "└── " else "├── "
        val cont   = if (isLast) "    " else "│   "
        when (node) {
            is Node.Sup -> {
                appendLine("$prefix$branch[SUP] ${node.name} strategy=${node.strategy} restarts=${node.restartCount}")
                node.children.forEachIndexed { i, child ->
                    append(toAscii(child, prefix + cont, i == node.children.lastIndex))
                }
            }
            is Node.Worker ->
                appendLine("$prefix$branch[WRK] ${node.name} <${node.module}> q=${node.queueLen} restarts=${node.restartCount}")
        }
    }

    /** Graphviz DOT — paste into graphviz.net or `dot -Tsvg`. */
    fun toDot(root: Node): String = buildString {
        appendLine("digraph supervision_tree {")
        appendLine("  rankdir=TB; node [fontname=\"monospace\" fontsize=10];")
        fun id(n: Node) = when (n) {
            is Node.Sup    -> "\"${n.id}\""
            is Node.Worker -> "\"${n.id}\""
        }
        fun visit(n: Node) {
            when (n) {
                is Node.Sup -> {
                    appendLine("  ${id(n)} [label=\"${n.name}\\n${n.strategy}\" shape=ellipse style=filled fillcolor=lightblue];")
                    n.children.forEach { child ->
                        appendLine("  ${id(n)} -> ${id(child)};")
                        visit(child)
                    }
                }
                is Node.Worker ->
                    appendLine("  ${id(n)} [label=\"${n.name}\\n${n.module}\" style=filled fillcolor=lightyellow];")
            }
        }
        visit(root)
        appendLine("}")
    }
}
```

### Usage

```kotlin
val tree = SupervisorTree.build(rootSupervisorRef)
println(SupervisorTree.toAscii(tree))

// Output:
// └── [SUP] root_sup strategy=OneForOne restarts=0
//     ├── [WRK] auth_server <AuthServer> q=0 restarts=0
//     ├── [WRK] order_server <OrderServer> q=3 restarts=1
//     └── [SUP] db_sup strategy=OneForAll restarts=0
//         ├── [WRK] pool_1 <DbPoolWorker> q=0 restarts=0
//         └── [WRK] pool_2 <DbPoolWorker> q=0 restarts=0
```

**OTP reference (behaviour):** `lib/stdlib/src/supervisor.erl` — `which_children/1`, `count_children/1`; `lib/observer/src/observer_proc_wx.erl` — tree construction.

---

## 10. Actor profiling — `eprof` analogue (`otp-profiler` module)

### The real gap

`eprof:profile/3` records per-function call counts and wall-clock times across a set of processes. kotlin-otp’s **Recon**-style module already gives coarse reduction snapshots; profiling goes finer: per-callback timing inside a single actor.

The key insight is that `GenServer<S>` is an interface. A `ProfiledGenServer<S>` wraps any `GenServer<S>` and instruments every callback with `System.nanoTime()` before/after. No bytecode manipulation required.

### `ProfiledGenServer`

```kotlin
/**
 * Timing and call-count wrapper for any [GenServer].
 *
 * Wraps all callbacks with nanosecond timing. Overhead: two System.nanoTime()
 * calls per callback invocation — negligible in practice.
 *
 * OTP analogue: eprof:profile/3 — per-process function profiling
 * OTP reference: lib/tools/src/eprof.erl
 */
class ProfiledGenServer<S>(private val inner: GenServer<S>) : GenServer<S> {

    data class CallStats(
        val callCount: java.util.concurrent.atomic.AtomicLong = java.util.concurrent.atomic.AtomicLong(0),
        val totalNs:   java.util.concurrent.atomic.AtomicLong = java.util.concurrent.atomic.AtomicLong(0),
        val minNs:     java.util.concurrent.atomic.AtomicLong = java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE),
        val maxNs:     java.util.concurrent.atomic.AtomicLong = java.util.concurrent.atomic.AtomicLong(0),
    ) {
        val avgNs: Long get() = if (callCount.get() == 0L) 0L else totalNs.get() / callCount.get()
    }

    private val stats = ConcurrentHashMap<String, CallStats>()

    private inline fun <R> timed(name: String, block: () -> R): R {
        val t0 = System.nanoTime()
        return try {
            block()
        } finally {
            val elapsed = System.nanoTime() - t0
            val s = stats.getOrPut(name) { CallStats() }
            s.callCount.incrementAndGet()
            s.totalNs.addAndGet(elapsed)
            s.minNs.updateAndGet { minOf(it, elapsed) }
            s.maxNs.updateAndGet { maxOf(it, elapsed) }
        }
    }

    override suspend fun init()                                      = timed("init")         { inner.init() }
    override suspend fun handleCall(req: Any, s: S)                  = timed("handleCall")   { inner.handleCall(req, s) }
    override suspend fun handleCast(req: Any, s: S)                  = timed("handleCast")   { inner.handleCast(req, s) }
    override suspend fun handleInfo(msg: InfoMsg, s: S)              = timed("handleInfo")   { inner.handleInfo(msg, s) }
    override suspend fun terminate(reason: TerminateReason, s: S)    = timed("terminate")    { inner.terminate(reason, s) }
    override suspend fun codeChange(oldV: String, newV: String, s: S)= timed("codeChange")  { inner.codeChange(oldV, newV, s) }

    data class ProfileReport(
        val actorClass: String,
        val callbacks: Map<String, CallStats>,
    ) {
        fun formatted(): String = buildString {
            appendLine("Profile: $actorClass")
            appendLine("${"callback".padEnd(16)} ${"calls".padEnd(10)} ${"total_ms".padEnd(12)} ${"avg_us".padEnd(10)} ${"min_us".padEnd(10)} max_us")
            for ((name, s) in callbacks.entries.sortedByDescending { it.value.totalNs.get() }) {
                appendLine(
                    "${name.padEnd(16)} ${s.callCount.get().toString().padEnd(10)} " +
                    "${"%.2f".format(s.totalNs.get() / 1_000_000.0).padEnd(12)} " +
                    "${"%.1f".format(s.avgNs / 1_000.0).padEnd(10)} " +
                    "${"%.1f".format((if (s.minNs.get() == Long.MAX_VALUE) 0L else s.minNs.get()) / 1_000.0).padEnd(10)} " +
                    "%.1f".format(s.maxNs.get() / 1_000.0)
                )
            }
        }
    }

    fun report(): ProfileReport = ProfileReport(
        actorClass = inner::class.qualifiedName ?: inner::class.simpleName ?: "unknown",
        callbacks  = stats.toMap(),
    )

    fun reset() = stats.clear()
}

/** Convenience: wrap any GenServer with profiling. */
fun <S> GenServer<S>.profiled(): ProfiledGenServer<S> = ProfiledGenServer(this)
```

### Usage

```kotlin
val profiled = OrderProcessor().profiled()
val ref = GenServers.startLink(scope, profiled)

// ... run workload ...

println(profiled.report().formatted())
// Profile: org.myapp.OrderProcessor
// callback         calls      total_ms     avg_us     min_us     max_us
// handleCall       48203      1204.32      24.9       8.1        3201.4
// handleCast       12041      89.22        7.4        5.2        44.3
// handleInfo       3          0.09         29.3       22.1       41.8
// init             1          12.44        12440.0    12440.0    12440.0
```

**OTP reference (behaviour):** `lib/tools/src/eprof.erl` — `profile/1,3`, `analyse/0,1`; `lib/tools/src/fprof.erl` for call-graph variant.

---

## 11. Kotlin vs TypeScript as runtime host (roadmap meta)

This document assumes **Kotlin on the JVM**. It is still worth a **one-page decision** if you ever question that host.

**When to pause and compare:** you need maximum adoption on **Node or browsers**, you expect a large greenfield rewrite, or the deployment target is not the JVM.

**Criteria:**

- **Where it must run:** JVM/Android backends favor Kotlin; serverless or edge JS favors TypeScript.
- **Semantics:** Neither host gives BEAM-style preemptive processes; both approximate with **cooperative** scheduling (coroutines vs `async`). Kotlin’s structured concurrency maps cleanly to actors and supervision; Node requires discipline for CPU-bound work.
- **Hireability:** TypeScript has a larger absolute pool; Kotlin is mainstream for Android and many JVM backends. Both beat Erlang/OTP on raw hiring breadth for typical orgs.
- **Observability:** Kotlin inherits **JFR, JDWP, Micrometer, standard JVM dumps**; TypeScript fits JS-native APM and tooling.
- **LLM assistance:** Models see more generic TS/JS in training data, so bare prompts often “feel” easier; that gap shrinks when the repo pins **Kotlin version, Gradle KTS, and coroutine style** in project rules. Treat this as a **minor** tie-breaker, not a primary technical reason to switch.
- **Cost of a TS fork:** A TypeScript port is effectively a **second product**; diluting one codebase rarely helps either.

**Recommendation:** If the mission stays “OTP ideas for JVM shops,” stay on Kotlin. If the mission becomes “OTP ideas wherever JS runs,” plan a **separate** TypeScript effort rather than entangling scopes.

---

## 12. Production readiness

Full checklist, definitions, and non-goals: **[PRODUCTION_DEEP.md](PRODUCTION_DEEP.md)**.

---

## Suggested order (historical — Track B)

The sequence below was the **implementation order** for §2–§10; it is **largely complete**. Use it to understand dependencies, not as remaining work.

```
§7  (sys:get_state/replace_state)   → Done (GenServerRef + SysMsg).
§5  (timer module)                  → Done as OtpTimer in otp-gen-server.
§10 (actor profiling)               → Done (ProfiledGenServer; JFR for prod).
§3  (abcast/multi_call)             → Done (DistributedGenServers + NodeTransport).
§9  (supervision tree)              → Done (whichChildren + SupervisorTree + nested map).
§6  (port drivers)                  → Done (OtpPort).
§8  (JVM post-mortem / snapshot)    → Done (Postmortem in otp-observer).
§2  (Kotlin TCP transport)          → Done (KotlinNodeTransport + tests).
§4  (gen_leader)                    → Done (GenLeader in otp-distribution).
```

(Section §1 documents **out-of-scope** ETF/BEAM wire. **Production** checklist: [PRODUCTION_DEEP.md](PRODUCTION_DEEP.md).)

---

## What the library becomes at the end of this roadmap

After the **spec** (§1–§11) and **Track B** implementation, kotlin-otp exposes a **Kotlin-native** runtime with OTP-shaped semantics:

- **Real TCP clustering** — `KotlinNodeTransport` provides length-framed JSON, handshake, and `NodeTransport` integration; it composes with **`NodeMonitor`**, **`DistributedGenServers`**, and (in your app) **`GlobalRegistry`** / **`pg`** where you wire them. **`NodeTransport` stays pluggable**; Erlang/BEAM distribution is out of scope.
- **Scatter-gather** — `abcast` and `multiCall` over any `NodeTransport`.
- **Leader election** — `GenLeaderServer` / `GenLeaders` for exactly-one-active patterns across nodes (behavioural analogue to `gen_leader`, not a line-by-line port).
- **Centralized timers** — `OtpTimer` in **`otp-gen-server`** (global service; distinct from per-actor `OtpTimers`).
- **External bridging** — `OtpPort` for subprocess I/O as `InfoMsg`.
- **Live surgery** — `getState` / `replaceState` (and `sys` channel) on `GenServerRef`.
- **JVM post-mortem** — `Postmortem.capture()` and related types in **`otp-observer`**, aligned with thread/heap dumps and JFR — not `erl_crash.dump`.
- **Topology visibility** — `SupervisorTree` (ASCII / DOT), including optional **nested** supervisors via an explicit `nestedSupervisors` map.
- **Profiling** — `ProfiledGenServer` for callback-level timing in dev/staging; JFR for production.

**Production bar:** shipping is not “more features” but **CI, docs, operational assumptions, and honest limits** — [PRODUCTION_DEEP.md](PRODUCTION_DEEP.md).

The JVM constraints — shared GC, coarser preemption than the BEAM — remain, as described in [LIMITATIONS.md](LIMITATIONS.md). **Erlang/Elixir node integration, ETF, and BEAM distribution are deliberate non-goals** for this roadmap.

Keep [TRACEABILITY.md](TRACEABILITY.md) and [LIMITATIONS.md](LIMITATIONS.md) updated as behaviour or limits change.
