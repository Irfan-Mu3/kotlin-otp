package org.otpstudy.distribution

import kotlin.time.Duration

/**
 * Transport layer between nodes.
 *
 * For tests, use [InMemoryTransport] (routes directly between two [LocalNode]s in the same JVM).
 * For production, implement over gRPC, Aeron, or any RPC framework.
 *
 * JVM / OTP difference: OTP's distribution protocol (`dist_util.erl`) is a 10 000-line binary
 * protocol over TCP with EPMD for node discovery. This interface abstracts away that complexity;
 * jinterface (OTP's official Java library) provides the full protocol if wire compatibility
 * is needed (see docs/archive/roadmaps/THE_DEEP_END.md §3).
 */
interface NodeTransport {
    /** Send a one-way message to [targetName] on [targetNode]. Best-effort delivery. */
    suspend fun send(targetNode: NodeId, targetName: String, message: Any)

    /** RPC call to [targetName] on [targetNode]. Returns the reply or throws on timeout/error. */
    suspend fun call(targetNode: NodeId, targetName: String, request: Any, timeout: Duration): Any?

    /** Replicate [GlobalDistMsg] to connected peers (no-op unless overridden). */
    suspend fun broadcastGlobal(msg: GlobalDistMsg) {}
}
