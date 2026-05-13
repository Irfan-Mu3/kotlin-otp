package org.otpstudy.distribution

import org.otpstudy.genserver.GenServerRef
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Represents an actor runtime — local or remote. Analogous to an Erlang node.
 *
 * [LocalNode] is the in-process implementation; [RemoteNodeStub] routes via a [NodeTransport].
 * A [NodeTransport] bridge (e.g., [InMemoryTransport] for tests, gRPC for production) decouples
 * the node abstraction from the transport protocol.
 *
 * OTP source: `lib/kernel/src/net_kernel.erl`; distribution protocol: `lib/kernel/src/dist_util.erl`
 */
interface OtpNode {
    val id: NodeId

    /** Resolve a registered name to a ref, or null if unknown or unreachable. */
    fun <S> whereis(name: String): GenServerRef<S>?

    /** Fire-and-forget cast to a named process on this node. No delivery guarantee on remote nodes. */
    fun cast(name: String, message: Any)

    /** RPC-style synchronous call to a named process. [timeout] covers the full round trip. */
    suspend fun <R> call(name: String, request: Any, timeout: Duration = 5.seconds): R
}
