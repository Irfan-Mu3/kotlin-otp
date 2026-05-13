package org.otpstudy.distribution

import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServerRef
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Proxy for a remote node: routes calls through a [NodeTransport].
 *
 * `cast` is fire-and-forget; transport errors are swallowed (analogous to Erlang's unreliable
 * cast semantics over a potentially-partitioned network).
 * `call` propagates transport errors to the caller.
 *
 * [whereis] always returns null — remote process refs are opaque until discovered via a call.
 */
class RemoteNodeStub(
    override val id: NodeId,
    private val transport: NodeTransport,
) : OtpNode {
    override fun <S> whereis(name: String): GenServerRef<S>? = null

    override fun cast(name: String, message: Any) {
        runCatching { runBlocking { transport.send(id, name, message) } }
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> call(name: String, request: Any, timeout: Duration): R =
        transport.call(id, name, request, timeout) as R
}
