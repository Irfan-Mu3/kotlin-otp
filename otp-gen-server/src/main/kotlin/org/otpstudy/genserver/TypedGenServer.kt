package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Compile-time typed façade over a [GenServer] that uses [Any] at the mailbox edge.
 */
class TypedGenServerRef<S, Req, Rep>(
    private val inner: GenServerRef<S>,
) {
    val id get() = inner.id

    val job get() = inner.job

    @Suppress("UNCHECKED_CAST")
    suspend fun call(
        request: Req,
        timeout: Duration = 5.seconds,
    ): Rep = inner.call(request as Any, timeout)

    fun cast(request: Req) {
        @Suppress("UNCHECKED_CAST")
        inner.cast(request as Any)
    }

    suspend fun stop() = inner.stop()
}

/**
 * Starts a [GenServer] and returns a typed wrapper; the [server] must only receive [Req] on call/cast.
 */
object TypedGenServers {
    fun <S, Req, Rep> startLink(
        parent: CoroutineScope,
        server: GenServer<S>,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
    ): TypedGenServerRef<S, Req, Rep> =
        TypedGenServerRef(GenServers.startLink(parent, server, context, name))
}
