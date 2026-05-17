package org.otpstudy.distribution

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.OtpProcessId

/**
 * Kotlin-native stand-in for a BEAM remote pid: routes [call]/[cast] to [homeNode]'s
 * [localName] via [transport]. Not a [org.otpstudy.genserver.GenServerRef] — mailboxes
 * are JVM-local.
 */
class RemoteGenServerRef(
    val homeNode: NodeId,
    val localName: String,
    val processId: OtpProcessId,
    private val transport: NodeTransport,
) {
    val id: OtpProcessId get() = processId

    suspend fun <R> call(request: Any, timeout: Duration = 5.seconds): R {
        @Suppress("UNCHECKED_CAST")
        return transport.call(homeNode, localName, request, timeout) as R
    }

    fun cast(request: Any) {
        runBlocking { transport.send(homeNode, localName, request) }
    }
}
