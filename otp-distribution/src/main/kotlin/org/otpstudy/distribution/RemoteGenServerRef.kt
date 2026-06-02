package org.otpstudy.distribution

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.OtpProcessId
import org.otpstudy.genserver.ServerDownException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

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

    /**
     * Outcome-returning variant of [call]. Applies the same failure taxonomy as
     * [OtpNode.callSafe] so callers treating a [RemoteGenServerRef] and an [OtpNode] stub
     * interchangeably see identical outcome types — location transparency at the ref level.
     */
    suspend fun <R> callSafe(request: Any, timeout: Duration = 5.seconds): CallOutcome<R> =
        try {
            CallOutcome.Reply(call(request, timeout))
        } catch (e: CancellationException) {
            currentCoroutineContext().ensureActive()
            CallOutcome.Timeout(timeout)
        } catch (e: ServerDownException) {
            CallOutcome.ServerDown(e)
        } catch (e: IllegalStateException) {
            val msg = e.message ?: ""
            when {
                msg.contains("unknown node") -> CallOutcome.NoNode(homeNode)
                msg.contains("no process registered") -> CallOutcome.NoProcess(localName)
                else -> CallOutcome.RemoteError(msg, e)
            }
        } catch (e: Throwable) {
            CallOutcome.RemoteError(e.message ?: e.javaClass.simpleName, e)
        }

    fun cast(request: Any) {
        runBlocking { transport.send(homeNode, localName, request) }
    }

    /**
     * Outcome-returning variant of [cast]. Returns [CastOutcome.Delivered] when the transport
     * accepted the message, [CastOutcome.Dropped] on any transport failure.
     */
    suspend fun castSafe(request: Any): CastOutcome =
        try {
            transport.send(homeNode, localName, request)
            CastOutcome.Delivered
        } catch (e: Throwable) {
            CastOutcome.Dropped(e.message ?: e.javaClass.simpleName)
        }
}
