package org.otpstudy.distribution

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import org.otpstudy.core.OtpStudyDebug
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Scatter-gather helpers over [NodeTransport] (OTP `gen_server:abcast` / `multi_call`).
 */
object DistributedGenServers {

    suspend fun abcast(
        nodes: List<NodeId>,
        name: String,
        message: Any,
        transport: NodeTransport,
    ) {
        val op = OtpStudyDebug.newOpId()
        OtpStudyDebug.trace {
            "abcast op=$op name=$name nodes=${nodes.joinToString(",") { "$it" }}"
        }
        for (node in nodes) {
            try {
                transport.send(node, name, message)
                OtpStudyDebug.trace { "abcast op=$op send ok node=$node" }
            } catch (t: Throwable) {
                OtpStudyDebug.trace { "abcast op=$op send fail node=$node ex=${t.javaClass.simpleName}" }
                /* abcast: best-effort, OTP-style */
            }
        }
    }

    data class MultiCallResult<R>(
        val replies: List<Pair<NodeId, R>>,
        val noReplyNodes: List<NodeId>,
    )

    suspend fun <R> multiCall(
        nodes: List<NodeId>,
        name: String,
        request: Any,
        transport: NodeTransport,
        timeout: Duration = 5.seconds,
    ): MultiCallResult<R> = coroutineScope {
        val op = OtpStudyDebug.newOpId()
        OtpStudyDebug.trace {
            "multiCall op=$op name=$name nodes=${nodes.joinToString(",") { "$it" }} timeout=$timeout"
        }
        val deferreds = nodes.map { nodeId ->
            nodeId to async {
                try {
                    Result.success(
                        withTimeout(timeout) {
                            @Suppress("UNCHECKED_CAST")
                            transport.call(nodeId, name, request, timeout) as R
                        },
                    )
                } catch (t: Throwable) {
                    Result.failure(t)
                }
            }
        }
        val replies = mutableListOf<Pair<NodeId, R>>()
        val noReply = mutableListOf<NodeId>()
        for ((nodeId, d) in deferreds) {
            d.await().fold(
                onSuccess = { replies.add(nodeId to it) },
                onFailure = { noReply.add(nodeId) },
            )
        }
        OtpStudyDebug.trace { "multiCall op=$op done replies=${replies.size} noReply=${noReply.size}" }
        MultiCallResult(replies, noReply)
    }
}
