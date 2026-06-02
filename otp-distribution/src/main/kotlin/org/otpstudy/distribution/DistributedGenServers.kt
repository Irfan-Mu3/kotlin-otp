package org.otpstudy.distribution

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
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

    /**
     * Result of a [multiCall] scatter-gather.
     *
     * ### OTP analogue
     *
     * OTP `gen_server:multi_call/4` returns `{Replies, BadNodes}` where `BadNodes` is a flat
     * list of nodes that did not reply (timed out, node down, or unreachable). We extend this
     * with [failures] — a typed list of `(NodeId, CallOutcome)` pairs — so callers can
     * distinguish [CallOutcome.Timeout] from [CallOutcome.NoNode] without catching exceptions.
     *
     * This is a deliberate improvement over OTP: `BadNodes` loses the *reason* for failure,
     * which is useful for operational decisions (retry vs alert vs ignore).
     *
     * [noReplyNodes] is preserved for backward compatibility and matches OTP's `BadNodes`
     * semantics exactly (flat list of non-replying nodes).
     *
     * OTP source: `lib/stdlib/src/gen_server.erl` — `multi_call/4`, `mc_recv/5`.
     */
    data class MultiCallResult<R>(
        /** Successful `(Node, Reply)` pairs — OTP `Replies`. */
        val replies: List<Pair<NodeId, R>>,
        /**
         * Per-node typed failure outcome for nodes that did not reply — extends OTP `BadNodes`
         * with failure classification.
         */
        val failures: List<Pair<NodeId, CallOutcome<Nothing>>>,
    ) {
        /**
         * Flat list of nodes that did not reply. OTP `BadNodes` analogue.
         * Equivalent to `failures.map { it.first }`.
         */
        val noReplyNodes: List<NodeId> get() = failures.map { it.first }
    }

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
                RemoteNodeStub(nodeId, transport).callSafe<R>(name, request, timeout)
            }
        }
        val replies = mutableListOf<Pair<NodeId, R>>()
        val failures = mutableListOf<Pair<NodeId, CallOutcome<Nothing>>>()
        for ((nodeId, d) in deferreds) {
            when (val outcome = d.await()) {
                is CallOutcome.Reply -> replies.add(nodeId to outcome.value)
                else -> {
                    @Suppress("UNCHECKED_CAST")
                    failures.add(nodeId to (outcome as CallOutcome<Nothing>))
                }
            }
        }
        OtpStudyDebug.trace { "multiCall op=$op done replies=${replies.size} noReply=${failures.size}" }
        MultiCallResult(replies, failures)
    }
}
