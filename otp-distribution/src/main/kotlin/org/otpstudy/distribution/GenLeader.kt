package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InfoMsg
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import java.util.concurrent.atomic.AtomicReference

internal sealed class LeaderMsg : InfoMsg {
    /** If [exclude] is set (peer went down), that node is omitted from the election set. */
    data class Elect(val exclude: NodeId? = null) : LeaderMsg()
}

interface LeaderCallbacks<S> {
    suspend fun init(): S
    suspend fun elected(state: S, leader: NodeId): S
    suspend fun surrendered(state: S, leader: NodeId): S
    suspend fun handleLeaderCall(request: Any, state: S): Pair<Any, S>
    suspend fun handleCall(request: Any, state: S): Pair<Any, S>
    suspend fun handleCast(request: Any, state: S): S
}

/**
 * Bully-style leader election (lexicographically greatest [NodeId] wire string wins).
 */
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

    private val selfRef = AtomicReference<GenServerRef<LeaderState<S>>?>(null)

    internal fun bindRef(ref: GenServerRef<LeaderState<S>>) {
        selfRef.set(ref)
    }

    private fun nodeKey(n: NodeId): String = "${n.name}@${n.host}"

    private suspend fun runElection(state: LeaderState<S>, exclude: NodeId?): NoreplyResult<LeaderState<S>> {
        val survivors = (peers + localNode).distinct().filter { exclude == null || it != exclude }
        if (survivors.isEmpty()) return NoreplyResult.Noreply(state)
        val winner = survivors.maxByOrNull(::nodeKey) ?: localNode
        val newApp =
            if (winner == localNode) {
                callbacks.elected(state.appState, localNode)
            } else {
                callbacks.surrendered(state.appState, winner)
            }
        return NoreplyResult.Noreply(
            state.copy(
                appState = newApp,
                leader = winner,
                isLeader = winner == localNode,
            ),
        )
    }

    override suspend fun init(self: GenServerRef<LeaderState<S>>): InitResult<LeaderState<S>> {
        bindRef(self)
        val s = callbacks.init()
        return InitResult.Ok(LeaderState(s, leader = null, isLeader = false))
    }

    override suspend fun handleCast(request: Any, state: LeaderState<S>): NoreplyResult<LeaderState<S>> =
        when (request) {
            is LeaderMsg.Elect -> runElection(state, exclude = request.exclude)
            else -> NoreplyResult.Noreply(state.copy(appState = callbacks.handleCast(request, state.appState)))
        }

    override suspend fun handleCall(request: Any, state: LeaderState<S>): ReplyResult<LeaderState<S>> {
        val (reply, newApp) =
            if (state.isLeader) {
                callbacks.handleLeaderCall(request, state.appState)
            } else {
                callbacks.handleCall(request, state.appState)
            }
        return ReplyResult.Reply(reply, state.copy(appState = newApp))
    }

    override suspend fun handleInfo(msg: InfoMsg, state: LeaderState<S>): NoreplyResult<LeaderState<S>> {
        if (msg is NodeEvent.NodeDown && msg.nodeId == state.leader) {
            selfRef.get()?.cast(LeaderMsg.Elect(exclude = msg.nodeId))
            return NoreplyResult.Noreply(state)
        }
        return NoreplyResult.Noreply(state)
    }
}

object GenLeaders {
    fun <S> startLink(
        scope: CoroutineScope,
        callbacks: LeaderCallbacks<S>,
        localNode: NodeId,
        peers: List<NodeId>,
        name: String? = null,
    ): GenServerRef<GenLeaderServer.LeaderState<S>> {
        val server = GenLeaderServer(callbacks, localNode, peers)
        val ref = GenServers.startLink(scope, server, name = name)
        peers.forEach { NodeMonitor.monitorNode(it, ref) }
        ref.cast(LeaderMsg.Elect(exclude = null))
        return ref
    }
}
