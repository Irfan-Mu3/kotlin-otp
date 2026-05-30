package org.otpstudy.distribution

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InfoMsg
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.seconds

internal sealed class LeaderMsg : InfoMsg {
    /**
     * Trigger a new election. [exclude] omits a node from the candidate set (e.g. it went down).
     * [term] must match or exceed the receiver's current term — stale elections are ignored.
     * [wonQuorum] is set by the background election coroutine to skip the proposal phase and
     * directly claim leadership after quorum was already confirmed.
     */
    data class Elect(val exclude: NodeId? = null, val term: Long = 0L, val wonQuorum: Boolean = false) : LeaderMsg()

    /**
     * Phase-1 broadcast: candidate [candidateId] proposes leadership for [term].
     * Delivered via [GenServerRef.call]; peer replies with `true` (Grant) or `false` (Reject).
     *
     * Sent as a handleCall request so the peer can process it on its own actor loop
     * without the proposer's loop being blocked — the proposer sends this from a
     * background coroutine, not from within its own actor runLoop.
     */
    data class Propose(val term: Long, val candidateId: NodeId) : LeaderMsg()
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
 * Leader election with epoch/term fencing.
 *
 * **Term fencing** (always active): each election increments a monotonic term counter.
 * Stale [LeaderMsg.Elect] messages (term < current) are discarded, preventing old
 * re-election storms from overriding a settled leader.
 *
 * **Two-phase Propose protocol** (active when [transport] is provided): rather than
 * electing locally, the bully candidate broadcasts a [LeaderMsg.Propose] to each peer
 * via transport and waits for quorum (majority) before claiming leadership. This prevents
 * split-brain: a node whose peer view diverges from reality cannot unilaterally declare
 * victory if the real peers disagree.
 *
 * The Propose fan-out runs in a background coroutine (launched on [scope]) so the actor's
 * own mailbox loop remains free to receive incoming Propose calls from other candidates
 * during the election window — avoiding actor-level deadlock.
 *
 * When [transport] is null, falls back to local bully election for backward compatibility.
 */
class GenLeaderServer<S>(
    private val callbacks: LeaderCallbacks<S>,
    private val localNode: NodeId,
    private val peers: List<NodeId>,
    private val transport: NodeTransport? = null,
    private val actorName: String? = null,
    private val electionScope: CoroutineScope? = null,
) : GenServer<GenLeaderServer.LeaderState<S>> {

    data class LeaderState<S>(
        val appState: S,
        val leader: NodeId?,
        val isLeader: Boolean,
        /** Monotonically increasing election epoch. Incremented on each election trigger. */
        val term: Long = 0L,
    )

    private val selfRef = AtomicReference<GenServerRef<LeaderState<S>>?>(null)

    internal fun bindRef(ref: GenServerRef<LeaderState<S>>) {
        selfRef.set(ref)
    }

    private fun nodeKey(n: NodeId): String = "${n.name}@${n.host}"

    /**
     * Local bully-style election (no transport). Picks the lexicographically greatest
     * node from the survivor set.
     */
    private suspend fun runLocalElection(
        state: LeaderState<S>,
        exclude: NodeId?,
        newTerm: Long,
    ): NoreplyResult<LeaderState<S>> {
        val survivors = (peers + localNode).distinct().filter { exclude == null || it != exclude }
        if (survivors.isEmpty()) return NoreplyResult.Noreply(state)
        val winner = survivors.maxByOrNull(::nodeKey) ?: localNode
        val newApp = if (winner == localNode) {
            callbacks.elected(state.appState, localNode)
        } else {
            callbacks.surrendered(state.appState, winner)
        }
        return NoreplyResult.Noreply(
            state.copy(
                appState = newApp,
                leader = winner,
                isLeader = winner == localNode,
                term = newTerm,
            ),
        )
    }

    /**
     * Kick off a distributed election without blocking the actor's mailbox loop.
     *
     * Bully determination is local (same as before). If we are the candidate, launch a
     * background coroutine that sends [LeaderMsg.Propose] to each peer via [transport]
     * and delivers the quorum result back via a [LeaderMsg.Elect] cast to self.
     *
     * If we are not the candidate, surrender immediately (no broadcast needed).
     */
    private fun startDistributedElection(
        state: LeaderState<S>,
        exclude: NodeId?,
        newTerm: Long,
    ): LeaderState<S>? {
        val t = transport ?: return null
        val name = actorName ?: return null
        val scope = electionScope ?: return null
        val self = selfRef.get() ?: return null

        val survivors = (peers + localNode).distinct().filter { exclude == null || it != exclude }
        if (survivors.isEmpty()) return null

        val candidate = survivors.maxByOrNull(::nodeKey) ?: localNode

        if (candidate != localNode) {
            // We're not the candidate — surrender immediately without broadcasting.
            return null  // caller handles surrender
        }

        // We are the candidate. Launch background fan-out so actor loop stays free.
        val clusterSize = survivors.size
        val quorum = clusterSize / 2 + 1
        scope.launch(Dispatchers.Default) {
            var grants = 1  // self-grant
            val peerSurvivors = survivors.filter { it != localNode }
            for (peer in peerSurvivors) {
                try {
                    val ack = withTimeoutOrNull(1.seconds) {
                        t.call(peer, name, LeaderMsg.Propose(newTerm, localNode), 1.seconds)
                    }
                    if (ack == true) grants++
                } catch (_: Exception) {
                    // Peer unreachable — counts as rejection.
                }
            }
            // Deliver result back to actor via a quorum-confirmed Elect.
            if (grants >= quorum) {
                self.cast(LeaderMsg.Elect(exclude = exclude, term = newTerm, wonQuorum = true))
            }
            // Lost quorum — no action; remain leaderless until quorum is achievable.
        }

        // Return the in-progress state with the new term but no settled leader yet.
        return state.copy(term = newTerm)
    }

    override suspend fun init(self: GenServerRef<LeaderState<S>>): InitResult<LeaderState<S>> {
        bindRef(self)
        val s = callbacks.init()
        return InitResult.Ok(LeaderState(s, leader = null, isLeader = false, term = 0L))
    }

    override suspend fun handleCall(request: Any, state: LeaderState<S>): ReplyResult<LeaderState<S>> {
        // Incoming Propose from a peer candidate (two-phase protocol, peer side).
        if (request is LeaderMsg.Propose) {
            return if (request.term >= state.term) {
                // Accept: term is new or equal — grant and surrender.
                val newApp = callbacks.surrendered(state.appState, request.candidateId)
                ReplyResult.Reply(
                    true,
                    state.copy(
                        appState = newApp,
                        leader = request.candidateId,
                        isLeader = false,
                        term = request.term,
                    ),
                )
            } else {
                // Stale term — reject (peer has a higher term already).
                ReplyResult.Reply(false, state)
            }
        }

        val (reply, newApp) =
            if (state.isLeader) {
                callbacks.handleLeaderCall(request, state.appState)
            } else {
                callbacks.handleCall(request, state.appState)
            }
        return ReplyResult.Reply(reply, state.copy(appState = newApp))
    }

    override suspend fun handleCast(request: Any, state: LeaderState<S>): NoreplyResult<LeaderState<S>> =
        when (request) {
            is LeaderMsg.Elect -> {
                // Ignore stale elections (term fencing).
                if (request.term < state.term) {
                    NoreplyResult.Noreply(state)
                } else {
                    val newTerm = maxOf(request.term, state.term) + 1

                    // Fast path: quorum was already confirmed by the background coroutine.
                    if (request.wonQuorum) {
                        val newApp = callbacks.elected(state.appState, localNode)
                        NoreplyResult.Noreply(
                            state.copy(appState = newApp, leader = localNode, isLeader = true, term = newTerm)
                        )
                    } else if (transport != null) {
                        // Determine candidate locally first.
                        val survivors = (peers + localNode)
                            .distinct()
                            .filter { request.exclude == null || it != request.exclude }
                        val candidate = survivors.maxByOrNull(::nodeKey) ?: localNode

                        if (candidate != localNode) {
                            // Surrender immediately — background fan-out not needed.
                            val newApp = callbacks.surrendered(state.appState, candidate)
                            NoreplyResult.Noreply(
                                state.copy(
                                    appState = newApp,
                                    leader = candidate,
                                    isLeader = false,
                                    term = newTerm,
                                ),
                            )
                        } else {
                            // We are the candidate — kick off async quorum collection.
                            val inProgress = startDistributedElection(state, request.exclude, newTerm)
                            NoreplyResult.Noreply(inProgress ?: state.copy(term = newTerm))
                        }
                    } else {
                        runLocalElection(state, request.exclude, newTerm)
                    }
                }
            }
            else -> NoreplyResult.Noreply(state.copy(appState = callbacks.handleCast(request, state.appState)))
        }

    override suspend fun handleInfo(msg: InfoMsg, state: LeaderState<S>): NoreplyResult<LeaderState<S>> {
        if (msg is NodeEvent.NodeDown && msg.nodeId == state.leader) {
            val newTerm = state.term + 1
            selfRef.get()?.cast(LeaderMsg.Elect(exclude = msg.nodeId, term = newTerm))
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
        transport: NodeTransport? = null,
    ): GenServerRef<GenLeaderServer.LeaderState<S>> {
        val server = GenLeaderServer(callbacks, localNode, peers, transport, name, scope)
        val ref = GenServers.startLink(scope, server, name = name)
        peers.forEach { NodeMonitor.monitorNode(it, ref) }
        ref.cast(LeaderMsg.Elect(exclude = null, term = 1L))
        return ref
    }
}
