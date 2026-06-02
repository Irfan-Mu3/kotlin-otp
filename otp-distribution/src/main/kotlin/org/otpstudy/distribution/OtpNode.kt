package org.otpstudy.distribution

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.MailboxFullException
import org.otpstudy.genserver.ServerDownException
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

    /**
     * Outcome-returning variant of [call]. Never throws; classifies all failure modes into a
     * closed [CallOutcome] set that is identical for local and remote nodes — the key property
     * for location transparency.
     *
     * ### Classification
     *
     * | [CallOutcome] variant   | OTP exit reason          | Triggering condition                      |
     * |-------------------------|--------------------------|-------------------------------------------|
     * | [CallOutcome.Reply]     | `{ok, R}` unwrapped      | Server replied within timeout             |
     * | [CallOutcome.Timeout]   | `exit(timeout)`          | Timeout elapsed before reply              |
     * | [CallOutcome.NoNode]    | `exit({nodedown, Node})` | Node unreachable or disconnected          |
     * | [CallOutcome.NoProcess] | `exit(noproc)`           | Name not registered on target node        |
     * | [CallOutcome.CallingSelf]| `exit(calling_self)`    | Self-call detected (deadlock guard)       |
     * | [CallOutcome.ServerDown]| `exit(Reason)`           | Server Job ended during call (any reason) |
     * | [CallOutcome.MailboxFull]| *(JVM-specific)*        | Bounded mailbox overflow                  |
     * | [CallOutcome.RemoteError]| `exit(Reason)`          | Other unclassified failure                |
     *
     * ### Structured-concurrency safety
     *
     * If the *calling* coroutine is cancelled by its parent scope (not by a call-internal timeout),
     * the [CancellationException] is re-thrown so structured cancellation propagates correctly —
     * matching how OTP `exit(timeout)` is distinct from `exit(killed)` at the process level.
     *
     * OTP source: `lib/stdlib/src/gen.erl` — `do_call/4`, `do_for_proc/1`.
     */
    suspend fun <R> callSafe(name: String, request: Any, timeout: Duration = 5.seconds): CallOutcome<R> =
        try {
            CallOutcome.Reply(call(name, request, timeout))
        } catch (e: CancellationException) {
            // Re-throw if the *parent* was cancelled; only convert a call-internal timeout.
            // OTP analogue: distinguishing exit(timeout) from exit(killed).
            currentCoroutineContext().ensureActive()
            CallOutcome.Timeout(timeout)
        } catch (e: ServerDownException) {
            CallOutcome.ServerDown(e)
        } catch (e: MailboxFullException) {
            CallOutcome.MailboxFull(name)
        } catch (e: IllegalStateException) {
            classifyIllegalState(e, name, id)
        } catch (e: Throwable) {
            CallOutcome.RemoteError(e.message ?: e.javaClass.simpleName, e)
        }

    /**
     * Outcome-returning variant of [cast].
     *
     * OTP semantics: `gen_server:cast/2` always returns `ok` — there is no acknowledgment.
     * This default implementation preserves that contract: [CastOutcome.Delivered] is returned
     * when [cast] completes without throwing, which is always the case for both [LocalNode]
     * (silent no-op if name absent) and [RemoteNodeStub] (swallows transport errors).
     *
     * [RemoteGenServerRef.castSafe] provides a richer outcome by calling [NodeTransport.send]
     * directly and observing transport-level failures, returning [CastOutcome.Dropped] when
     * the transport throws. Use that when you need best-effort delivery observability.
     *
     * **[CastOutcome.Delivered] does not imply the server processed the message.**
     */
    suspend fun castSafe(name: String, message: Any): CastOutcome =
        try {
            cast(name, message)
            CastOutcome.Delivered
        } catch (e: Throwable) {
            CastOutcome.Dropped(e.message ?: e.javaClass.simpleName)
        }
}

/**
 * Maps an [IllegalStateException] from the transport or local registry into the most specific
 * [CallOutcome] failure variant.
 *
 * Message patterns are established by [InMemoryTransport], [LocalNode], and [KotlinNodeTransport].
 * Keep in sync if those error strings change.
 *
 * OTP analogue exit reasons:
 * - "unknown node" → `exit({nodedown, Node})` (connection not established)
 * - "no process registered" → `exit(noproc)` (name not registered on target)
 * - "calling self" → `exit(calling_self)` (self-call deadlock guard)
 */
private fun classifyIllegalState(
    e: IllegalStateException,
    name: String,
    nodeId: NodeId,
): CallOutcome<Nothing> {
    val msg = e.message ?: ""
    return when {
        msg.contains("unknown node") -> CallOutcome.NoNode(nodeId)
        msg.contains("no process registered") -> CallOutcome.NoProcess(name)
        msg.contains("calling self") -> CallOutcome.CallingSelf(name)
        else -> CallOutcome.RemoteError(msg, e)
    }
}
