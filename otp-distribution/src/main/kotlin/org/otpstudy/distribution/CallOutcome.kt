package org.otpstudy.distribution

import kotlin.time.Duration

/**
 * Sealed outcome of a [OtpNode.callSafe] or [RemoteGenServerRef.callSafe] invocation.
 *
 * Replaces raw exception inspection at call sites with a closed set of outcomes that are
 * equivalent across local and remote nodes — the key property for location transparency.
 *
 * ### Failure taxonomy — mapping to OTP exit reasons
 *
 * OTP's `gen_server:call/2,3` **exits the calling process** (`exit(Reason)`) rather than
 * returning error tuples. `gen.erl`'s `do_call/4` classifies the low-level VM signals
 * into these canonical reasons (see `lib/stdlib/src/gen.erl`):
 *
 * | Variant        | OTP exit reason          | Origin in gen.erl / gen_server.erl          |
 * |----------------|--------------------------|---------------------------------------------|
 * | [Reply]        | `{ok, Reply}` unwrapped  | Normal `receive {Mref, Reply}`              |
 * | [Timeout]      | `exit(timeout)`          | `after Timeout` clause in `do_call/4`       |
 * | [NoNode]       | `exit({nodedown, Node})` | `DOWN` with reason `noconnection` promoted  |
 * | [NoProcess]    | `exit(noproc)`           | `whereis` returns `undefined` in `do_for_proc` |
 * | [CallingSelf]  | `exit(calling_self)`     | Guard in `do_call/4` when `Process =:= self()` |
 * | [ServerDown]   | `exit(Reason)`           | `DOWN` with any other reason (including `normal`, `shutdown`, `{shutdown, T}`) |
 * | [RemoteError]  | `exit(Reason)`           | Unclassified transport error not covered above |
 * | [MailboxFull]  | *(JVM-specific)*         | Bounded mailbox overflow — no OTP equivalent; OTP mailboxes are unbounded |
 *
 * ### JVM divergences (intentional)
 *
 * - **Outcome type instead of exit:** OTP exits the calling process; we return a sealed value
 *   so the caller decides whether to continue, log, or escalate. This avoids Kotlin
 *   exception-driven control flow for expected conditions.
 * - **[MailboxFull]:** OTP has no bounded mailbox by default; this variant is JVM-specific,
 *   added to support the `OverflowPolicy.CrashSender` mailbox bound.
 * - **[CallingSelf]:** OTP detects this via pid identity at the VM level. Kotlin detection
 *   requires explicit opt-in (see [LocalNode.callSafe] overrides) because coroutine identity
 *   is not globally available at the [OtpNode] interface level.
 *
 * OTP source: `lib/stdlib/src/gen.erl` — `do_call/4`, `do_for_proc/1`;
 *             `lib/stdlib/src/gen_server.erl` — `call/2,3`, `multi_call/4`.
 */
sealed class CallOutcome<out R> {
    /** The server replied successfully within the timeout. OTP: `{ok, Reply}` unwrapped. */
    data class Reply<R>(val value: R) : CallOutcome<R>()

    /**
     * The call did not receive a reply before [after] elapsed.
     * OTP: `exit(timeout)` from the `after Timeout` clause in `gen.erl do_call/4`.
     */
    data class Timeout(val after: Duration) : CallOutcome<Nothing>()

    /**
     * The transport has no route to [nodeId] — node is unknown or the connection was lost
     * before or during the call.
     *
     * OTP: `exit({nodedown, Node})`. The low-level VM reason is `noconnection` on the `DOWN`
     * monitor message; `gen.erl do_call/4` promotes this to `{nodedown, Node}`:
     * ```erlang
     * {'DOWN', Mref, _, _, noconnection} -> exit({nodedown, Node})
     * ```
     */
    data class NoNode(val nodeId: NodeId) : CallOutcome<Nothing>()

    /**
     * The transport reached the node but no process is registered under the requested name.
     * OTP: `exit(noproc)` from `gen.erl do_for_proc/1` when `whereis(Name)` returns `undefined`.
     *
     * Note: OTP also maps a `{nodedown, Node}` during a `global`/`via` lookup to `noproc`
     * ("node went down before global detected it"), so [NoProcess] subsumes that edge case.
     */
    data class NoProcess(val name: String) : CallOutcome<Nothing>()

    /**
     * The caller attempted to call a process that resolved to itself (would deadlock).
     * OTP: `exit(calling_self)` — a guard in `gen.erl do_call/4`:
     * ```erlang
     * do_call(Process, _Label, _Request, _Timeout) when Process =:= self() ->
     *     exit(calling_self);
     * ```
     *
     * JVM note: automatic detection requires the coroutine identity of the *caller* to be
     * matched against the registered actor's [Job]. [LocalNode] performs this check when
     * the caller's [Job] is available via [kotlinx.coroutines.currentCoroutineContext].
     * Without that context (e.g. from a plain thread) [CallingSelf] is not produced.
     */
    data class CallingSelf(val name: String) : CallOutcome<Nothing>()

    /**
     * The target server's coroutine [Job] completed (crash, `NoreplyResult.Stop`, or
     * `ReplyResult.Stop`) while the call was in flight.
     *
     * OTP: `exit(Reason)` from the `{'DOWN', Mref, _, _, Reason}` clause in `do_call/4`,
     * where `Reason` is the server's exit reason. This covers `normal`, `shutdown`,
     * `{shutdown, Term}`, and arbitrary crash reasons — OTP does **not** special-case them
     * at the `gen_server:call` layer.
     */
    data class ServerDown(val cause: Throwable?) : CallOutcome<Nothing>()

    /**
     * Any transport or handler error not covered by the specific variants above.
     * OTP analogue: `exit(Reason)` for unclassified reasons.
     */
    data class RemoteError(val message: String, val cause: Throwable? = null) : CallOutcome<Nothing>()

    /**
     * The local actor's bounded mailbox rejected the call message (overflow policy
     * [org.otpstudy.genserver.OverflowPolicy.CrashSender]).
     *
     * JVM-specific: OTP mailboxes are unbounded by default; this variant has no direct
     * OTP analogue. Back-pressure in OTP is achieved via synchronous `call` semantics
     * or explicit `{active, N}` flow control on ports.
     */
    data class MailboxFull(val name: String) : CallOutcome<Nothing>()

    // Convenience

    val isOk: Boolean get() = this is Reply
    val isFailure: Boolean get() = this !is Reply

    /** Returns the reply value or null. */
    fun valueOrNull(): R? = if (this is Reply) value else null

    /** Returns the reply or throws an [IllegalStateException] with failure description. */
    fun valueOrThrow(): R = when (this) {
        is Reply -> value
        is Timeout -> throw IllegalStateException("call timed out after $after")
        is NoNode -> throw IllegalStateException("no route to node $nodeId (nodedown)")
        is NoProcess -> throw IllegalStateException("no process registered as '$name' (noproc)")
        is CallingSelf -> throw IllegalStateException("cannot call self synchronously via '$name' (calling_self)")
        is ServerDown -> throw IllegalStateException("server went down", cause)
        is RemoteError -> throw IllegalStateException("remote error: $message", cause)
        is MailboxFull -> throw IllegalStateException("mailbox full for '$name'")
    }
}

/**
 * Outcome of a [OtpNode.castSafe] or [RemoteGenServerRef.castSafe] invocation.
 *
 * ### OTP semantics
 *
 * `gen_server:cast/2` in OTP is **truly fire-and-forget**: it sends the message and returns
 * `ok` unconditionally — no acknowledgment, no error, even if the target node is down or the
 * process does not exist. The caller never knows whether the message was delivered.
 *
 * ### JVM affordance
 *
 * [castSafe] is a JVM-specific extension that provides a *best-effort* observable result:
 * - [Delivered] — the message was dispatched to the local mailbox channel or accepted by the
 *   transport layer. This is observable on the JVM because the local channel `send` is a
 *   suspending function that can report success. There is **no OTP equivalent**.
 * - [Dropped] — a transport-level exception occurred before the message reached the target.
 *   OTP would silently swallow this.
 *
 * **[Delivered] does not imply the server processed the message** — it only confirms the
 * message entered the actor's mailbox or transport. Consistent with the principle of minimal
 * divergence, callers should treat cast as advisory and not rely on [Delivered] for
 * correctness — use `call`/`callSafe` when delivery confirmation matters.
 *
 * OTP source: `lib/stdlib/src/gen_server.erl` — `cast/2` always returns `ok`.
 */
sealed class CastOutcome {
    /**
     * Message was dispatched to the target mailbox or accepted by the transport.
     * JVM-specific affordance — no OTP equivalent.
     */
    data object Delivered : CastOutcome()

    /** Message was dropped before reaching the target due to [reason]. */
    data class Dropped(val reason: String) : CastOutcome()
}
