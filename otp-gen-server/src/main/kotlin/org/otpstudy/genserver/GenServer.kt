package org.otpstudy.genserver

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.otpstudy.core.OtpLogContext
import org.otpstudy.core.OtpLogLevel
import org.otpstudy.core.OtpLogging
import org.otpstudy.core.OtpProcessId
import org.otpstudy.memory.ActorArena
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Internal protocol; public so [GenServerRef] does not leak forbidden visibility combinations (Kotlin 2.2+). */
sealed class GenServerMsg {
    class Call(
        val request: Any,
        val reply: CompletableDeferred<Any?>,
        /**
         * Suspending caller's [Job], captured **before** [GenServerRef.call]'s [withTimeout] block.
         * Must not be read from inside the timeout scope: that job completes when the call returns
         * and is unsuitable for long-lived borrower monitors (e.g. worker pools).
         */
        val callerJob: Job? = null,
    ) : GenServerMsg()

    data class Cast(val request: Any) : GenServerMsg()

    /**
     * Info message (typed). Implement [InfoMsg] on your own data types to use [GenServer.handleInfo].
     */
    data class Info(val msg: InfoMsg) : GenServerMsg()

    data object Stop : GenServerMsg()
}

/**
 * Opaque handle for deferred replies.
 * Returned in [ReplyResult.DeferReply]; call [reply] from any coroutine to unblock the caller.
 */
class ReplyHandle<S> internal constructor(
    internal val pending: CompletableDeferred<Any?>,
    /** Caller [Job] for in-process [GenServerRef.call]; null when unknown. */
    val callerJob: Job? = null,
) {
    /** Complete the caller's reply. Returns false if already completed (caller timed out). */
    fun reply(response: Any?): Boolean = pending.complete(response)
    /** True when a reply has already been completed (by timeout or explicit reply). */
    val isCompleted: Boolean get() = pending.isCompleted
}

/**
 * OTP gen_server-shaped callbacks. Mailbox loop runs callbacks sequentially.
 *
 * ### Typed protocols
 * Use [TypedGenServerRef] for compile-time request/reply safety.
 *
 * ### Async reply (DeferReply)
 * Override [handleCallFrom] to access the `from` handle and return [ReplyResult.DeferReply].
 * The default [handleCallFrom] delegates to [handleCall] (backward-compatible).
 *
 * ### trap_exit
 * Set [trapExit] = true. Exit signals from linked processes are delivered to [handleInfo]
 * as [ExitSignal.Exit] instead of cancelling this actor's job.
 */
interface GenServer<S> {
    /**
     * Initialises actor state. [self] is the running handle; [GenServerRef.sendInfo] during
     * init only enqueues — the mailbox loop has not started yet.
     */
    suspend fun init(self: GenServerRef<S>): InitResult<S>

    suspend fun handleCall(request: Any, state: S): ReplyResult<S>

    /**
     * Async-reply-capable variant. Default delegates to [handleCall].
     * Override to use [ReplyResult.DeferReply]: save [from] and call [ReplyHandle.reply] later.
     */
    suspend fun handleCallFrom(request: Any, state: S, from: ReplyHandle<S>): ReplyResult<S> =
        handleCall(request, state)

    suspend fun handleCast(request: Any, state: S): NoreplyResult<S>

    /** Called for [InfoMsg] deliveries (info channel, monitors, timers, control messages). */
    suspend fun handleInfo(msg: InfoMsg, state: S): NoreplyResult<S> = NoreplyResult.Noreply(state)

    suspend fun terminate(reason: TerminateReason, state: S) {}

    /** If true, EXIT signals from linked processes arrive as [ExitSignal.Exit] in [handleInfo]. */
    val trapExit: Boolean get() = false

    /**
     * Analogous to OTP `code_change/3`.
     *
     * Called on each message boundary when a hot upgrade is pending in [CodeChangeRegistry].
     * Return the migrated state compatible with [newVersion].
     * Default: state is unchanged (compatible upgrade with no schema change).
     *
     * OTP source: `lib/stdlib/src/gen_server.erl` code_change/3
     */
    suspend fun codeChange(oldVersion: String, newVersion: String, state: S): S = state
}

sealed class InitResult<out S> {
    data class Ok<S>(val state: S) : InitResult<S>()

    data class Stop(val reason: TerminateReason = TerminateReason.Normal) : InitResult<Nothing>()
}

sealed class ReplyResult<out S> {
    data class Reply<S>(val response: Any?, val newState: S) : ReplyResult<S>()

    /** Defer the reply: save [from] and call [ReplyHandle.reply] later from any coroutine. */
    data class DeferReply<S>(val from: ReplyHandle<S>, val newState: S) : ReplyResult<S>()

    data class Stop<S>(
        val response: Any?,
        val reason: TerminateReason,
        val newState: S,
    ) : ReplyResult<S>()
}

sealed class NoreplyResult<out S> {
    data class Noreply<S>(val newState: S) : NoreplyResult<S>()
    data class Stop<S>(val reason: TerminateReason, val newState: S) : NoreplyResult<S>()
    /**
     * Suspend the actor until the next message arrives, then invoke [onWake].
     *
     * Analogous to erlang:hibernate/3: the call stack is released, keeping only [newState].
     * The actor consumes no CPU until a message arrives.
     *
     * OTP source: erts/emulator/beam/erl_process.c — erts_hibernate
     */
    data class Hibernate<S>(
        val newState: S,
        val onWake: suspend (Any, S) -> NoreplyResult<S> = { _, s -> Noreply(s) },
    ) : NoreplyResult<S>()
}

class GenServerRef<S>(
    val id: OtpProcessId,
    val job: Job,
    private val mailbox: Channel<GenServerMsg>,
    private val controlMailbox: Channel<InfoMsg>,
    val sysMailbox: Channel<SysMsg>,
    private val mailboxBound: MailboxBound?,
    val isTrapExit: Boolean,
    internal val queueLen: AtomicInteger = AtomicInteger(0),
) {
    /** Child scope tied to this server's lifecycle; useful for [OtpTimers]. */
    val timerScope: CoroutineScope get() = CoroutineScope(job)

    // Completed exceptionally with ServerDownException exactly once when this server's
    // job finishes. All in-flight calls listen to it via a select clause rather than
    // each installing their own invokeOnCompletion handler — one allocation per actor
    // lifetime instead of one per call.
    // OTP source: lib/stdlib/src/gen_server.erl — do_call/4, the {'DOWN', …} receive clause.
    private val _serverDown = CompletableDeferred<Nothing>()

    init {
        job.invokeOnCompletion { cause ->
            _serverDown.completeExceptionally(
                ServerDownException(id, cause ?: CancellationException("server stopped"))
            )
        }
    }

    suspend fun <R> call(request: Any, timeout: Duration = 5.seconds): R {
        // Capture callerJob before suspension for borrower-monitoring hooks (e.g. poolboy).
        val callerJob = currentCoroutineContext()[Job]
        val reply = CompletableDeferred<Any?>()
        val msg = GenServerMsg.Call(request, reply, callerJob)
        when (mailboxBound?.policy) {
            OverflowPolicy.CrashSender -> {
                if (!mailbox.trySend(msg).isSuccess)
                    throw MailboxFullException("mailbox full for $id")
                else queueLen.incrementAndGet()
            }
            // Block policy: send suspends when mailbox is full; timeout governs the reply
            // wait below but not the send itself, which is the correct semantic for Block.
            else -> { mailbox.send(msg); queueLen.incrementAndGet() }
        }
        // select races three clauses without a withTimeout wrapper — eliminates the
        // TimeoutCoroutine child-Job + timer registration per call (~1–2 µs saved).
        // onTimeout(Duration) requires ExperimentalCoroutinesApi; CancellationException
        // is the public parent of TimeoutCancellationException with identical
        // structured-concurrency behaviour. Callers catching TimeoutCancellationException
        // specifically will see CancellationException instead — document in changelog.
        @Suppress("UNCHECKED_CAST")
        return select {
            reply.onAwait { it as R }
            _serverDown.onAwait { error("unreachable") }  // always throws ServerDownException
            onTimeout(timeout) { throw CancellationException("call to $id timed out after $timeout") }
        }
    }

    fun cast(request: Any) { trySendMsg(GenServerMsg.Cast(request)) }

    /** Send an [InfoMsg] to the main mailbox. */
    fun sendInfo(msg: InfoMsg) { trySendMsg(GenServerMsg.Info(msg)) }

    /**
     * Send an [InfoMsg] to the high-priority control channel.
     * The server drains this channel before each main-mailbox message.
     */
    fun sendControl(msg: InfoMsg) { controlMailbox.trySend(msg) }

    // sys operations

    /** Alias for [sysGetState] — matches OTP `sys:get_state/1` naming. */
    suspend fun getState(): Any? = sysGetState()

    suspend fun sysGetState(): Any? {
        val r = CompletableDeferred<Any?>()
        sysMailbox.send(SysMsg.GetState(r))
        return r.await()
    }

    suspend fun sysGetStatus(): SysStatus {
        val r = CompletableDeferred<SysStatus>()
        sysMailbox.send(SysMsg.GetStatus(r))
        return r.await()
    }

    /** Alias for [sysReplaceState] — matches OTP `sys:replace_state/2` naming. */
    suspend fun replaceState(transform: (Any?) -> Any?): Any? = sysReplaceState(transform)

    suspend fun sysReplaceState(transform: (Any?) -> Any?): Any? {
        val r = CompletableDeferred<Any?>()
        sysMailbox.send(SysMsg.ReplaceState(transform, r))
        return r.await()
    }

    suspend fun sysSuspend() {
        val r = CompletableDeferred<Unit>()
        sysMailbox.send(SysMsg.Suspend(r))
        r.await()
    }

    suspend fun sysResume() {
        val r = CompletableDeferred<Unit>()
        sysMailbox.send(SysMsg.Resume(r))
        r.await()
    }

    /**
     * Apply a hot code upgrade synchronously via the sys channel.
     *
     * Calls [GenServer.codeChange] to transform the state, then swaps the running implementation.
     * Returns after the upgrade is applied. This gives deterministic ordering:
     * the upgrade is applied before any user messages queued after this call.
     *
     * Use [CodeChangeRegistry] instead for batch deployment upgrades.
     */
    suspend fun sysCodeChange(
        newClass: Class<out GenServer<*>>,
        oldVersion: String = "1.0",
        newVersion: String = "2.0",
    ) {
        val r = CompletableDeferred<Unit>()
        sysMailbox.send(SysMsg.CodeChange(newClass, oldVersion, newVersion, r))
        r.await()
    }

    suspend fun stop() {
        mailbox.send(GenServerMsg.Stop)
        job.join()
    }

    private fun trySendMsg(msg: GenServerMsg) {
        when (mailboxBound?.policy) {
            null, OverflowPolicy.Block, OverflowPolicy.DropOldest, OverflowPolicy.DropNew -> {
                if (mailbox.trySend(msg).isSuccess) queueLen.incrementAndGet()
            }
            OverflowPolicy.CrashSender -> {
                if (!mailbox.trySend(msg).isSuccess)
                    throw MailboxFullException("mailbox full for $id")
                else queueLen.incrementAndGet()
            }
            is OverflowPolicy.DeadLetterTo -> {
                if (mailbox.trySend(msg).isSuccess) queueLen.incrementAndGet()
                else mailboxBound.policy.sink.sendInfo(DeadLetterMsg(msg, id))
            }
        }
    }
}

object GenServers {
    // Discriminates run-loop receive outcomes without boxing a wrapper object.
    // At the JVM level an unboxed LoopMsg *is* the underlying Any? reference —
    // LoopMsg.user(msg) compiles to a no-op; only sentinel comparisons add cost.
    @JvmInline
    private value class LoopMsg private constructor(private val raw: Any?) {
        val isSysHandled: Boolean   get() = raw === SYS_HANDLED
        val isChannelClosed: Boolean get() = raw === CHANNEL_CLOSED
        fun asUserMsg(): GenServerMsg = raw as GenServerMsg

        companion object {
            private val SYS_HANDLED   = Any()
            private val CHANNEL_CLOSED = Any()
            val SysHandled    = LoopMsg(SYS_HANDLED)
            val ChannelClosed = LoopMsg(CHANNEL_CLOSED)
            fun user(msg: GenServerMsg) = LoopMsg(msg)
        }
    }

    fun <S> startLink(
        parent: CoroutineScope,
        server: GenServer<S>,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
        mailboxBound: MailboxBound? = null,
        reductionLimit: Int? = null,
        withArena: Boolean = false,
        fastReply: Boolean = false,
        hibernateAfter: Duration? = null,
    ): GenServerRef<S> {
        val id = OtpProcessId.allocate()
        val mailbox: Channel<GenServerMsg> = when {
            mailboxBound == null -> Channel(Channel.UNLIMITED)
            mailboxBound.policy == OverflowPolicy.DropOldest ->
                Channel(mailboxBound.capacity, onBufferOverflow = BufferOverflow.DROP_OLDEST)
            mailboxBound.policy == OverflowPolicy.DropNew ->
                Channel(mailboxBound.capacity, onBufferOverflow = BufferOverflow.DROP_LATEST)
            else -> Channel(mailboxBound.capacity)
        }
        val controlMailbox = Channel<InfoMsg>(Channel.UNLIMITED)
        val sysMailbox = Channel<SysMsg>(Channel.UNLIMITED)
        val budget = reductionLimit?.let { ReductionBudget(it) } ?: ReductionBudget()
        val arena: ActorArena? = if (withArena) ActorArena() else null
        val arenaCtx: CoroutineContext = arena ?: EmptyCoroutineContext
        val queueLen = AtomicInteger(0)
        val jobName = name?.let { CoroutineName("gen_server:$it") } ?: CoroutineName("gen_server:$id")
        val refReady = CompletableDeferred<GenServerRef<S>>()
        val job =
            parent.launch(context + jobName + budget + arenaCtx, start = CoroutineStart.LAZY) {
                try {
                    val self = refReady.await()
                    runLoop(id, self, server, mailbox, controlMailbox, sysMailbox, name, budget, queueLen, fastReply, hibernateAfter)
                } finally {
                    coroutineContext[ActorArena]?.close()
                }
            }
        val ref = GenServerRef<S>(id, job, mailbox, controlMailbox, sysMailbox, mailboxBound, server.trapExit, queueLen)
        refReady.complete(ref)
        job.start()
        val hookReg = GenServerHooks.onActorStart?.invoke {
            ActorSnapshot(id, name, job.isActive, queueLen.get(), budget.totalReductions, server.trapExit,
                arena?.bytesAllocated ?: 0L)
        }
        if (hookReg != null) job.invokeOnCompletion { hookReg.close() }
        return ref
    }

    /**
     * Start a GenServer in a detached supervisor child scope.
     *
     * Unlike [startLink], this actor is not a structured child of [parent]'s [Job].
     * Useful in tests where `runBlocking` should not wait for long-lived actors.
     * Caller must explicitly stop the returned ref.
     */
    fun <S> startLinkDetached(
        parent: CoroutineScope,
        server: GenServer<S>,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
        mailboxBound: MailboxBound? = null,
        reductionLimit: Int? = null,
        withArena: Boolean = false,
        fastReply: Boolean = false,
        hibernateAfter: Duration? = null,
    ): GenServerRef<S> {
        val detachedParent = CoroutineScope(parent.coroutineContext.minusKey(Job) + SupervisorJob())
        return startLink(
            detachedParent,
            server,
            context,
            name,
            mailboxBound,
            reductionLimit,
            withArena,
            fastReply,
            hibernateAfter,
        )
    }

    /**
     * Start a GenServer and await its [GenServer.init] acknowledgement before returning.
     *
     * Implements `proc_lib:start_link` semantics: if [GenServer.init] returns [InitResult.Stop]
     * or throws, the caller receives [InitFailedException] rather than silently getting back
     * a reference to a dead actor.
     *
     * OTP source: lib/stdlib/src/proc_lib.erl — start_link/3
     */
    suspend fun <S> startLinkSync(
        parent: CoroutineScope,
        server: GenServer<S>,
        context: CoroutineContext = Dispatchers.Default,
        name: String? = null,
        mailboxBound: MailboxBound? = null,
        reductionLimit: Int? = null,
        withArena: Boolean = false,
        fastReply: Boolean = false,
        hibernateAfter: Duration? = null,
    ): GenServerRef<S> {
        val initAck = CompletableDeferred<Result<Unit>>()
        val wrapped = ProcLibWrapper(server, initAck)
        val ref = startLink(parent, wrapped, context, name, mailboxBound, reductionLimit, withArena, fastReply, hibernateAfter)
        initAck.await().getOrThrow()
        return ref
    }

    private class ProcLibWrapper<S>(
        private val inner: GenServer<S>,
        private val ack: CompletableDeferred<Result<Unit>>,
    ) : GenServer<S> by inner {
        override suspend fun init(self: GenServerRef<S>): InitResult<S> {
            return try {
                val result = inner.init(self)
                when (result) {
                    is InitResult.Ok   -> ack.complete(Result.success(Unit))
                    is InitResult.Stop -> ack.complete(Result.failure(InitFailedException(result.reason)))
                }
                result
            } catch (ce: CancellationException) {
                ack.complete(Result.failure(ce))
                throw ce   // always re-throw CancellationException
            } catch (t: Throwable) {
                ack.complete(Result.failure(t))
                // Do NOT re-throw: convert to Stop so runLoop exits cleanly without an uncaught
                // exception hitting the global handler. The caller gets the exception via ack.
                @Suppress("UNCHECKED_CAST")
                InitResult.Stop(TerminateReason.Failure(t)) as InitResult<S>
            }
        }
    }

    private suspend fun <S> runLoop(
        id: OtpProcessId,
        self: GenServerRef<S>,
        server: GenServer<S>,
        mailbox: Channel<GenServerMsg>,
        controlMailbox: Channel<InfoMsg>,
        sysMailbox: Channel<SysMsg>,
        actorName: String?,
        budget: ReductionBudget,
        queueLen: AtomicInteger,
        fastReply: Boolean = false,
        hibernateAfter: Duration? = null,
    ) {
        var currentServer = server
        var state: S = when (val init = currentServer.init(self)) {
            is InitResult.Ok -> init.state
            is InitResult.Stop -> {
                OtpLogging.log(OtpLogLevel.Info,
                    OtpLogContext("gen_server", id, tag = actorName), "init stopped before loop")
                return
            }
        }
        var suspended = false
        val ctx = OtpLogContext("gen_server", id, tag = actorName)
        OtpLogging.log(OtpLogLevel.Debug, ctx, "mailbox loop started")

        // Handle one sys message; returns true if the loop should stop.
        suspend fun handleOneSys(sys: SysMsg): Boolean {
            when (sys) {
                is SysMsg.GetState -> sys.reply.complete(state)
                is SysMsg.GetStatus -> sys.reply.complete(
                    SysStatus(id, actorName, currentServer::class.qualifiedName ?: "GenServer", state, queueLen.get())
                )
                is SysMsg.ReplaceState -> {
                    @Suppress("UNCHECKED_CAST")
                    state = sys.transform(state) as S
                    sys.reply.complete(state)
                }
                is SysMsg.Suspend -> {
                    suspended = true
                    sys.reply.complete(Unit)
                }
                is SysMsg.Resume -> {
                    suspended = false
                    sys.reply.complete(Unit)
                }
                is SysMsg.CodeChange -> {
                    try {
                        @Suppress("UNCHECKED_CAST")
                        val ctor = sys.newClass.getDeclaredConstructor().also { it.isAccessible = true }
                        val newInstance = ctor.newInstance() as GenServer<S>
                        state = newInstance.codeChange(sys.oldVersion, sys.newVersion, state)
                        currentServer = newInstance
                        OtpLogging.log(OtpLogLevel.Info, ctx, "sys code change applied: ${sys.newVersion}")
                        sys.reply.complete(Unit)
                    } catch (t: Throwable) {
                        if (t is CancellationException) { sys.reply.completeExceptionally(t); throw t }
                        OtpLogging.log(OtpLogLevel.Error, ctx, "sys code change failed: ${t.message}", t)
                        sys.reply.completeExceptionally(t)
                    }
                }
            }
            return false
        }

        // Drain high-priority control channel; returns true if loop should stop.
        suspend fun drainControl(): Boolean {
            while (true) {
                val ctrl = controlMailbox.tryReceive().getOrNull() ?: return false
                when (val r = currentServer.handleInfo(ctrl, state)) {
                    is NoreplyResult.Noreply<*> -> {
                        @Suppress("UNCHECKED_CAST") val ns = r.newState as S; state = ns
                    }
                    is NoreplyResult.Hibernate<*> -> {
                        // Hibernate from control channel: update state only — control msgs don't hibernate
                        @Suppress("UNCHECKED_CAST")
                        state = r.newState as S
                    }
                    is NoreplyResult.Stop<*> -> {
                        @Suppress("UNCHECKED_CAST") val ns = r.newState as S; state = ns
                        currentServer.terminate(r.reason, state)
                        return true
                    }
                }
            }
        }

        // Apply NoreplyResult; returns true if loop should stop.
        // Handles Hibernate by blocking on the next mailbox message and invoking onWake.
        suspend fun applyNoreply(r: NoreplyResult<*>): Boolean = when (r) {
            is NoreplyResult.Noreply<*> -> {
                @Suppress("UNCHECKED_CAST") val ns = r.newState as S; state = ns; false
            }
            is NoreplyResult.Stop<*> -> {
                @Suppress("UNCHECKED_CAST") val ns = r.newState as S; state = ns
                currentServer.terminate(r.reason, state); true
            }
            is NoreplyResult.Hibernate<*> -> {
                @Suppress("UNCHECKED_CAST") val h = r as NoreplyResult.Hibernate<S>
                state = h.newState
                // Block until one message arrives; no sys/control drain during hibernation.
                val wakeMsg = mailbox.receive()
                queueLen.decrementAndGet()
                when (wakeMsg) {
                    is GenServerMsg.Cast -> applyNoreply(h.onWake(wakeMsg.request, state))
                    is GenServerMsg.Info -> applyNoreply(currentServer.handleInfo(wakeMsg.msg, state))
                    GenServerMsg.Stop -> { currentServer.terminate(TerminateReason.Shutdown, state); true }
                    is GenServerMsg.Call -> {
                        val handle = ReplyHandle<S>(wakeMsg.reply, wakeMsg.callerJob)
                        try {
                            when (val cr = currentServer.handleCallFrom(wakeMsg.request, state, handle)) {
                                is ReplyResult.Reply<*> -> {
                                    if (!wakeMsg.reply.isCompleted) wakeMsg.reply.complete(cr.response)
                                    @Suppress("UNCHECKED_CAST")
                                    state = cr.newState as S
                                    false
                                }
                                is ReplyResult.DeferReply<*> -> {
                                    @Suppress("UNCHECKED_CAST")
                                    state = cr.newState as S
                                    false
                                }
                                is ReplyResult.Stop<*> -> {
                                    if (!wakeMsg.reply.isCompleted) wakeMsg.reply.complete(cr.response)
                                    @Suppress("UNCHECKED_CAST")
                                    state = cr.newState as S
                                    currentServer.terminate(cr.reason, state); true
                                }
                            }
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            if (!wakeMsg.reply.isCompleted) wakeMsg.reply.completeExceptionally(t)
                            throw t
                        }
                    }
                }
            }
        }

        try {
            outer@ while (currentCoroutineContext().isActive) {
                // 0. Check for pending hot code upgrade (analogous to BEAM module version check).
                if (actorName != null) {
                    val upgrade = CodeChangeRegistry.consumeUpgrade(actorName)
                    if (upgrade != null) {
                        try {
                            @Suppress("UNCHECKED_CAST")
                            val ctor = upgrade.newClass.getDeclaredConstructor().also { it.isAccessible = true }
                            val newInstance = ctor.newInstance() as GenServer<S>
                            state = newInstance.codeChange(upgrade.oldVersion, upgrade.newVersion, state)
                            currentServer = newInstance
                            OtpLogging.log(OtpLogLevel.Info, ctx, "hot code change applied: ${upgrade.newVersion}")
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            OtpLogging.log(OtpLogLevel.Error, ctx, "hot code change failed: ${t.message}", t)
                        }
                    }
                }

                // 1. Drain sys channel non-blocking (highest priority)
                while (true) {
                    val sys = sysMailbox.tryReceive().getOrNull() ?: break
                    handleOneSys(sys)
                }

                // 2. Drain control channel non-blocking (when not suspended)
                if (!suspended && drainControl()) break

                // 3. Block-wait on sys OR (if not suspended) the user mailbox.
                //    This ensures sys messages are always processed even when the mailbox is idle.
                //    When hibernateAfter is set, the select times out and re-enters: analogous to
                //    OTP's gen_server hibernate_after timer (erlang:hibernate/3 after idle period).
                // Full select: races sysMailbox and mailbox. Used when a sys message may
                // be pending or when hibernateAfter requires a timeout.
                suspend fun awaitMsg(): LoopMsg = select {
                    sysMailbox.onReceiveCatching { r ->
                        val sys = r.getOrNull() ?: return@onReceiveCatching LoopMsg.ChannelClosed
                        handleOneSys(sys)
                        LoopMsg.SysHandled
                    }
                    if (!suspended) {
                        mailbox.onReceiveCatching { r ->
                            val msg = r.getOrNull() ?: return@onReceiveCatching LoopMsg.ChannelClosed
                            LoopMsg.user(msg)
                        }
                    }
                }
                val loopMsg: LoopMsg = if (hibernateAfter != null) {
                    // Timeout fires when no message arrives within hibernateAfter: re-enter
                    // the blocking select indefinitely, mirroring OTP's hibernate_after semantics.
                    withTimeoutOrNull(hibernateAfter) { awaitMsg() } ?: awaitMsg()
                } else {
                    awaitMsg()
                }

                when {
                    loopMsg.isSysHandled  -> continue@outer
                    loopMsg.isChannelClosed -> break@outer
                }
                val msg = loopMsg.asUserMsg()
                queueLen.decrementAndGet()

                // 4. Process user message
                when (msg) {
                    is GenServerMsg.Call -> {
                        val handle = ReplyHandle<S>(msg.reply, msg.callerJob)
                        try {
                            when (val r = currentServer.handleCallFrom(msg.request, state, handle)) {
                                is ReplyResult.Reply<*> -> {
                                    @Suppress("UNCHECKED_CAST") val ns = r.newState as S
                                    state = ns
                                    if (!msg.reply.isCompleted) msg.reply.complete(r.response)
                                    // Cooperative yield: allows the caller's await() continuation to be
                                    // scheduled on the same dispatcher thread immediately, reducing
                                    // dispatcher round-trips from 4 to 3 on the reply path.
                                    if (fastReply) yield()
                                }
                                is ReplyResult.DeferReply<*> -> {
                                    @Suppress("UNCHECKED_CAST") val ns = r.newState as S; state = ns
                                    // caller's deferred is completed later via handle.reply()
                                }
                                is ReplyResult.Stop<*> -> {
                                    @Suppress("UNCHECKED_CAST") val ns = r.newState as S
                                    state = ns
                                    if (!msg.reply.isCompleted) msg.reply.complete(r.response)
                                    currentServer.terminate(r.reason, state)
                                    break
                                }
                            }
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            if (!msg.reply.isCompleted) msg.reply.completeExceptionally(t)
                            CrashReporting.reportCrash(
                                CrashReport(id, actorName, null, TerminateReason.Failure(t),
                                    state, 0, t.stackTrace.toList(), emptyList(), Instant.now())
                            )
                            throw t
                        }
                    }
                    is GenServerMsg.Cast -> if (applyNoreply(currentServer.handleCast(msg.request, state))) break
                    is GenServerMsg.Info -> if (applyNoreply(currentServer.handleInfo(msg.msg, state))) break
                    GenServerMsg.Stop -> {
                        currentServer.terminate(TerminateReason.Shutdown, state)
                        break
                    }
                }

                // 5. Burn one reduction per message — cooperative scheduler fairness.
                budget.reduce(1)
            }
        } catch (ce: CancellationException) {
            val reason = if (ce.message?.contains("brutal_kill", ignoreCase = true) == true)
                TerminateReason.BrutalKill else TerminateReason.Shutdown
            OtpLogging.log(OtpLogLevel.Debug, ctx, "mailbox loop cancelled: $reason")
            runCatching { currentServer.terminate(reason, state) }
            throw ce
        } catch (t: Throwable) {
            OtpLogging.log(OtpLogLevel.Error, ctx, "mailbox loop failed", t)
            val reason = TerminateReason.Failure(t)
            CrashReporting.reportCrash(
                CrashReport(id, actorName, null, reason, state, 0,
                    t.stackTrace.toList(), emptyList(), Instant.now())
            )
            currentServer.terminate(reason, state)
        }
    }
}

/**
 * Thrown by [GenServers.startLinkSync] when [GenServer.init] returns [InitResult.Stop].
 *
 * Mirrors `proc_lib:start_link` behaviour: the parent receives an error rather than
 * silently getting a reference to a dead actor.
 *
 * OTP source: lib/stdlib/src/proc_lib.erl — init_ack/2
 */
class InitFailedException(val reason: TerminateReason) : Exception("GenServer init failed: $reason")

/**
 * Thrown by [GenServerRef.call] when the server process exits before it could reply.
 *
 * Analogous to receiving `{'DOWN', Ref, process, Pid, Reason}` in OTP's gen_server:call/3
 * when the server crashes or stops during the call window.
 *
 * OTP source: lib/stdlib/src/gen_server.erl — do_call/4, the {'DOWN',...} receive clause
 */
class ServerDownException(val pid: OtpProcessId, cause: Throwable) :
    Exception("server $pid exited during call", cause)
