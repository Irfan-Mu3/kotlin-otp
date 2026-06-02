package org.otpstudy.distribution

import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import org.otpstudy.genserver.GenServerRef
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Single-JVM node: all [GenServerRef]s are local and resolved by name from an in-process registry.
 *
 * Use [register]/[unregister] to make actors reachable by name on this node.
 * Analogous to Erlang's local registered processes (`erlang:register/2`), but scoped to a node
 * rather than the global process table.
 *
 * OTP source: `lib/kernel/src/net_kernel.erl` — local process registration;
 *             `lib/stdlib/src/gen.erl` — `do_for_proc/1`, `do_call/4`.
 */
class LocalNode(override val id: NodeId) : OtpNode {
    private val registry = ConcurrentHashMap<String, GenServerRef<*>>()

    fun register(name: String, ref: GenServerRef<*>) {
        registry[name] = ref
    }

    fun unregister(name: String): Boolean = registry.remove(name) != null

    @Suppress("UNCHECKED_CAST")
    override fun <S> whereis(name: String): GenServerRef<S>? = registry[name] as GenServerRef<S>?

    override fun cast(name: String, message: Any) {
        registry[name]?.cast(message)
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> call(name: String, request: Any, timeout: Duration): R {
        val ref = registry[name] ?: error("no process registered as '$name' on $id")
        return ref.call(request, timeout)
    }

    /**
     * Override of [OtpNode.callSafe] that adds [CallOutcome.CallingSelf] detection before
     * dispatching — analogous to OTP's `gen.erl do_call/4` guard:
     * ```erlang
     * do_call(Process, _, _, _) when Process =:= self() -> exit(calling_self);
     * ```
     *
     * Detection: if the calling coroutine's [Job] is the same object as the registered
     * actor's [Job], the call would deadlock (the actor cannot process its own mailbox while
     * suspended waiting for a reply). [CallOutcome.CallingSelf] is returned immediately.
     *
     * Limitation: detection only works when `currentCoroutineContext()[Job]` is the actor's
     * own job (i.e. called from inside a GenServer callback). Called from a plain thread or
     * a non-actor coroutine, the check is skipped and the call proceeds normally.
     */
    override suspend fun <R> callSafe(name: String, request: Any, timeout: Duration): CallOutcome<R> {
        val ref = registry[name]
        val callerJob = currentCoroutineContext()[Job]
        if (ref != null && callerJob != null && callerJob === ref.job) {
            return CallOutcome.CallingSelf(name)
        }
        return super.callSafe(name, request, timeout)
    }
}
