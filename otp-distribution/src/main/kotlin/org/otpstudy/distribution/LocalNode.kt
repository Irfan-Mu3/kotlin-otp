package org.otpstudy.distribution

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
}
