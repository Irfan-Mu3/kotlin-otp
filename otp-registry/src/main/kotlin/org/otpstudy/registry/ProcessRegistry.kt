package org.otpstudy.registry

import java.util.concurrent.ConcurrentHashMap
import org.otpstudy.genserver.GenServerRef

/**
 * String-keyed registry of running [GenServerRef] instances (OTP `global` / `pg`-style naming, library-only).
 */
class ProcessRegistry {
    private val byName = ConcurrentHashMap<String, GenServerRef<*>>()

    fun register(
        name: String,
        ref: GenServerRef<*>,
    ) {
        val prev = byName.putIfAbsent(name, ref)
        require(prev == null) { "name already registered: $name" }
        ref.job.invokeOnCompletion {
            unregister(name, ref)
        }
    }

    fun unregister(
        name: String,
        expected: GenServerRef<*>? = null,
    ) {
        if (expected == null) {
            byName.remove(name)
        } else {
            byName.remove(name, expected)
        }
    }

    fun lookup(name: String): GenServerRef<*>? = byName[name]

    fun names(): Set<String> = byName.keys.toSet()
}

/** Global default registry for demos and single-app use (not process-wide like Erlang). */
object GlobalProcessRegistry {
    private val impl = ProcessRegistry()

    fun register(
        name: String,
        ref: GenServerRef<*>,
    ) = impl.register(name, ref)

    fun unregister(
        name: String,
        expected: GenServerRef<*>? = null,
    ) = impl.unregister(name, expected)

    fun lookup(name: String): GenServerRef<*>? = impl.lookup(name)

    fun names(): Set<String> = impl.names()
}
