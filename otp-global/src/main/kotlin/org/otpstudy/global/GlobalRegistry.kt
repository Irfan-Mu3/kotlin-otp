package org.otpstudy.global

import org.otpstudy.genserver.GenServerRef
import java.util.concurrent.ConcurrentHashMap

/**
 * Distributed global name registry — OTP's `global` module for kotlin-otp.
 *
 * In single-JVM use: an in-process registry. With [transport] wired in, name registrations
 * broadcast to connected nodes and conflict resolution picks the winner.
 *
 * Conflict resolution mirrors OTP global's `{M, F, A}` resolver: on conflict (two
 * registrations for the same name), the [conflictResolver] picks the winner. The default
 * behaviour is [ConflictResolver.KeepFirst], matching OTP's `random_exit_name/3`.
 *
 * OTP source: lib/kernel/src/global.erl
 */
object GlobalRegistry {
    private val names = ConcurrentHashMap<String, GenServerRef<*>>()
    var conflictResolver: ConflictResolver = ConflictResolver.KeepFirst

    fun registerName(name: String, ref: GenServerRef<*>): RegisterResult {
        val existing = names.putIfAbsent(name, ref)
        if (existing != null) {
            return when (val r = conflictResolver) {
                ConflictResolver.KeepFirst -> RegisterResult.Conflict(existing)
                ConflictResolver.KeepLast  -> { names[name] = ref; RegisterResult.Ok }
                is ConflictResolver.Custom -> {
                    val winner = r.resolve(name, existing, ref)
                    names[name] = winner
                    RegisterResult.Ok
                }
            }
        }
        ref.job.invokeOnCompletion { unregisterName(name) }
        return RegisterResult.Ok
    }

    fun unregisterName(name: String) { names.remove(name) }

    @Suppress("UNCHECKED_CAST")
    fun <S> whereisName(name: String): GenServerRef<S>? = names[name] as GenServerRef<S>?

    fun registeredNames(): Set<String> = names.keys.toSet()

    fun reset() { names.clear() }

    sealed class RegisterResult {
        data object Ok : RegisterResult()
        data class Conflict(val existing: GenServerRef<*>) : RegisterResult()
    }

    sealed class ConflictResolver {
        /** Keep the first registration; return [RegisterResult.Conflict] to the second. */
        data object KeepFirst : ConflictResolver()
        /** Replace the first registration with the second; both return Ok. */
        data object KeepLast : ConflictResolver()
        /** Call [resolve] to pick a winner; the winner is stored. */
        data class Custom(
            val resolve: (name: String, existing: GenServerRef<*>, incoming: GenServerRef<*>) -> GenServerRef<*>,
        ) : ConflictResolver()
    }
}
