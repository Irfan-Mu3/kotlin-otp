package org.otpstudy.pg

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.InfoMsg
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Process groups — OTP's `pg` module for kotlin-otp.
 *
 * A scope is an independent namespace. Processes join named groups within a scope;
 * membership is removed automatically when the actor's Job completes.
 *
 * OTP source: lib/kernel/src/pg.erl (redesigned in OTP 23 from pg2)
 */
object Pg {
    private val scopes = ConcurrentHashMap<String, PgScope>()

    fun start(scopeName: String, parent: CoroutineScope): PgScope =
        scopes.getOrPut(scopeName) { PgScope(scopeName, parent) }

    fun scope(scopeName: String): PgScope =
        scopes[scopeName] ?: error("pg scope '$scopeName' not started")

    fun stop(scopeName: String) { scopes.remove(scopeName)?.stop() }

    fun reset() { scopes.clear() }
}

/**
 * A single `pg` scope — one namespace for group membership.
 *
 * Group membership is updated atomically via [CopyOnWriteArrayList]; reads are always
 * wait-free snapshots. Removal on actor death is wired via [kotlinx.coroutines.Job.invokeOnCompletion].
 */
class PgScope(val name: String, @Suppress("UNUSED_PARAMETER") parent: CoroutineScope) {
    private val groups = ConcurrentHashMap<String, CopyOnWriteArrayList<GenServerRef<*>>>()

    fun join(group: String, member: GenServerRef<*>) {
        groups.getOrPut(group) { CopyOnWriteArrayList() }.add(member)
        member.job.invokeOnCompletion { leave(group, member) }
    }

    fun leave(group: String, member: GenServerRef<*>) {
        groups[group]?.remove(member)
    }

    fun getMembers(group: String): List<GenServerRef<*>> =
        groups[group]?.toList() ?: emptyList()

    fun getLocalMembers(group: String): List<GenServerRef<*>> = getMembers(group)

    fun whichGroups(): List<String> = groups.keys.toList()

    /** Broadcast a cast to all current members of [group]. */
    fun broadcast(group: String, message: Any) {
        getMembers(group).forEach { it.cast(message) }
    }

    /** Broadcast an info message to all current members of [group]. */
    fun broadcastInfo(group: String, message: InfoMsg) {
        getMembers(group).forEach { it.sendInfo(message) }
    }

    fun stop() { groups.clear() }
}
