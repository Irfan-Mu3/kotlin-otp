package org.otpstudy.genserver

import java.util.concurrent.ConcurrentHashMap

/**
 * Registry of pending hot code upgrades.
 *
 * At deploy time, call [schedule] with the new [GenServer] class.
 * The run loop checks this registry on each message boundary and applies any pending upgrade
 * by calling [GenServer.codeChange] to migrate state, then swapping the running implementation.
 *
 * Analogous to OTP's `release_handler` scheduling a `code_change` for a module.
 * OTP source: `lib/sasl/src/release_handler.erl`, `lib/stdlib/src/gen_server.erl` code_change/3
 */
object CodeChangeRegistry {
    private val pending = ConcurrentHashMap<String, PendingUpgrade>()

    data class PendingUpgrade(
        val newClass: Class<out GenServer<*>>,
        val oldVersion: String,
        val newVersion: String,
    )

    fun schedule(
        serverName: String,
        newClass: Class<out GenServer<*>>,
        oldVersion: String = "1.0",
        newVersion: String = "2.0",
    ) {
        pending[serverName] = PendingUpgrade(newClass, oldVersion, newVersion)
    }

    /** Consume and return a pending upgrade for [serverName], or null if none. */
    fun consumeUpgrade(serverName: String): PendingUpgrade? = pending.remove(serverName)

    fun clear() { pending.clear() }
}
