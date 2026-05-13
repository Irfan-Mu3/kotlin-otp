package org.otpstudy.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

/**
 * Analog to Erlang's `persistent_term` (OTP 21+): configuration data that is read far more
 * often than written.
 *
 * **Write cost:** O(n) — each write publishes a new snapshot and notifies watchers.
 * A [OtpLogLevel.Warn] is emitted after the first write to make the cost visible, teaching
 * the design constraint: write once at startup, read millions of times.
 *
 * **Read cost:** O(1) — single [AtomicReference.get] + map lookup; no lock.
 *
 * JVM / OTP difference: OTP embeds the value in each process's heap on first read (truly
 * zero-copy). Here reads are lock-free but still reference the shared snapshot map.
 * The semantic guarantee — value does not change under a running process — is preserved.
 *
 * OTP source: `lib/kernel/src/persistent_term.erl`
 */
object OtpPersistentTerms {
    private data class Snapshot(val map: Map<String, Any?>)

    private val snapshot = AtomicReference(Snapshot(emptyMap()))
    private val watchers = ConcurrentHashMap<String, CopyOnWriteArrayList<(String, Any?) -> Unit>>()
    private var writeCount = 0

    fun <V> put(key: String, value: V) {
        synchronized(this) {
            writeCount++
            if (writeCount > 1) {
                OtpLogging.log(
                    OtpLogLevel.Warn,
                    OtpLogContext("persistent_term"),
                    "put after init: key='$key' write #$writeCount — writes are expensive",
                )
            }
            val newMap = snapshot.get().map.toMutableMap().also { it[key] = value }
            snapshot.set(Snapshot(newMap))
        }
        watchers[key]?.forEach { cb -> cb(key, value) }
    }

    fun delete(key: String) {
        val existed: Boolean
        synchronized(this) {
            val old = snapshot.get().map
            if (!old.containsKey(key)) { existed = false; return }
            snapshot.set(Snapshot(old.toMutableMap().also { it.remove(key) }))
            existed = true
        }
        if (existed) watchers[key]?.forEach { cb -> cb(key, null) }
    }

    @Suppress("UNCHECKED_CAST")
    fun <V> get(key: String): V? = snapshot.get().map[key] as V?

    @Suppress("UNCHECKED_CAST")
    fun <V> require(key: String): V =
        snapshot.get().map[key] as V? ?: error("persistent_term '$key' not set")

    /**
     * Register [callback] to be called when [key] is updated or deleted.
     * Returns an [AutoCloseable] to unregister the watcher.
     */
    fun watch(key: String, callback: (String, Any?) -> Unit): AutoCloseable {
        val list = watchers.getOrPut(key) { CopyOnWriteArrayList() }
        list.add(callback)
        return AutoCloseable {
            list.remove(callback)
            if (list.isEmpty()) watchers.remove(key, list)
        }
    }

    /** Reset all state (useful in tests). */
    fun clear() {
        synchronized(this) {
            snapshot.set(Snapshot(emptyMap()))
            writeCount = 0
        }
    }
}
