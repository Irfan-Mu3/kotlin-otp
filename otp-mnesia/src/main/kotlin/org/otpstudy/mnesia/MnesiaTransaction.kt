package org.otpstudy.mnesia

import kotlinx.coroutines.delay
import org.otpstudy.ets.OtpTable

/**
 * Commit outcome for a [MnesiaTransaction].
 */
sealed class CommitResult {
    data object Ok : CommitResult()
    /** One of the keys read during the transaction was modified before commit. */
    data class Conflict(val key: Pair<OtpTable<*, *>, Any?>) : CommitResult()
}

class TransactionAbortedException(message: String) : Exception(message)

/**
 * MVCC transaction context over one or more [OtpTable]s.
 *
 * Reads snapshot the current value and record a version token.
 * Writes are buffered locally. Commit acquires write locks on all involved tables
 * (in identity-hash order to prevent deadlock), validates that no read key changed,
 * then applies all writes atomically.
 *
 * This teaches the Mnesia commit protocol without the distribution layer.
 *
 * OTP source: lib/mnesia/src/mnesia_tm.erl — transaction/1, commit protocol
 */
class MnesiaTransaction {
    private data class ReadRecord(val version: Long, val value: Any?)
    private data class WriteRecord(val value: Any?, val isTombstone: Boolean)

    private val reads  = mutableMapOf<Pair<OtpTable<*, *>, Any?>, ReadRecord>()
    private val writes = mutableMapOf<Pair<OtpTable<*, *>, Any?>, WriteRecord>()

    @Suppress("UNCHECKED_CAST")
    fun <K, V> read(table: OtpTable<K, V>, key: K): V? {
        val tableKey = table to key as Any?
        val buffered = writes[tableKey]
        if (buffered != null) return if (buffered.isTombstone) null else buffered.value as V?
        val existing = reads[tableKey]
        if (existing != null) return existing.value as V?
        val (value, version) = table.readWithVersion(key)
        reads[tableKey] = ReadRecord(version, value)
        return value
    }

    fun <K, V> write(table: OtpTable<K, V>, key: K, value: V) {
        @Suppress("UNCHECKED_CAST")
        writes[table to key as Any?] = WriteRecord(value, isTombstone = false)
    }

    fun <K> delete(table: OtpTable<K, *>, key: K) {
        @Suppress("UNCHECKED_CAST")
        writes[table to key as Any?] = WriteRecord(null, isTombstone = true)
    }

    fun commit(): CommitResult {
        val allKeys = (reads.keys + writes.keys)
        val tables = allKeys.map { it.first }.distinct()
            .sortedBy { System.identityHashCode(it) }

        return lockAll(tables) {
            // Validate: no read-key changed since we snapshotted it
            for ((tableKey, record) in reads) {
                val (table, key) = tableKey
                @Suppress("UNCHECKED_CAST")
                val currentVersion = (table as OtpTable<Any?, Any?>).currentVersion(key)
                if (currentVersion != record.version) return@lockAll CommitResult.Conflict(tableKey)
            }
            // Apply writes
            for ((tableKey, record) in writes) {
                val (table, key) = tableKey
                @Suppress("UNCHECKED_CAST")
                val t = table as OtpTable<Any?, Any?>
                if (!record.isTombstone && record.value != null) t.insertVersioned(key, record.value)
                else t.delete(key)
            }
            CommitResult.Ok
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> lockAll(tables: List<OtpTable<*, *>>, block: () -> T): T {
        if (tables.isEmpty()) return block()
        val head = tables.first() as OtpTable<Any?, Any?>
        return head.withWriteLock {
            lockAll(tables.drop(1), block)
        }
    }
}

/**
 * Run [block] in an MVCC transaction; retries on conflict up to [maxRetries] times.
 *
 * Analogous to `mnesia:transaction/1`.
 */
suspend fun <T> transaction(
    maxRetries: Int = 5,
    retryDelayMs: Long = 0L,
    block: MnesiaTransaction.() -> T,
): T {
    repeat(maxRetries) {
        val tx = MnesiaTransaction()
        val result = tx.block()
        when (tx.commit()) {
            CommitResult.Ok -> return result
            is CommitResult.Conflict -> { if (retryDelayMs > 0) delay(retryDelayMs) }
        }
    }
    throw TransactionAbortedException("transaction failed after $maxRetries retries")
}
