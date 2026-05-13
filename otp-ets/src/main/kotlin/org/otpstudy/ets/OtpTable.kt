package org.otpstudy.ets

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

enum class TableType { Set, OrderedSet, Bag, DuplicateBag }

enum class TableAccess {
    /** Owner can read and write; any actor can read. */
    Protected,
    /** Any actor can read and write. */
    Public,
    /** Only the owning actor can read or write. */
    Private,
}

/**
 * ETS-style in-process table.
 *
 * | Type         | Backing store           | Duplicate keys |
 * |--------------|-------------------------|----------------|
 * | Set          | ConcurrentHashMap       | No (overwrite) |
 * | OrderedSet   | ConcurrentSkipListMap   | No (overwrite) |
 * | Bag          | HashMap<K, List<V>>     | Yes, unique V  |
 * | DuplicateBag | HashMap<K, List<V>>     | Yes, any V     |
 *
 * **Ownership:** use [OtpTableRegistry.new] with an ownerJob; the table is removed from the
 * registry when the job completes, analogous to OTP's "table is deleted when the owning process
 * exits."
 *
 * **Access control:** [TableAccess.Protected] uses a [ReentrantReadWriteLock] so only one writer
 * at a time; reads are concurrent. This is weaker than BEAM ETS (no per-row locking) but teaches
 * the concept.
 *
 * OTP source: `lib/stdlib/src/ets.erl`, `erts/emulator/beam/erl_db.c`
 */
class OtpTable<K, V>(
    val name: String,
    val type: TableType = TableType.Set,
    val access: TableAccess = TableAccess.Protected,
) {
    private val lock = ReentrantReadWriteLock()
    private val versions = ConcurrentHashMap<Any?, AtomicLong>()

    private val setStore: ConcurrentHashMap<K, V>? =
        if (type == TableType.Set) ConcurrentHashMap() else null

    @Suppress("UNCHECKED_CAST")
    private val orderedStore: ConcurrentSkipListMap<Any, V>? =
        if (type == TableType.OrderedSet) ConcurrentSkipListMap() else null

    private val bagStore: ConcurrentHashMap<K, CopyOnWriteArrayList<V>>? =
        if (type == TableType.Bag || type == TableType.DuplicateBag) ConcurrentHashMap() else null

    fun insert(key: K, value: V): Unit = lock.write {
        when (type) {
            TableType.Set -> setStore!![key] = value
            TableType.OrderedSet -> orderedStore!![key as Any] = value
            TableType.Bag -> {
                val list = bagStore!!.getOrPut(key) { CopyOnWriteArrayList() }
                if (!list.contains(value)) list.add(value)
            }
            TableType.DuplicateBag ->
                bagStore!!.getOrPut(key) { CopyOnWriteArrayList() }.add(value)
        }
    }

    fun lookup(key: K): List<V> = lock.read {
        when (type) {
            TableType.Set -> setStore!![key]?.let { listOf(it) } ?: emptyList()
            TableType.OrderedSet -> orderedStore!![key as Any]?.let { listOf(it) } ?: emptyList()
            TableType.Bag, TableType.DuplicateBag -> bagStore!![key]?.toList() ?: emptyList()
        }
    }

    fun delete(key: K): Unit = lock.write {
        when (type) {
            TableType.Set -> setStore!!.remove(key)
            TableType.OrderedSet -> orderedStore!!.remove(key as Any)
            TableType.Bag, TableType.DuplicateBag -> bagStore!!.remove(key)
        }
    }

    fun match(pattern: (K, V) -> Boolean): List<Pair<K, V>> = lock.read {
        when (type) {
            TableType.Set -> setStore!!.entries
                .filter { pattern(it.key, it.value) }
                .map { it.key to it.value }
            TableType.OrderedSet -> orderedStore!!.entries
                .filter { pattern(it.key as K, it.value) }
                .map { it.key as K to it.value }
            TableType.Bag, TableType.DuplicateBag ->
                bagStore!!.entries.flatMap { (k, vs) ->
                    vs.filter { v -> pattern(k, v) }.map { v -> k to v }
                }
        }
    }

    fun <A> foldl(initial: A, f: (K, V, A) -> A): A = lock.read {
        when (type) {
            TableType.Set -> setStore!!.entries.fold(initial) { acc, e -> f(e.key, e.value, acc) }
            TableType.OrderedSet -> orderedStore!!.entries
                .fold(initial) { acc, e -> f(e.key as K, e.value, acc) }
            TableType.Bag, TableType.DuplicateBag ->
                bagStore!!.entries.fold(initial) { outerAcc, (k, vs) ->
                    vs.fold(outerAcc) { acc, v -> f(k, v, acc) }
                }
        }
    }

    fun toList(): List<Pair<K, V>> = match { _, _ -> true }

    fun size(): Int = lock.read {
        when (type) {
            TableType.Set -> setStore!!.size
            TableType.OrderedSet -> orderedStore!!.size
            TableType.Bag, TableType.DuplicateBag -> bagStore!!.values.sumOf { it.size }
        }
    }

    // ── Versioned operations for Mnesia-style MVCC transactions ──────────────

    /**
     * Read [key] and return the current value alongside its version token.
     * Used by [org.otpstudy.mnesia.MnesiaTransaction] to detect conflicts at commit time.
     */
    fun readWithVersion(key: K): Pair<V?, Long> = lock.read {
        val value = lookup(key).firstOrNull()
        val ver = versions[key as Any?]?.get() ?: 0L
        value to ver
    }

    /** Current version of [key] — 0 if the key has never been written. */
    fun currentVersion(key: K): Long = versions[key as Any?]?.get() ?: 0L

    /**
     * Insert and bump the version counter for [key].
     * Used by [org.otpstudy.mnesia.MnesiaTransaction.commit].
     */
    fun insertVersioned(key: K, value: V) = lock.write {
        insert(key, value)
        versions.getOrPut(key as Any?) { AtomicLong(0L) }.incrementAndGet()
    }

    /** Expose the table's write lock for atomic multi-table commit in transactions. */
    fun <T> withWriteLock(block: () -> T): T = lock.write(block)

    /**
     * Type-safe analogue of `ets:select/2`.
     *
     * Applies [spec]'s guard and projection to every row; returns all matching projections.
     * For [TableType.OrderedSet], iteration follows the natural key order — matching
     * `ets:select` on `ordered_set`.
     */
    fun <R> select(spec: MatchSpec<K, V, R>): List<R> = lock.read {
        val entries: Sequence<Pair<K, V>> = when (type) {
            TableType.Set -> setStore!!.entries.asSequence().map { it.key to it.value }
            TableType.OrderedSet -> orderedStore!!.entries.asSequence().map { it.key as K to it.value }
            TableType.Bag, TableType.DuplicateBag ->
                bagStore!!.entries.asSequence().flatMap { (k, vs) -> vs.asSequence().map { v -> k to v } }
        }
        val guarded = if (spec.guard != null) entries.filter { (k, v) -> spec.guard.invoke(k, v) } else entries
        guarded.map { (k, v) -> spec.projection(k, v) }.toList()
    }

    /**
     * Continuation-based select (mirrors `ets:select/3`).
     *
     * Returns at most [limit] results and a [Continuation] for the next page, or null if done.
     */
    fun <R> selectContinuation(spec: MatchSpec<K, V, R>, limit: Int): Pair<List<R>, Continuation<K, V, R>?> {
        val all = toList()
        val filtered = if (spec.guard != null)
            all.filter { (k, v) -> spec.guard.invoke(k, v) }
        else all
        val page = filtered.take(limit)
        val rest = filtered.drop(limit)
        val results = page.map { (k, v) -> spec.projection(k, v) }
        val cont = if (rest.isEmpty()) null else Continuation(rest, spec)
        return results to cont
    }

    /** Resume a continuation produced by [selectContinuation]. */
    fun <R> selectContinue(cont: Continuation<K, V, R>, limit: Int): Pair<List<R>, Continuation<K, V, R>?> {
        val page = cont.remaining.take(limit)
        val rest = cont.remaining.drop(limit)
        val results = page.map { (k, v) -> cont.spec.projection(k, v) }
        val next = if (rest.isEmpty()) null else Continuation(rest, cont.spec)
        return results to next
    }
}
