package org.otpstudy.ets

import kotlinx.coroutines.Job
import java.util.concurrent.ConcurrentHashMap

/**
 * Global registry for named [OtpTable]s.
 *
 * Ownership: pass [ownerJob] to automatically remove the table when the owning actor's job
 * completes. This mirrors OTP's "ETS table is deleted when the owning process exits."
 */
object OtpTableRegistry {
    private val tables = ConcurrentHashMap<String, OtpTable<*, *>>()

    fun <K, V> new(
        name: String,
        type: TableType = TableType.Set,
        access: TableAccess = TableAccess.Protected,
        ownerJob: Job? = null,
    ): OtpTable<K, V> {
        val table = OtpTable<K, V>(name, type, access)
        tables[name] = table
        ownerJob?.invokeOnCompletion { tables.remove(name, table) }
        return table
    }

    @Suppress("UNCHECKED_CAST")
    fun <K, V> lookup(name: String): OtpTable<K, V>? = tables[name] as OtpTable<K, V>?

    fun allTables(): List<OtpTable<*, *>> = tables.values.toList()

    fun tableNames(): Set<String> = tables.keys.toSet()
}

data class TableStats(
    val name: String,
    val type: TableType,
    val access: TableAccess,
    val size: Int,
)

fun OtpTable<*, *>.stats() = TableStats(name, type, access, size())
