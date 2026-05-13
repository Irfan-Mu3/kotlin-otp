package org.otpstudy.dets

import org.otpstudy.ets.OtpTable
import org.otpstudy.ets.TableType
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE

/**
 * DETS-style disk-backed table.
 *
 * Wraps an in-memory [OtpTable] and persists mutations via a write-ahead log (WAL).
 * The WAL is an append-only text file; each line is one of:
 *
 *   `I\tkey_hex\tvalue_hex\n`   — insert
 *   `D\tkey_hex\n`              — delete
 *
 * On construction the existing log is replayed to reconstruct the in-memory state.
 * [sync] is a no-op in the WAL model (every mutation is immediately appended), but
 * it is provided for API symmetry with `dets:sync/1`.
 * [close] calls [sync] and then closes the [FileChannel].
 *
 * OTP analogue: `lib/dets/src/dets.erl`
 */
class DetsTable<K : Any, V : Any>(
    val name: String,
    val file: Path,
    val type: TableType = TableType.Set,
    val keySerializer: (K) -> ByteArray,
    val valueSerializer: (V) -> ByteArray,
    val keyDeserializer: (ByteArray) -> K,
    val valueDeserializer: (ByteArray) -> V,
) : AutoCloseable {

    private val mem: OtpTable<K, V> = OtpTable(name, type)
    private val channel: FileChannel = FileChannel.open(file, CREATE, READ, WRITE)

    // Track dirty keys so sync() knows what to flush.
    // Because the WAL is append-only every write is already on disk, so the
    // dirty set is only needed if a caller wants to do a compaction/checkpoint
    // in the future. We maintain it for completeness and to support the
    // contract described in the spec.
    private val dirtyKeys: MutableSet<K> = mutableSetOf()

    init {
        replayLog()
    }

    // ── WAL replay ────────────────────────────────────────────────────────────

    private fun replayLog() {
        // Read the full file content then parse line by line.
        val size = channel.size()
        if (size == 0L) return
        val buf = ByteBuffer.allocate(size.toInt())
        channel.read(buf, 0L)
        buf.flip()
        val content = String(buf.array(), 0, buf.limit(), Charsets.US_ASCII)
        for (line in content.lineSequence()) {
            if (line.isEmpty()) continue
            val parts = line.split('\t')
            when (parts[0]) {
                "I" -> {
                    if (parts.size < 3) continue
                    val key = keyDeserializer(fromHex(parts[1]))
                    val value = valueDeserializer(fromHex(parts[2]))
                    mem.insert(key, value)
                }
                "D" -> {
                    if (parts.size < 2) continue
                    val key = keyDeserializer(fromHex(parts[1]))
                    mem.delete(key)
                }
            }
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /** Insert [key]/[value] into the in-memory table and append an I-entry to the WAL. */
    fun insert(key: K, value: V) {
        mem.insert(key, value)
        dirtyKeys.add(key)
        val line = "I\t${toHex(keySerializer(key))}\t${toHex(valueSerializer(value))}\n"
        appendToLog(line)
    }

    /** Look up [key] in the in-memory table. */
    fun lookup(key: K): List<V> = mem.lookup(key)

    /** Remove [key] from the in-memory table and append a D-entry to the WAL. */
    fun delete(key: K) {
        mem.delete(key)
        dirtyKeys.add(key)
        val line = "D\t${toHex(keySerializer(key))}\n"
        appendToLog(line)
    }

    /**
     * Flush dirty entries to disk.
     *
     * Because the WAL is append-only all mutations are already durable by the
     * time this method is called. [sync] forces the OS page-cache to disk via
     * [FileChannel.force] and clears the dirty set — analogous to `dets:sync/1`.
     */
    fun sync() {
        channel.force(true)
        dirtyKeys.clear()
    }

    /** [sync] then close the underlying [FileChannel]. */
    override fun close() {
        sync()
        channel.close()
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private fun appendToLog(line: String) {
        val bytes = line.toByteArray(Charsets.US_ASCII)
        val buf = ByteBuffer.wrap(bytes)
        channel.position(channel.size())
        while (buf.hasRemaining()) {
            channel.write(buf)
        }
    }

    private fun toHex(bytes: ByteArray): String =
        bytes.joinToString("") { "%02x".format(it) }

    private fun fromHex(hex: String): ByteArray {
        val len = hex.length
        return ByteArray(len / 2) { i ->
            hex.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
    }

    // ── Companion ─────────────────────────────────────────────────────────────

    companion object {
        /**
         * Convenience factory for `String -> String` tables.
         *
         * Analogous to opening a DETS table with `{type, set}` and a simple
         * external term format for UTF-8 strings.
         */
        fun openString(name: String, file: Path): DetsTable<String, String> =
            DetsTable(
                name = name,
                file = file,
                keySerializer = String::toByteArray,
                valueSerializer = String::toByteArray,
                keyDeserializer = ::String,
                valueDeserializer = ::String,
            )
    }
}
