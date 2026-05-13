package org.otpstudy.dets

import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DetsTableTest {

    private var tempFile: Path? = null

    @AfterTest
    fun cleanup() {
        tempFile?.let { Files.deleteIfExists(it) }
    }

    private fun tempPath(): Path =
        Files.createTempFile("dets-test-", ".wal").also { tempFile = it }

    // ── 1. insert and lookup returns value ────────────────────────────────────

    @Test
    fun `insert and lookup returns value`() {
        val path = tempPath()
        DetsTable.openString("t1", path).use { table ->
            table.insert("hello", "world")
            assertEquals(listOf("world"), table.lookup("hello"))
        }
    }

    // ── 2. delete removes entry ───────────────────────────────────────────────

    @Test
    fun `delete removes entry`() {
        val path = tempPath()
        DetsTable.openString("t2", path).use { table ->
            table.insert("key", "value")
            table.delete("key")
            assertEquals(emptyList(), table.lookup("key"))
        }
    }

    // ── 3. sync writes to file ────────────────────────────────────────────────

    @Test
    fun `sync writes to file`() {
        val path = tempPath()
        DetsTable.openString("t3", path).use { table ->
            table.insert("a", "b")
            table.sync()
        }
        assertTrue(Files.exists(path), "WAL file should exist after sync")
        assertTrue(Files.size(path) > 0, "WAL file should be non-empty after sync")
    }

    // ── 4. replay on reopen recovers data ─────────────────────────────────────

    @Test
    fun `replay on reopen recovers data`() {
        val path = tempPath()

        // Write some data and close.
        DetsTable.openString("t4", path).use { table ->
            table.insert("k1", "v1")
            table.insert("k2", "v2")
            table.sync()
        }

        // Reopen and verify data is still there.
        DetsTable.openString("t4", path).use { table ->
            assertEquals(listOf("v1"), table.lookup("k1"))
            assertEquals(listOf("v2"), table.lookup("k2"))
        }
    }

    // ── 5. close syncs automatically ─────────────────────────────────────────

    @Test
    fun `close syncs automatically`() {
        val path = tempPath()

        // Insert without explicit sync, just close.
        DetsTable.openString("t5", path).use { table ->
            table.insert("persist", "me")
            // no explicit sync — close() should do it
        }

        // Reopen and verify.
        DetsTable.openString("t5", path).use { table ->
            assertEquals(listOf("me"), table.lookup("persist"))
        }
    }
}
