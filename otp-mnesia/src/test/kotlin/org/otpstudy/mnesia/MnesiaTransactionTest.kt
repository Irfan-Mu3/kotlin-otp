package org.otpstudy.mnesia

import kotlinx.coroutines.test.runTest
import org.otpstudy.ets.OtpTable
import org.otpstudy.ets.TableType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class MnesiaTransactionTest {

    @Test
    fun `read within transaction sees initial value`() = runTest {
        val table = OtpTable<String, Long>("accounts", TableType.Set)
        table.insert("alice", 1000L)

        transaction {
            assertEquals(1000L, read(table, "alice"))
        }
    }

    @Test
    fun `atomic transfer - both writes applied`() = runTest {
        val table = OtpTable<String, Long>("accounts", TableType.Set)
        table.insert("alice", 1000L)
        table.insert("bob", 500L)

        transaction {
            val alice = read(table, "alice") ?: 0L
            val bob = read(table, "bob") ?: 0L
            write(table, "alice", alice - 200L)
            write(table, "bob", bob + 200L)
        }

        assertEquals(800L, table.lookup("alice").first())
        assertEquals(700L, table.lookup("bob").first())
    }

    @Test
    fun `delete within transaction removes key`() = runTest {
        val table = OtpTable<String, Long>("accounts", TableType.Set)
        table.insert("temp", 42L)

        transaction {
            delete(table, "temp")
        }

        assertEquals(emptyList(), table.lookup("temp"))
    }

    @Test
    fun `read-your-own-writes within transaction`() = runTest {
        val table = OtpTable<String, Int>("counters", TableType.Set)
        table.insert("k", 0)

        transaction {
            val v = read(table, "k") ?: 0
            write(table, "k", v + 1)
            assertEquals(1, read(table, "k"))  // see buffered write
        }

        assertEquals(1, table.lookup("k").first())
    }

    @Test
    fun `read after delete in same transaction returns null`() = runTest {
        val table = OtpTable<String, Int>("t", TableType.Set)
        table.insert("x", 99)

        transaction {
            delete(table, "x")
            assertNull(read(table, "x"))
        }
    }

    @Test
    fun `conflict detected when key modified between read and commit`() = runTest {
        val table = OtpTable<String, Int>("t", TableType.Set)
        table.insert("k", 0)

        val tx = MnesiaTransaction()
        tx.read(table, "k")       // snapshot version = 0

        table.insertVersioned("k", 99)  // external write bumps version

        val result = tx.commit()
        assert(result is CommitResult.Conflict) { "expected Conflict but got $result" }
        // External write should stand
        assertEquals(99, table.lookup("k").first())
    }

    @Test
    fun `no conflict when no external writes occur`() = runTest {
        val table = OtpTable<String, Int>("t", TableType.Set)
        table.insert("k", 0)

        val tx = MnesiaTransaction()
        tx.read(table, "k")
        tx.write(table, "k", 1)
        assertEquals(CommitResult.Ok, tx.commit())
        assertEquals(1, table.lookup("k").first())
    }

    @Test
    fun `transaction retries on conflict`() = runTest {
        val table = OtpTable<String, Int>("t", TableType.Set)
        table.insert("k", 0)

        var attempts = 0
        transaction(maxRetries = 3) {
            attempts++
            val v = read(table, "k") ?: 0
            if (attempts < 2) {
                // Simulate external write after read to force first attempt to conflict
                table.insertVersioned("k", 999)
            }
            write(table, "k", v + 1)
        }

        assertEquals(2, attempts)
    }

    @Test
    fun `transaction throws after max retries exhausted`() = runTest {
        val table = OtpTable<String, Int>("t", TableType.Set)
        table.insert("k", 0)

        assertFailsWith<TransactionAbortedException> {
            transaction(maxRetries = 2) {
                val v = read(table, "k") ?: 0
                // Always inject a conflict after reading
                table.insertVersioned("k", 999)
                write(table, "k", v + 1)
            }
        }
    }

    @Test
    fun `multi-table transaction applies all writes atomically`() = runTest {
        val accounts = OtpTable<String, Long>("accounts", TableType.Set)
        val audit = OtpTable<Int, String>("audit", TableType.Set)
        accounts.insert("alice", 500L)

        var auditKey = 0

        transaction {
            val balance = read(accounts, "alice") ?: 0L
            write(accounts, "alice", balance - 100L)
            write(audit, auditKey++, "debit 100 from alice")
        }

        assertEquals(400L, accounts.lookup("alice").first())
        assertEquals(1, audit.size())
    }

    @Test
    fun `version is zero for key never written via insertVersioned`() = runTest {
        val table = OtpTable<String, Int>("t", TableType.Set)
        table.insert("k", 1)  // plain insert — no version bump
        assertEquals(0L, table.currentVersion("k"))
    }

    @Test
    fun `version increments after insertVersioned`() = runTest {
        val table = OtpTable<String, Int>("t", TableType.Set)
        table.insertVersioned("k", 1)
        assertEquals(1L, table.currentVersion("k"))
        table.insertVersioned("k", 2)
        assertEquals(2L, table.currentVersion("k"))
    }
}
