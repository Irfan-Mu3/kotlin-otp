package org.otpstudy.ets

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OtpTableTest {

    @Test
    fun `Set insert and lookup`() {
        val t = OtpTable<String, Int>("set-1", TableType.Set)
        t.insert("a", 1); t.insert("b", 2)
        assertEquals(listOf(1), t.lookup("a"))
        assertEquals(listOf(2), t.lookup("b"))
        assertEquals(emptyList(), t.lookup("missing"))
    }

    @Test
    fun `Set overwrites on re-insert`() {
        val t = OtpTable<String, Int>("set-2", TableType.Set)
        t.insert("k", 1); t.insert("k", 99)
        assertEquals(listOf(99), t.lookup("k"))
        assertEquals(1, t.size())
    }

    @Test
    fun `OrderedSet preserves insertion order by key`() {
        val t = OtpTable<Int, String>("ordered-1", TableType.OrderedSet)
        t.insert(3, "c"); t.insert(1, "a"); t.insert(2, "b")
        val all = t.toList()
        assertEquals(listOf(1, 2, 3), all.map { it.first })
    }

    @Test
    fun `Bag stores multiple unique values per key`() {
        val t = OtpTable<String, String>("bag-1", TableType.Bag)
        t.insert("key", "a"); t.insert("key", "b"); t.insert("key", "a")
        val vals = t.lookup("key")
        // Bag de-duplicates: "a" only once
        assertEquals(2, vals.size)
        assertTrue(vals.containsAll(listOf("a", "b")))
    }

    @Test
    fun `DuplicateBag allows duplicate values`() {
        val t = OtpTable<String, String>("dupbag-1", TableType.DuplicateBag)
        t.insert("k", "v"); t.insert("k", "v"); t.insert("k", "v")
        assertEquals(3, t.lookup("k").size)
    }

    @Test
    fun `delete removes entry`() {
        val t = OtpTable<Int, Int>("del-1", TableType.Set)
        t.insert(1, 10); t.delete(1)
        assertEquals(emptyList(), t.lookup(1))
    }

    @Test
    fun `match filters by predicate`() {
        val t = OtpTable<String, Int>("match-1", TableType.Set)
        t.insert("a", 1); t.insert("b", 5); t.insert("c", 3)
        val high = t.match { _, v -> v > 2 }
        assertEquals(2, high.size)
        assertTrue(high.any { it.first == "b" && it.second == 5 })
        assertTrue(high.any { it.first == "c" && it.second == 3 })
    }

    @Test
    fun `foldl accumulates values`() {
        val t = OtpTable<String, Int>("fold-1", TableType.Set)
        t.insert("a", 1); t.insert("b", 2); t.insert("c", 3)
        val sum = t.foldl(0) { _, v, acc -> acc + v }
        assertEquals(6, sum)
    }

    @Test
    fun `OtpTableRegistry new and lookup`() {
        val t = OtpTableRegistry.new<String, Int>("reg-1")
        assertTrue("reg-1" in OtpTableRegistry.tableNames())
        assertEquals(t, OtpTableRegistry.lookup<String, Int>("reg-1"))
    }

    @Test
    fun `OtpTableRegistry removes table when owner job completes`() {
        val job = kotlinx.coroutines.Job()
        OtpTableRegistry.new<String, Int>("owned-1", ownerJob = job)
        assertTrue("owned-1" in OtpTableRegistry.tableNames())
        job.cancel()
        Thread.sleep(20)
        assertNull(OtpTableRegistry.lookup<String, Int>("owned-1"))
    }

    @Test
    fun `stats returns correct size`() {
        val t = OtpTable<Int, String>("stats-1", TableType.Set)
        t.insert(1, "x"); t.insert(2, "y")
        val s = t.stats()
        assertEquals("stats-1", s.name); assertEquals(2, s.size)
        assertEquals(TableType.Set, s.type)
    }

    // ---- §5 match spec DSL ----

    @Test
    fun `select with guard filters rows`() {
        val t = OtpTable<String, Int>("ms-1", TableType.Set)
        t.insert("a", 1); t.insert("b", 5); t.insert("c", 3)
        val spec = matchSpec<String, Int, String> {
            guard { (_, v) -> v > 2 }
            project { (k, _) -> k }
        }
        val keys = t.select(spec).sorted()
        assertEquals(listOf("b", "c"), keys)
    }

    @Test
    fun `select without guard returns all projected values`() {
        val t = OtpTable<Int, String>("ms-2", TableType.Set)
        t.insert(1, "x"); t.insert(2, "y"); t.insert(3, "z")
        val spec = matchSpec<Int, String, String> {
            project { (_, v) -> v }
        }
        assertEquals(3, t.select(spec).size)
    }

    @Test
    fun `select on OrderedSet respects key order`() {
        val t = OtpTable<Int, String>("ms-3", TableType.OrderedSet)
        t.insert(3, "c"); t.insert(1, "a"); t.insert(2, "b")
        val spec = matchSpec<Int, String, Int> {
            project { (k, _) -> k }
        }
        assertEquals(listOf(1, 2, 3), t.select(spec))
    }

    @Test
    fun `selectContinuation pages through results`() {
        val t = OtpTable<Int, Int>("ms-4", TableType.OrderedSet)
        (1..5).forEach { t.insert(it, it * 10) }
        val spec = matchSpec<Int, Int, Int> { project { (_, v) -> v } }
        val (page1, cont1) = t.selectContinuation(spec, 3)
        assertEquals(3, page1.size)
        val (page2, cont2) = t.selectContinue(cont1!!, 10)
        assertEquals(2, page2.size)
        assertEquals(null, cont2)
    }

    @Test
    fun `select returns empty list when guard matches nothing`() {
        val t = OtpTable<String, Int>("ms-5", TableType.Set)
        t.insert("a", 1); t.insert("b", 2)
        val spec = matchSpec<String, Int, String> {
            guard { (_, v) -> v > 100 }
            project { (k, _) -> k }
        }
        assertEquals(emptyList(), t.select(spec))
    }
}
