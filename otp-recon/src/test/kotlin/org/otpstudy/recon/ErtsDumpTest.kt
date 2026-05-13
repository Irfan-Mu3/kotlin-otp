package org.otpstudy.recon

import kotlin.test.Test
import kotlin.test.assertTrue

class ErtsDumpTest {

    @Test
    fun `size of null is zero`() {
        assertTrue(ErtsDump.size(null) == 0L)
    }

    @Test
    fun `size of simple string is positive`() {
        assertTrue(ErtsDump.size("hello") > 0L)
    }

    @Test
    fun `larger string has larger size than empty string`() {
        assertTrue(ErtsDump.size("a".repeat(100)) > ErtsDump.size(""))
    }

    @Test
    fun `size of nested object is greater than size of leaf`() {
        data class Inner(val x: Int)
        data class Outer(val inner: Inner, val label: String)

        val inner = Inner(42)
        val outer = Outer(inner, "test")
        assertTrue(ErtsDump.size(outer) > ErtsDump.size(inner))
    }

    @Test
    fun `size of list is positive`() {
        val list = listOf(1, 2, 3, 4, 5)
        assertTrue(ErtsDump.size(list) > 0L)
    }

    @Test
    fun `size of byte array scales with size`() {
        val small = ByteArray(10)
        val large = ByteArray(1000)
        assertTrue(ErtsDump.size(large) > ErtsDump.size(small))
    }

    @Test
    fun `SchedulerStats sample has positive pool size`() {
        val s = SchedulerStats.sample()
        assertTrue(s.poolSize > 0)
        assertTrue(s.activeThreads > 0)
        assertTrue(s.timestamp > 0L)
    }

    @Test
    fun `SchedulerStats utilisation is non-negative`() {
        val s1 = SchedulerStats.sample()
        val s2 = SchedulerStats.sample()
        val u = SchedulerStats.utilisation(s1, s2)
        assertTrue(u >= 0.0)
    }

    @Test
    fun `utilisation is zero when poolSize is zero`() {
        val s1 = SchedulerStats.Sample(0L, 5, 0)
        val s2 = SchedulerStats.Sample(1000L, 5, 0)
        assertTrue(SchedulerStats.utilisation(s1, s2) == 0.0)
    }
}
