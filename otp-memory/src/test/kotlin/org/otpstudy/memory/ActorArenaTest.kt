package org.otpstudy.memory

import java.lang.foreign.MemoryLayout
import java.lang.foreign.ValueLayout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ActorArenaTest {

    @Test
    fun `allocate returns a non-null segment`() {
        val arena = ActorArena()
        val layout = MemoryLayout.sequenceLayout(1024, ValueLayout.JAVA_BYTE)
        val seg = arena.allocate(layout)
        assertNotNull(seg)
        arena.close()
    }

    @Test
    fun `bytesAllocated tracks total allocation`() {
        val arena = ActorArena()
        val layout = MemoryLayout.sequenceLayout(64, ValueLayout.JAVA_BYTE)
        arena.allocate(layout)
        arena.allocate(layout)
        assertEquals(128L, arena.bytesAllocated)
        arena.close()
    }

    @Test
    fun `allocateBytes works for raw byte count`() {
        val arena = ActorArena()
        arena.allocateBytes(256L)
        assertEquals(256L, arena.bytesAllocated)
        arena.close()
    }

    @Test
    fun `close frees the arena without throwing`() {
        val arena = ActorArena()
        arena.allocateBytes(512L)
        arena.close()  // must not throw
    }

    @Test
    fun `multiple close calls are idempotent`() {
        val arena = ActorArena()
        arena.close()
        arena.close()  // second close via runCatching — must not throw
    }

    @Test
    fun `segment byteSize matches layout`() {
        val arena = ActorArena()
        val layout = MemoryLayout.sequenceLayout(100, ValueLayout.JAVA_BYTE)
        val seg = arena.allocate(layout)
        assertTrue(seg.byteSize() >= 100L)
        arena.close()
    }
}
