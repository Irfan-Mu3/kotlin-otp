package org.otpstudy.memory

import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class IsolatedTest {

    @Test
    fun `owner can read Isolated value`() = runTest {
        val iso = isolated("hello")
        assertEquals("hello", iso.get())
    }

    @Test
    fun `different actor throws on read`() = runBlocking {
        // Create the Isolated value bound to the parent job
        val iso = isolated(42)

        // Launch a child with a different Job and try to read
        val child = launch {
            assertFailsWith<IllegalStateException> {
                iso.get()
            }
        }
        child.join()
    }

    @Test
    fun `getUnchecked is always accessible`() = runTest {
        val iso = isolated(99)
        assertEquals(99, iso.getUnchecked())
    }

    @Test
    fun `isolated wraps different types`() = runTest {
        data class State(val count: Int)
        val iso = isolated(State(7))
        assertEquals(State(7), iso.get())
    }
}
