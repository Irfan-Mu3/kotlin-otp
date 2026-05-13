package org.otpstudy.mailbox

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class SelectiveMailboxTest {

    @Test
    fun `receive matches and saves non-matching`() = runTest {
        val ch = Channel<Int>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)
        ch.send(1); ch.send(2); ch.send(3); ch.send(4)

        // Receive only even numbers
        val first = box.receive { it % 2 == 0 }
        assertEquals(2, first)
        assertEquals(1, box.savedSize)  // 1 was saved

        val second = box.receive { it % 2 == 0 }
        assertEquals(4, second)
        assertEquals(2, box.savedSize)  // 1 and 3 saved
    }

    @Test
    fun `saved messages reconsidered on next receive`() = runTest {
        val ch = Channel<String>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)
        ch.send("a"); ch.send("b"); ch.send("c")

        // Skip "a" and "b" to get "c"
        val c = box.receive { it == "c" }
        assertEquals("c", c)
        assertEquals(2, box.savedSize)

        // Now receive "a" — it's in the saved list
        val a = box.receive { it == "a" }
        assertEquals("a", a)
        assertEquals(1, box.savedSize)  // "b" still saved
    }

    @Test
    fun `flushSaved returns all saved messages to channel`() = runTest {
        val ch = Channel<Int>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)
        ch.send(1); ch.send(2); ch.send(3)

        box.receive { it == 3 }  // saves 1, 2
        assertEquals(2, box.savedSize)

        box.flushSaved()
        assertEquals(0, box.savedSize)

        // 1 and 2 should now be back in channel
        val next1 = box.receive { true }
        val next2 = box.receive { true }
        assertEquals(1, next1)
        assertEquals(2, next2)
    }

    @Test
    fun `receive with after analog using withTimeoutOrNull`() = runTest {
        val ch = Channel<String>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)
        ch.send("hello")

        // Match everything — should get "hello"
        val result = kotlinx.coroutines.withTimeoutOrNull(100) {
            box.receive { it == "hello" }
        }
        assertEquals("hello", result)
    }

    @Test
    fun `priority pattern - high priority first even if arrived late`() = runTest {
        val ch = Channel<Pair<Int, String>>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)
        ch.send(Pair(2, "low-1"))
        ch.send(Pair(2, "low-2"))
        ch.send(Pair(1, "high-1"))

        // Drain all high-priority first
        val high = box.receive { it.first == 1 }
        assertEquals(Pair(1, "high-1"), high)

        // Then low-priority in original FIFO order
        val low1 = box.receive { it.first == 2 }
        assertEquals(Pair(2, "low-1"), low1)
    }
}
