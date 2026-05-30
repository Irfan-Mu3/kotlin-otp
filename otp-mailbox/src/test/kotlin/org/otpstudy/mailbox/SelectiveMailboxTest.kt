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

    // -------------------------------------------------------------------------
    // mark / receiveFrom
    // -------------------------------------------------------------------------

    @Test
    fun `mark returns current saved size`() = runTest {
        val ch = Channel<Int>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)
        assertEquals(0, box.mark())

        ch.send(1); ch.send(2); ch.send(3)
        box.receive { it == 3 }  // saves 1 and 2
        assertEquals(2, box.mark())
    }

    @Test
    fun `receiveFrom skips pre-mark saved messages`() = runTest {
        val ch = Channel<Int>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)

        // Build up pre-mark noise in saved
        ch.send(10); ch.send(20); ch.send(99)
        box.receive { it == 99 }  // saves 10, 20
        assertEquals(2, box.savedSize)

        val m = box.mark()  // m == 2

        // Match arrives in channel after the mark
        ch.send(42)
        val result = box.receiveFrom(m) { it == 42 }

        assertEquals(42, result)
        // Pre-mark noise (10, 20) must still be in saved, untouched
        assertEquals(2, box.savedSize)
    }

    @Test
    fun `receiveFrom finds match in post-mark saved slice`() = runTest {
        val ch = Channel<Int>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)

        // Build pre-mark noise
        ch.send(10); ch.send(20); ch.send(99)
        box.receive { it == 99 }  // saved = [10, 20]

        val m = box.mark()  // m == 2

        // Receive something that saves a new message, then the match arrives
        ch.send(30); ch.send(55)
        box.receiveFrom(m) { it == 55 }  // saves 30 at index 2, finds 55 at index 3

        // Pre-mark saved (10, 20) plus the post-mark noise (30) remain
        assertEquals(3, box.savedSize)
    }

    @Test
    fun `receiveFrom with mark zero behaves like receive`() = runTest {
        val ch = Channel<String>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)

        ch.send("a"); ch.send("b"); ch.send("c")
        val m = box.mark()  // 0 — nothing saved yet

        val result = box.receiveFrom(m) { it == "b" }
        assertEquals("b", result)
        assertEquals(1, box.savedSize)  // "a" was saved
    }

    @Test
    fun `receiveFrom non-matching channel messages are appended to saved`() = runTest {
        val ch = Channel<Int>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)

        val m = box.mark()  // 0
        ch.send(1); ch.send(2); ch.send(3)

        val result = box.receiveFrom(m) { it == 3 }
        assertEquals(3, result)
        // 1 and 2 should be in saved
        assertEquals(2, box.savedSize)
        assertEquals(1, box.peekSaved())
    }

    @Test
    fun `multiple mark-receiveFrom cycles accumulate pre-mark messages`() = runTest {
        data class Reply(val tag: Any, val value: String)

        val ch = Channel<Reply>(Channel.UNLIMITED)
        val box = SelectiveMailbox(ch)
        val tag1 = Any()
        val tag2 = Any()

        // First call: noise arrives before the match so receiveFrom saves it
        ch.send(Reply(Any(), "noise"))
        ch.send(Reply(tag1, "r1"))
        val m1 = box.mark()  // m1 == 0, saved is empty
        val r1 = box.receiveFrom(m1) { it is Reply && it.tag === tag1 }
        assertEquals("r1", r1.value)
        // "noise" was consumed from channel and saved before the match was found
        assertEquals(1, box.savedSize)

        // Second call: mark advances past the noise saved from cycle 1
        val m2 = box.mark()  // m2 == 1
        ch.send(Reply(tag2, "r2"))
        val r2 = box.receiveFrom(m2) { it is Reply && it.tag === tag2 }
        assertEquals("r2", r2.value)
        // Noise from cycle 1 still in saved, untouched by the second receiveFrom
        assertEquals(1, box.savedSize)
    }
}
