package org.otpstudy.core

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class OtpPersistentTermsTest {

    @Test
    fun `put and get round-trip`() {
        OtpPersistentTerms.clear()
        OtpPersistentTerms.put("key1", "value1")
        assertEquals("value1", OtpPersistentTerms.get<String>("key1"))
    }

    @Test
    fun `get returns null for missing key`() {
        OtpPersistentTerms.clear()
        assertNull(OtpPersistentTerms.get<String>("missing"))
    }

    @Test
    fun `delete removes key`() {
        OtpPersistentTerms.clear()
        OtpPersistentTerms.put("del-key", 42)
        OtpPersistentTerms.delete("del-key")
        assertNull(OtpPersistentTerms.get<Int>("del-key"))
    }

    @Test
    fun `require throws on missing key`() {
        OtpPersistentTerms.clear()
        try {
            OtpPersistentTerms.require<String>("nonexistent")
            error("should have thrown")
        } catch (e: IllegalStateException) {
            assert(e.message!!.contains("nonexistent"))
        }
    }

    @Test
    fun `watch fires callback on put`() {
        OtpPersistentTerms.clear()
        var received: Any? = "sentinel"
        val handle = OtpPersistentTerms.watch("watched-key") { _, v -> received = v }
        OtpPersistentTerms.put("watched-key", "trigger")
        assertEquals("trigger", received)
        handle.close()
    }

    @Test
    fun `watch fires null on delete`() {
        OtpPersistentTerms.clear()
        OtpPersistentTerms.put("del-watch", "initial")
        var received: Any? = "sentinel"
        val handle = OtpPersistentTerms.watch("del-watch") { _, v -> received = v }
        OtpPersistentTerms.delete("del-watch")
        assertNull(received)
        handle.close()
    }

    @Test
    fun `watch closed stops receiving updates`() {
        OtpPersistentTerms.clear()
        var callCount = 0
        val handle = OtpPersistentTerms.watch("closed-watch") { _, _ -> callCount++ }
        OtpPersistentTerms.put("closed-watch", "a")
        assertEquals(1, callCount)
        handle.close()
        OtpPersistentTerms.put("closed-watch", "b")
        assertEquals(1, callCount)
    }
}
