package org.otpstudy.logger

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OtpStructuredLoggerTest {

    private lateinit var backend: ListBackend

    @BeforeTest
    fun setUp() {
        backend = ListBackend()
        OtpStructuredLogger.reset()
        OtpStructuredLogger.addHandler(
            LogHandler("test", backend = backend)
        )
    }

    @AfterTest
    fun tearDown() {
        OtpStructuredLogger.reset()
    }

    @Test
    fun `log event reaches handler backend`() {
        OtpStructuredLogger.info("hello world")
        assertEquals(1, backend.lines.size)
        assertTrue(backend.lines.first().contains("hello world"))
    }

    @Test
    fun `log level is included in output`() {
        OtpStructuredLogger.error("boom")
        assertTrue(backend.lines.first().contains("Error"))
    }

    @Test
    fun `metadata is appended to output`() {
        OtpStructuredLogger.info("event", mapOf("user" to "alice", "action" to "login"))
        val line = backend.lines.first()
        assertTrue(line.contains("alice"))
        assertTrue(line.contains("login"))
    }

    @Test
    fun `process metadata auto-attached to events`() {
        OtpStructuredLogger.setProcessMetadata(mapOf("actor.name" to "counter"))
        OtpStructuredLogger.info("tick")
        assertTrue(backend.lines.first().contains("counter"))
        OtpStructuredLogger.clearProcessMetadata()
    }

    @Test
    fun `primary filter Stop discards event`() {
        OtpStructuredLogger.addPrimaryFilter { LogFilterResult.Stop }
        OtpStructuredLogger.info("should be dropped")
        assertTrue(backend.lines.isEmpty())
    }

    @Test
    fun `primary filter Pass bypasses remaining filters`() {
        // First filter passes immediately; second would stop (but is never reached).
        OtpStructuredLogger.addPrimaryFilter { LogFilterResult.Pass }
        OtpStructuredLogger.addPrimaryFilter { LogFilterResult.Stop }
        OtpStructuredLogger.info("reaches handler")
        assertEquals(1, backend.lines.size)
    }

    @Test
    fun `per-handler filter Stop drops event for that handler`() {
        val filtered = ListBackend()
        OtpStructuredLogger.addHandler(LogHandler(
            id = "filtered",
            filters = listOf(LogFilter { LogFilterResult.Stop }),
            backend = filtered,
        ))
        OtpStructuredLogger.info("message")
        assertEquals(1, backend.lines.size)    // default handler received it
        assertTrue(filtered.lines.isEmpty())   // filtered handler did not
    }

    @Test
    fun `handler minLevel filters low-priority events`() {
        val errOnly = ListBackend()
        OtpStructuredLogger.addHandler(
            LogHandler("err-only", backend = errOnly, minLevel = LogLevel.Error)
        )
        OtpStructuredLogger.debug("debug msg")
        OtpStructuredLogger.info("info msg")
        OtpStructuredLogger.warning("warn msg")
        OtpStructuredLogger.error("error msg")
        assertTrue(errOnly.lines.size == 1)
        assertTrue(errOnly.lines.first().contains("error msg"))
    }

    @Test
    fun `multiple handlers each receive the event`() {
        val b2 = ListBackend()
        OtpStructuredLogger.addHandler(LogHandler("second", backend = b2))
        OtpStructuredLogger.info("fan-out")
        assertEquals(1, backend.lines.size)
        assertEquals(1, b2.lines.size)
    }

    @Test
    fun `remove handler stops delivery`() {
        OtpStructuredLogger.removeHandler("test")
        OtpStructuredLogger.info("nobody home")
        assertTrue(backend.lines.isEmpty())
    }

    @Test
    fun `custom formatter applied per handler`() {
        val custom = ListBackend()
        OtpStructuredLogger.addHandler(LogHandler(
            id = "custom",
            formatter = LogFormatter { event -> "CUSTOM:${event.message}" },
            backend = custom,
        ))
        OtpStructuredLogger.info("structured")
        assertTrue(custom.lines.first().startsWith("CUSTOM:"))
    }

    @Test
    fun `convenience level helpers work`() {
        OtpStructuredLogger.debug("d")
        OtpStructuredLogger.info("i")
        OtpStructuredLogger.warning("w")
        OtpStructuredLogger.error("e")
        assertEquals(4, backend.lines.size)
    }

    @Test
    fun `primary filter Ignore passes control to next filter`() {
        OtpStructuredLogger.addPrimaryFilter { LogFilterResult.Ignore }  // ignore → falls through
        OtpStructuredLogger.info("passes")
        assertEquals(1, backend.lines.size)
    }

    @Test
    fun `handlerIds returns all registered handler ids`() {
        OtpStructuredLogger.addHandler(LogHandler("h2", backend = ListBackend()))
        val ids = OtpStructuredLogger.handlerIds()
        assertTrue("test" in ids)
        assertTrue("h2" in ids)
    }

    @Test
    fun `reset clears all state`() {
        OtpStructuredLogger.addPrimaryFilter { LogFilterResult.Stop }
        OtpStructuredLogger.reset()
        OtpStructuredLogger.addHandler(LogHandler("test", backend = backend))
        OtpStructuredLogger.info("after reset")
        assertFalse(backend.lines.isEmpty())
    }
}
