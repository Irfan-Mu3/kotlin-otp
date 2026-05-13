package org.otpstudy.sasl

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import org.otpstudy.genevent.GenEventHandler
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AlarmHandlerTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        AlarmHandler.init(scope)
    }

    @AfterTest
    fun tearDown() = runTest {
        AlarmHandler.reset()
        scope.cancel()
    }

    @Test
    fun `setAlarm adds alarm to active set`() = runTest {
        AlarmHandler.setAlarm("disk_full", "partition /var")
        assertTrue(AlarmHandler.isAlarmSet("disk_full"))
        AlarmHandler.clearAlarm("disk_full")
    }

    @Test
    fun `setAlarm is idempotent`() = runTest {
        val events = mutableListOf<AlarmEvent>()
        val handler = object : GenEventHandler<AlarmEvent> {
            override suspend fun handleEvent(event: AlarmEvent) { events.add(event) }
        }
        AlarmHandler.addHandler(handler)

        AlarmHandler.setAlarm("cpu_hot")
        AlarmHandler.setAlarm("cpu_hot")  // second call — should not fire again
        AlarmHandler.setAlarm("cpu_hot")

        kotlinx.coroutines.delay(50)
        assertEquals(1, events.filterIsInstance<AlarmEvent.Set>().size,
            "set handler should fire exactly once for repeated set_alarm")

        AlarmHandler.removeHandler(handler)
        AlarmHandler.clearAlarm("cpu_hot")
    }

    @Test
    fun `clearAlarm removes alarm and fires Cleared event`() = runTest {
        val events = mutableListOf<AlarmEvent>()
        val handler = object : GenEventHandler<AlarmEvent> {
            override suspend fun handleEvent(event: AlarmEvent) { events.add(event) }
        }
        AlarmHandler.addHandler(handler)

        AlarmHandler.setAlarm("net_down")
        AlarmHandler.clearAlarm("net_down")

        kotlinx.coroutines.delay(50)
        assertFalse(AlarmHandler.isAlarmSet("net_down"))
        val cleared = events.filterIsInstance<AlarmEvent.Cleared>()
        assertEquals(1, cleared.size)
        assertEquals("net_down", cleared.first().alarmId)

        AlarmHandler.removeHandler(handler)
    }

    @Test
    fun `clearAlarm on unset alarm does nothing`() = runTest {
        val events = mutableListOf<AlarmEvent>()
        val handler = object : GenEventHandler<AlarmEvent> {
            override suspend fun handleEvent(event: AlarmEvent) { events.add(event) }
        }
        AlarmHandler.addHandler(handler)

        AlarmHandler.clearAlarm("not_set")
        kotlinx.coroutines.delay(50)
        assertTrue(events.isEmpty())

        AlarmHandler.removeHandler(handler)
    }

    @Test
    fun `getAlarms returns all active alarms`() = runTest {
        AlarmHandler.setAlarm("a1", "desc1")
        AlarmHandler.setAlarm("a2", "desc2")
        val alarms = AlarmHandler.getAlarms()
        assertTrue("a1" in alarms)
        assertTrue("a2" in alarms)
        AlarmHandler.clearAlarm("a1")
        AlarmHandler.clearAlarm("a2")
    }

    @Test
    fun `Set event carries description`() = runTest {
        val events = mutableListOf<AlarmEvent>()
        val handler = object : GenEventHandler<AlarmEvent> {
            override suspend fun handleEvent(event: AlarmEvent) { events.add(event) }
        }
        AlarmHandler.addHandler(handler)

        AlarmHandler.setAlarm("queue_full", mapOf("queue" to "jobs", "depth" to 10000))
        kotlinx.coroutines.delay(50)

        val setEvent = events.filterIsInstance<AlarmEvent.Set>().first()
        assertEquals("queue_full", setEvent.alarmId)
        @Suppress("UNCHECKED_CAST")
        val desc = setEvent.description as Map<String, Any>
        assertEquals("jobs", desc["queue"])

        AlarmHandler.removeHandler(handler)
        AlarmHandler.clearAlarm("queue_full")
    }

    @Test
    fun `multiple handlers all receive events`() = runTest {
        val e1 = mutableListOf<AlarmEvent>()
        val e2 = mutableListOf<AlarmEvent>()
        val h1 = object : GenEventHandler<AlarmEvent> { override suspend fun handleEvent(event: AlarmEvent) { e1.add(event) } }
        val h2 = object : GenEventHandler<AlarmEvent> { override suspend fun handleEvent(event: AlarmEvent) { e2.add(event) } }
        AlarmHandler.addHandler(h1)
        AlarmHandler.addHandler(h2)

        AlarmHandler.setAlarm("disk_low")
        kotlinx.coroutines.delay(50)

        assertEquals(1, e1.size)
        assertEquals(1, e2.size)

        AlarmHandler.removeHandler(h1)
        AlarmHandler.removeHandler(h2)
        AlarmHandler.clearAlarm("disk_low")
    }
}
