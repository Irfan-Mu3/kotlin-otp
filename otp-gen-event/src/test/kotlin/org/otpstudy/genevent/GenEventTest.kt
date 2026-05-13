package org.otpstudy.genevent

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class GenEventTest {

    // -----------------------------------------------------------------------
    // Ordering: handlers notified in insertion order
    // -----------------------------------------------------------------------

    @Test
    fun handlersNotifiedInInsertionOrder() = runBlocking {
        val order = mutableListOf<String>()
        val mgr = GenEventManagers.startLink<String>(this, BackPressurePolicy.Block)

        val h1 = object : GenEventHandler<String> {
            override suspend fun handleEvent(event: String) { order.add("h1:$event") }
        }
        val h2 = object : GenEventHandler<String> {
            override suspend fun handleEvent(event: String) { order.add("h2:$event") }
        }

        mgr.addHandler(h1)
        mgr.addHandler(h2)
        mgr.notify("A")
        mgr.notify("B")

        mgr.shutdown()

        assertEquals(listOf("h1:A", "h2:A", "h1:B", "h2:B"), order)
    }

    // -----------------------------------------------------------------------
    // Handler removal during notify
    // -----------------------------------------------------------------------

    @Test
    fun removedHandlerNotCalledAfterDeletion() = runBlocking {
        val received = mutableListOf<String>()
        val mgr = GenEventManagers.startLink<String>(this, BackPressurePolicy.Block)

        val h = object : GenEventHandler<String> {
            override suspend fun handleEvent(event: String) { received.add(event) }
        }

        mgr.addHandler(h)
        mgr.notify("first")
        mgr.deleteHandler(h)
        mgr.notify("second")

        mgr.shutdown()

        assertEquals(listOf("first"), received)
    }

    // -----------------------------------------------------------------------
    // terminate() called on removeHandler
    // -----------------------------------------------------------------------

    @Test
    fun terminateCalledOnRemove() = runBlocking {
        var terminateCalled = false
        val mgr = GenEventManagers.startLink<Int>(this, BackPressurePolicy.Block)

        val h = object : GenEventHandler<Int> {
            override suspend fun handleEvent(event: Int) {}
            override suspend fun terminate() { terminateCalled = true }
        }

        mgr.addHandler(h)
        mgr.deleteHandler(h)
        mgr.shutdown()

        assertEquals(true, terminateCalled)
    }

    // -----------------------------------------------------------------------
    // terminate() called on all handlers during shutdown
    // -----------------------------------------------------------------------

    @Test
    fun allHandlersTerminatedOnShutdown() = runBlocking {
        val terminated = mutableListOf<Int>()
        val mgr = GenEventManagers.startLink<String>(this, BackPressurePolicy.Block)

        for (i in 1..3) {
            val id = i
            mgr.addHandler(object : GenEventHandler<String> {
                override suspend fun handleEvent(event: String) {}
                override suspend fun terminate() { terminated.add(id) }
            })
        }

        mgr.shutdown()

        assertEquals(3, terminated.size)
        assertEquals(listOf(1, 2, 3), terminated)
    }

    // -----------------------------------------------------------------------
    // Back-pressure: DropNew
    // -----------------------------------------------------------------------

    @Test
    fun dropNewPolicyDoesNotBlock() = runBlocking {
        // DropNew with capacity 1; second fire-and-forget send is dropped silently
        val mgr = GenEventManagers.startLink<Int>(this, BackPressurePolicy.DropNew(1))

        // These are fire-and-forget (trySend)
        mgr.notify(1)
        mgr.notify(2)
        mgr.notify(3)

        mgr.shutdown()
        // Test passes as long as it does not deadlock or throw
    }

    // -----------------------------------------------------------------------
    // No handlers: notify is a no-op
    // -----------------------------------------------------------------------

    @Test
    fun notifyWithNoHandlers() = runBlocking {
        val mgr = GenEventManagers.startLink<String>(this, BackPressurePolicy.Block)
        mgr.notify("lonely")
        mgr.shutdown()
    }
}
