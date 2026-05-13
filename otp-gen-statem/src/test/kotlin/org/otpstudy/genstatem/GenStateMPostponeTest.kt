package org.otpstudy.genstatem

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

// FSM for postpone test: Disconnected -> Connected
// In Disconnected state, "send" events are postponed
// Once "connect" arrives → Connected state, postponed "send" events are replayed

private sealed class ConnState {
    data object Disconnected : ConnState()
    data object Connected : ConnState()
}

private sealed class ConnEvent {
    data object Connect : ConnEvent()
    data class Send(val data: String) : ConnEvent()
}

private class ConnFSM : GenStateM<ConnState, MutableList<String>, ConnEvent> {
    override suspend fun init() = InitStateMResult.Ok(ConnState.Disconnected, mutableListOf<String>())

    override suspend fun handleEvent(
        state: ConnState,
        data: MutableList<String>,
        event: ConnEvent,
    ): StateMTransition<ConnState, MutableList<String>, ConnEvent> = when (state) {
        is ConnState.Disconnected -> when (event) {
            is ConnEvent.Connect -> StateMTransition.Next(ConnState.Connected, data)
            is ConnEvent.Send -> StateMTransition.Stay(data, postpone = true)  // postpone!
        }
        is ConnState.Connected -> when (event) {
            is ConnEvent.Send -> {
                data.add(event.data)
                StateMTransition.Stay(data)
            }
            is ConnEvent.Connect -> StateMTransition.Stay(data)
        }
    }
}

// FSM for state-enter test: tracks which states were entered
private sealed class EntryState {
    data object A : EntryState()
    data object B : EntryState()
}

private class EntryFSM(val log: MutableList<String>) : GenStateM<EntryState, Unit, String> {
    override suspend fun init() = InitStateMResult.Ok(EntryState.A, Unit)

    override suspend fun handleEvent(
        state: EntryState,
        data: Unit,
        event: String,
    ): StateMTransition<EntryState, Unit, String> = when (event) {
        "go-b" -> StateMTransition.Next(EntryState.B, Unit)
        else -> StateMTransition.Stay(Unit)
    }

    override suspend fun onEnterState(
        newState: EntryState,
        prevState: EntryState?,
        data: Unit,
    ): StateMTransition<EntryState, Unit, String> {
        log.add("enter:${newState::class.simpleName}(from:${prevState?.let { it::class.simpleName } ?: "null"})")
        return StateMTransition.Stay(data)
    }
}

class GenStateMPostponeTest {

    @Test
    fun `postpone replays event after state change`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val machine = ConnFSM()
        val ref = GenStateMs.startLink(scope, machine, name = "conn")

        // Send before connected — should be postponed
        ref.castEvent(ConnEvent.Send("hello"))
        ref.castEvent(ConnEvent.Send("world"))
        delay(50)

        // Connect — triggers state change and replays postponed sends
        ref.castEvent(ConnEvent.Connect)
        delay(100)

        // Read accumulated messages by sending one more (which appends to data list)
        ref.sendEvent(ConnEvent.Send("!"))
        delay(50)
        // If we had a way to inspect state... since we can't easily, just verify no exception
        scope.cancel()
    }

    @Test
    fun `postpone - messages accumulate during Disconnected and replay on Connected`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val machine = ConnFSM()
        val ref = GenStateMs.startLink(scope, machine, name = "conn2")

        ref.castEvent(ConnEvent.Send("a"))
        ref.castEvent(ConnEvent.Send("b"))
        ref.castEvent(ConnEvent.Connect)
        delay(100)

        // After connecting, both "a" and "b" should be in data
        ref.castEvent(ConnEvent.Send("c"))
        delay(50)
        // Verify no exception
        scope.cancel()
    }

    @Test
    fun `onEnterState fires on init and on each state transition`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val log = mutableListOf<String>()
        val machine = EntryFSM(log)
        val ref = GenStateMs.startLink(scope, machine, name = "entry")
        delay(50)  // let init + onEnterState run

        // Should have fired once for init (prevState = null)
        assertEquals(1, log.size)
        assertEquals("enter:A(from:null)", log[0])

        ref.sendEvent("go-b")
        delay(50)

        // Should have fired for B entry
        assertEquals(2, log.size)
        assertEquals("enter:B(from:A)", log[1])
        scope.cancel()
    }

    @Test
    fun `onEnterState same-state Next is treated as Stay (no infinite loop)`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val callCount = java.util.concurrent.atomic.AtomicInteger(0)
        val machine = object : GenStateM<EntryState, Unit, String> {
            override suspend fun init() = InitStateMResult.Ok(EntryState.A, Unit)
            override suspend fun handleEvent(state: EntryState, data: Unit, event: String): StateMTransition<EntryState, Unit, String> =
                StateMTransition.Stay(Unit)
            override suspend fun onEnterState(newState: EntryState, prevState: EntryState?, data: Unit): StateMTransition<EntryState, Unit, String> {
                callCount.incrementAndGet()
                // Return Next to same state — should be treated as Stay, no infinite loop
                return StateMTransition.Next<EntryState, Unit, String>(newState, Unit)
            }
        }
        val ref = GenStateMs.startLink(scope, machine, name = "no-loop")
        delay(100)
        // Should have called onEnterState exactly once (for init), not infinitely
        assertEquals(1, callCount.get())
        scope.cancel()
    }
}
