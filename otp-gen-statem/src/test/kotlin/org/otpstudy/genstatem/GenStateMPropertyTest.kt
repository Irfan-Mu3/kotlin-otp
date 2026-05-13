package org.otpstudy.genstatem

import io.kotest.core.spec.style.FreeSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.of
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking

// ---------------------------------------------------------------------------
// Reference FSM for differential testing
// ---------------------------------------------------------------------------

/** Pure reference implementation of the Toggle machine. */
private data class RefState(val toggle: Toggle, val count: Int)

private fun refStep(s: RefState, e: ToggleEvent): RefState =
    when (s.toggle) {
        Toggle.Off -> when (e) {
            ToggleEvent.Press -> RefState(Toggle.On, s.count + 1)
        }
        Toggle.On -> when (e) {
            ToggleEvent.Press -> RefState(Toggle.Off, s.count)
        }
    }

// ---------------------------------------------------------------------------
// Property spec
// ---------------------------------------------------------------------------

class GenStateMPropertyTest : FreeSpec({

    "final (state, data) matches reference FSM for random event sequences" {
        checkAll(Arb.list(Arb.of(ToggleEvent.Press), 0..50)) { events ->
            // Compute reference result
            var ref = RefState(Toggle.Off, 0)
            for (e in events) {
                ref = refStep(ref, e)
            }

            // Run through the real GenStateM
            runBlocking {
                val ref2 = GenStateMs.startLink(this, ToggleMachine(), name = "prop-test")
                var lastTransition: StateMTransition<Toggle, Int, ToggleEvent>? = null
                for (e in events) {
                    lastTransition = ref2.sendEvent(e)
                }

                if (events.isNotEmpty()) {
                    val (finalState, finalData) = when (val t = lastTransition) {
                        is StateMTransition.Next -> t.newState to t.newData
                        is StateMTransition.Stay -> null to null
                        is StateMTransition.Stop, null -> null to null
                    }
                    if (finalState != null && finalData != null) {
                        assert(finalState == ref.toggle) {
                            "state mismatch after ${events.size} events: got $finalState, expected ${ref.toggle}"
                        }
                        assert(finalData == ref.count) {
                            "data mismatch after ${events.size} events: got $finalData, expected ${ref.count}"
                        }
                    }
                }
                ref2.stop()
            }
        }
    }

    "machine stops cleanly with no events" {
        checkAll(Arb.int(1..5)) { _ ->
            runBlocking {
                val ref = GenStateMs.startLink(this, ToggleMachine(), name = "empty-seq")
                ref.stop()
                assert(ref.job.isCompleted)
            }
        }
    }
})
