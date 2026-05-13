package org.otpstudy.genserver

import kotlinx.coroutines.yield
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * Coroutine-scoped reduction counter — the cooperative analogue of BEAM preemption.
 *
 * The BEAM preempts processes after ~2 000 reductions (each BIF/function call burns reductions).
 * Here, the actor must call [reduce] explicitly; the run loop does so after each message, and
 * user code can call it in hot paths. When the budget is exhausted [reduce] yields, giving other
 * coroutines a chance to run.
 *
 * Levels of fidelity:
 * - Level 1 (this class): cooperative — actors must call [reduce] themselves.
 * - Level 2: Kotlin compiler plugin that injects [reduce] at loop back-edges (see docs/archive/roadmaps/THE_DEEP_END.md §2).
 * - Level 3: Byte Buddy Java agent for zero-source injection (see docs/archive/roadmaps/THE_DEEP_END.md §2).
 *
 * OTP source: `erts/emulator/beam/erl_process.c` — REDS_LEFT / schedule()
 */
class ReductionBudget(val limit: Int = 2_000) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<ReductionBudget>
    override val key get() = Key

    private var count = 0
    private val _total = AtomicLong(0L)

    /** Total reductions since this actor started; thread-safe for external probes. */
    val totalReductions: Long get() = _total.get()

    /**
     * Burn [n] reductions. Yields to the scheduler when the budget is exhausted.
     * Call in tight loops or after each significant unit of work.
     */
    suspend fun reduce(n: Int = 1) {
        count += n
        _total.addAndGet(n.toLong())
        if (count >= limit) {
            count = 0
            yield()
        }
    }
}
