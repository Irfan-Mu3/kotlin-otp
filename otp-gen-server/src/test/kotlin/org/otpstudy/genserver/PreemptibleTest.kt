package org.otpstudy.genserver

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@Preemptible
private class SampleAnnotatedServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(null, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class PreemptibleTest {
    @Test
    fun `Preemptible annotation is retained and accessible`() {
        val annotation = SampleAnnotatedServer::class.java.getAnnotation(Preemptible::class.java)
        assertNotNull(annotation, "Preemptible annotation should be present at runtime")
        assertEquals(1, annotation.reductionsPerCall)
    }

    @Test
    fun `reduce top-level function decrements ReductionBudget in context`() = runTest {
        val budget = ReductionBudget(100)
        withContext(budget) {
            reduce(5)
            assertEquals(5L, budget.totalReductions)
        }
    }

    @Test
    fun `reduce is no-op outside budget context`() = runTest {
        // Should not throw when no budget is in context
        reduce(10)
    }

    @Test
    fun `reduce accumulates across multiple calls`() = runTest {
        val budget = ReductionBudget(10_000)
        withContext(budget) {
            repeat(10) { reduce(3) }
            assertEquals(30L, budget.totalReductions)
        }
    }
}
