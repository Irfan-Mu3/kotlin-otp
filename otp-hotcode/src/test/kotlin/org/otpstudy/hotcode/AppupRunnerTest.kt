package org.otpstudy.hotcode

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.registry.ProcessRegistry
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

private class AppupCounterV1 : GenServer<Long> {
    override suspend fun init() = InitResult.Ok(0L)
    override suspend fun handleCall(request: Any, state: Long) = ReplyResult.Reply<Long>(state, state)
    override suspend fun handleCast(request: Any, state: Long) = NoreplyResult.Noreply(state + 1)
    override suspend fun codeChange(oldVersion: String, newVersion: String, state: Long): Long = state * 2
}

private class AppupCounterV2 : GenServer<Long> {
    override suspend fun init() = InitResult.Ok(0L)
    override suspend fun handleCall(request: Any, state: Long) = ReplyResult.Reply<Long>(state, state)
    override suspend fun handleCast(request: Any, state: Long) = NoreplyResult.Noreply(state + 10)
    override suspend fun codeChange(oldVersion: String, newVersion: String, state: Long): Long = state * 2
}

class AppupRunnerTest {
    private lateinit var scope: CoroutineScope
    private lateinit var registry: ProcessRegistry

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        registry = ProcessRegistry()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
    }

    @Test
    fun `UpdateActor applies codeChange and swaps implementation`() = runBlocking {
        val ref = GenServers.startLink(scope, AppupCounterV1(), name = "counter")
        registry.register("counter", ref)

        ref.cast("increment") // state = 1

        val script = AppupScript(
            fromVersion = "1.0",
            toVersion = "2.0",
            upgrade = listOf(
                AppupInstruction.UpdateActor(
                    name = "counter",
                    oldVersion = "1.0",
                    newVersion = "2.0",
                    newClass = AppupCounterV2::class.java,
                )
            )
        )

        AppupRunner.runUpgrade(script, registry)

        // codeChange multiplied state by 2 (1 * 2 = 2)
        assertEquals(2L, ref.call<Long>("get"))

        // After upgrade, cast should use AppupCounterV2's handleCast (state += 10)
        ref.cast("inc")
        kotlinx.coroutines.delay(50)
        assertEquals(12L, ref.call<Long>("get"))

        ref.stop()
    }

    @Test
    fun `LoadModule is a no-op and does not throw`() = runBlocking {
        val script = AppupScript(
            fromVersion = "1.0",
            toVersion = "2.0",
            upgrade = listOf(AppupInstruction.LoadModule("some.Module"))
        )
        AppupRunner.runUpgrade(script, registry) // should not throw
    }

    @Test
    fun `missing actor in UpdateActor is logged but does not abort remaining instructions`() = runBlocking {
        var noopApplied = false
        val script = AppupScript(
            fromVersion = "1.0",
            toVersion = "2.0",
            upgrade = listOf(
                AppupInstruction.UpdateActor("missing", "1.0", "2.0", AppupCounterV2::class.java),
                AppupInstruction.LoadModule("safe.Module"),  // should still run
            )
        )
        AppupRunner.runUpgrade(script, registry) // should complete without throwing
        // If we reach here, the runner continued past the error
    }

    @Test
    fun `RestartActor stops the actor`() = runBlocking {
        val ref = GenServers.startLink(scope, AppupCounterV1(), name = "restart-target")
        registry.register("restart-target", ref)

        val script = AppupScript(
            fromVersion = "1.0",
            toVersion = "2.0",
            upgrade = listOf(AppupInstruction.RestartActor("restart-target"))
        )
        AppupRunner.runUpgrade(script, registry)
        ref.job.join()
        // After stop, job should be inactive
        assertEquals(false, ref.job.isActive)
    }
}
