package org.otpstudy.supervisor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.otpstudy.core.Restart
import kotlin.test.Test
import kotlin.test.assertTrue

class SupervisorBridgeTest {

    @Test
    fun `SupervisorBridge childSpec creates a valid child that runs and completes`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        var ran = false

        val spec = SupervisorBridge.childSpec(
            id = "bridge-child",
            restart = Restart.Temporary,
        ) { bridge ->
            ran = true
            bridge.exit(BridgeExit.Normal)
        }

        val supRef = Supervisor.startLink(
            scope,
            SupervisorFlags(SupervisorStrategy.OneForOne),
            listOf(spec),
        )
        delay(50)
        assertTrue(ran, "Bridge block should have run")
        supRef.shutdown()
        scope.cancel()
    }

    @Test
    fun `SupervisorBridge - abnormal exit restarts child when Permanent`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        var runCount = 0

        val spec = SupervisorBridge.childSpec(
            id = "bridge-restart",
            restart = Restart.Permanent,
        ) { _ ->
            runCount++
            if (runCount < 3) throw RuntimeException("simulated failure")
        }

        Supervisor.startLink(
            scope,
            SupervisorFlags(SupervisorStrategy.OneForOne, intensity = 5),
            listOf(spec),
        )
        delay(500)
        assertTrue(runCount >= 3, "Should have restarted at least twice, ran $runCount times")
        scope.cancel()
    }
}
