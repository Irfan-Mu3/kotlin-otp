package org.otpstudy.application

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.runTest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ApplicationControllerTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        ApplicationController.reset()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        ApplicationController.reset()
    }

    @Test
    fun `load makes spec available in loadedApplications`() {
        val spec = ApplicationController.ApplicationSpec("db", "1.0")
        ApplicationController.load(spec)
        assertTrue(ApplicationController.loadedApplications().any { it.name == "db" })
    }

    @Test
    fun `start requires application to be loaded`() = runTest {
        assertFailsWith<IllegalStateException> {
            ApplicationController.start("nonexistent", scope)
        }
    }

    @Test
    fun `start simple app without dependencies`() = runTest {
        ApplicationController.load(ApplicationController.ApplicationSpec("core", "1.0"))
        val app = ApplicationController.start("core", scope)
        assertEquals("core", app.spec.name)
        assertTrue(ApplicationController.whichApplications().any { it.spec.name == "core" })
    }

    @Test
    fun `start app auto-starts dependencies in order`() = runTest {
        ApplicationController.load(ApplicationController.ApplicationSpec("db", "1.0"))
        ApplicationController.load(ApplicationController.ApplicationSpec("web", "1.0", applications = listOf("db")))
        ApplicationController.load(ApplicationController.ApplicationSpec("api", "1.0", applications = listOf("web", "db")))

        ApplicationController.start("api", scope)

        val running = ApplicationController.whichApplications().map { it.spec.name }
        assertTrue("db" in running)
        assertTrue("web" in running)
        assertTrue("api" in running)
    }

    @Test
    fun `start is idempotent`() = runTest {
        ApplicationController.load(ApplicationController.ApplicationSpec("svc", "1.0"))
        ApplicationController.start("svc", scope)
        ApplicationController.start("svc", scope)
        assertEquals(1, ApplicationController.whichApplications().count { it.spec.name == "svc" })
    }

    @Test
    fun `stop removes from whichApplications`() = runTest {
        ApplicationController.load(ApplicationController.ApplicationSpec("x", "1.0"))
        ApplicationController.start("x", scope)
        ApplicationController.stop("x")
        assertTrue(ApplicationController.whichApplications().none { it.spec.name == "x" })
    }

    @Test
    fun `stop propagates to dependents`() = runTest {
        ApplicationController.load(ApplicationController.ApplicationSpec("kernel", "1.0"))
        ApplicationController.load(ApplicationController.ApplicationSpec("stdlib", "1.0", applications = listOf("kernel")))
        ApplicationController.start("stdlib", scope)

        ApplicationController.stop("kernel")

        val running = ApplicationController.whichApplications().map { it.spec.name }
        assertTrue("kernel" !in running, "kernel should be stopped")
        assertTrue("stdlib" !in running, "stdlib depends on kernel, should be stopped too")
    }

    @Test
    fun `unload fails when app is running`() = runTest {
        ApplicationController.load(ApplicationController.ApplicationSpec("live", "1.0"))
        ApplicationController.start("live", scope)
        assertFailsWith<IllegalStateException> {
            ApplicationController.unload("live")
        }
    }

    @Test
    fun `unload succeeds when app is stopped`() {
        ApplicationController.load(ApplicationController.ApplicationSpec("stopped", "1.0"))
        ApplicationController.unload("stopped")
        assertTrue(ApplicationController.loadedApplications().none { it.name == "stopped" })
    }
}
