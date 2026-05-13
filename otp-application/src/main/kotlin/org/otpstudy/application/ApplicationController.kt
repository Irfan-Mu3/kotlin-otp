package org.otpstudy.application

import kotlinx.coroutines.CoroutineScope
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * Central application registry — OTP's application_controller in kotlin-otp.
 *
 * Tracks the loaded/started distinction, enforces dependency ordering,
 * and propagates stops to dependents.
 *
 * OTP source: lib/kernel/src/application_controller.erl
 */
object ApplicationController {
    private val loaded  = ConcurrentHashMap<String, ApplicationSpec>()
    private val running = ConcurrentHashMap<String, RunningApp>()

    data class ApplicationSpec(
        val name: String,
        val vsn: String,
        val description: String = "",
        /** Names of applications that must be started before this one. */
        val applications: List<String> = emptyList(),
        val startModule: String? = null,
    )

    data class RunningApp(
        val spec: ApplicationSpec,
        val ref: Any,
        val startedAt: Instant = Instant.now(),
    )

    /** Load an application spec without starting it. Analogous to application:load/1. */
    fun load(spec: ApplicationSpec) {
        loaded[spec.name] = spec
    }

    /** Remove a loaded application. Fails if the application is currently running. */
    fun unload(name: String) {
        check(!running.containsKey(name)) { "cannot unload running application '$name'" }
        loaded.remove(name)
    }

    /**
     * Start [name] and any unstarted dependencies in dependency order.
     * Analogous to application:start/1.
     */
    suspend fun start(name: String, scope: CoroutineScope): RunningApp {
        if (running.containsKey(name)) return running[name]!!
        val spec = loaded[name] ?: error("application '$name' not loaded")
        for (dep in spec.applications) {
            if (!running.containsKey(dep)) start(dep, scope)
        }
        val app = RunningApp(spec, Unit)
        running[name] = app
        return app
    }

    /**
     * Stop [name] and any running applications that depend on it.
     * Analogous to application:stop/1.
     */
    fun stop(name: String) {
        running.remove(name) ?: return
        val dependents = running.values.filter { it.spec.applications.contains(name) }
        for (dep in dependents) stop(dep.spec.name)
    }

    /** All currently running applications. Analogous to application:which_applications/0. */
    fun whichApplications(): List<RunningApp> = running.values.toList()

    /** All loaded applications (running or not). Analogous to application:loaded_applications/0. */
    fun loadedApplications(): List<ApplicationSpec> = loaded.values.toList()

    fun reset() {
        loaded.clear()
        running.clear()
    }
}
