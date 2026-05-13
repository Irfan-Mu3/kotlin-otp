package org.otpstudy.hotcode

import org.otpstudy.genserver.CodeChangeRegistry
import org.otpstudy.genserver.GenServer
import java.net.URLClassLoader
import java.nio.file.Path

/**
 * Load a new [GenServer] implementation from a JAR at [jarPath].
 *
 * Analogous to the BEAM loading a new `.beam` alongside the old module version.
 * The old class stays in memory until its last reference is GC'd.
 */
fun loadUpgrade(jarPath: Path, className: String): Class<out GenServer<*>> {
    val loader = URLClassLoader(
        arrayOf(jarPath.toUri().toURL()),
        Thread.currentThread().contextClassLoader,
    )
    @Suppress("UNCHECKED_CAST")
    return loader.loadClass(className) as Class<out GenServer<*>>
}

/**
 * Descriptor for an upgrade or downgrade between two application versions.
 *
 * Analogous to OTP `.appup` files used by `release_handler`.
 * OTP source: `lib/sasl/src/release_handler.erl`, `lib/sasl/src/appup_compiler.erl`
 */
data class UpgradeDescriptor(
    val fromVersion: String,
    val toVersion: String,
    val steps: List<UpgradeStep>,
)

sealed class UpgradeStep {
    /** Load a new module version from [jarPath] and schedule it for the named server. */
    data class LoadModule(
        val jarPath: Path,
        val className: String,
        val serverName: String,
    ) : UpgradeStep()

    /** Signal that a named server should apply a pending code change on its next message boundary. */
    data class ApplyCodeChange(val serverName: String) : UpgradeStep()

    /** Restart a named OTP application. */
    data class RestartApplication(val appName: String) : UpgradeStep()
}

/**
 * Applies or rolls back an [UpgradeDescriptor].
 *
 * Analogous to `release_handler:install_release/1` and `release_handler:remove_release/1`.
 * The gen-server run loop picks up code changes on the next message boundary.
 */
object ReleaseHandler {
    suspend fun apply(descriptor: UpgradeDescriptor) {
        for (step in descriptor.steps) applyStep(step, descriptor)
    }

    suspend fun rollback(descriptor: UpgradeDescriptor) {
        for (step in descriptor.steps.reversed()) rollbackStep(step, descriptor)
    }

    private fun applyStep(step: UpgradeStep, desc: UpgradeDescriptor) {
        when (step) {
            is UpgradeStep.LoadModule -> {
                val cls = loadUpgrade(step.jarPath, step.className)
                CodeChangeRegistry.schedule(step.serverName, cls, desc.fromVersion, desc.toVersion)
            }
            is UpgradeStep.ApplyCodeChange -> {
                // The gen-server run loop picks this up automatically via CodeChangeRegistry.
            }
            is UpgradeStep.RestartApplication -> {
                // Application restart is wired via OtpApplication; reserved for future use.
            }
        }
    }

    private fun rollbackStep(step: UpgradeStep, desc: UpgradeDescriptor) {
        // Rollback schedules the old version back; inverse of apply.
        when (step) {
            is UpgradeStep.LoadModule -> {
                val cls = loadUpgrade(step.jarPath, step.className)
                CodeChangeRegistry.schedule(step.serverName, cls, desc.toVersion, desc.fromVersion)
            }
            else -> { /* no-op for other step types during rollback */ }
        }
    }
}
