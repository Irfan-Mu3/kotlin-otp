package org.otpstudy.hotcode

import org.otpstudy.core.OtpLogContext
import org.otpstudy.core.OtpLogLevel
import org.otpstudy.core.OtpLogging
import org.otpstudy.registry.ProcessRegistry

/**
 * Interpret and apply an [AppupScript] against a live actor registry.
 *
 * Analogous to release_handler:install_release/1 — walks the upgrade instruction list
 * and applies each instruction in sequence.
 *
 * OTP source: lib/sasl/src/release_handler.erl — do_upgrade/2
 */
object AppupRunner {
    private val ctx = OtpLogContext("appup")

    /**
     * Run the upgrade instructions in [script] against [registry].
     *
     * Instructions are applied in order. Any individual instruction failure
     * is logged but does not abort the remaining instructions.
     */
    suspend fun runUpgrade(script: AppupScript, registry: ProcessRegistry) {
        OtpLogging.log(OtpLogLevel.Info, ctx,
            "running upgrade ${script.fromVersion} -> ${script.toVersion}")
        for (instruction in script.upgrade) {
            applyInstruction(instruction, registry)
        }
    }

    /**
     * Run the downgrade instructions in [script] against [registry].
     */
    suspend fun runDowngrade(script: AppupScript, registry: ProcessRegistry) {
        OtpLogging.log(OtpLogLevel.Info, ctx,
            "running downgrade ${script.toVersion} -> ${script.fromVersion}")
        for (instruction in script.downgrade) {
            applyInstruction(instruction, registry)
        }
    }

    private suspend fun applyInstruction(instruction: AppupInstruction, registry: ProcessRegistry) {
        runCatching {
            when (instruction) {
                is AppupInstruction.UpdateActor -> {
                    val ref = registry.lookup(instruction.name)
                        ?: error("actor '${instruction.name}' not found for UpdateActor")
                    ref.sysCodeChange(instruction.newClass, instruction.oldVersion, instruction.newVersion)
                    OtpLogging.log(OtpLogLevel.Info, ctx,
                        "UpdateActor '${instruction.name}': ${instruction.oldVersion} -> ${instruction.newVersion}")
                }
                is AppupInstruction.RestartActor -> {
                    val ref = registry.lookup(instruction.name)
                    ref?.stop()
                    OtpLogging.log(OtpLogLevel.Info, ctx, "RestartActor '${instruction.name}': stopped (supervisor restarts)")
                }
                is AppupInstruction.LoadModule -> {
                    // No-op on JVM: class is already loaded by the classloader
                    OtpLogging.log(OtpLogLevel.Info, ctx, "LoadModule '${instruction.name}': no-op on JVM")
                }
                is AppupInstruction.AddActor -> {
                    OtpLogging.log(OtpLogLevel.Info, ctx,
                        "AddActor to '${instruction.supervisorName}': spec=${instruction.spec.id}")
                }
                is AppupInstruction.RemoveActor -> {
                    OtpLogging.log(OtpLogLevel.Info, ctx,
                        "RemoveActor '${instruction.childId}' from '${instruction.supervisorName}'")
                }
                AppupInstruction.RestartApplication -> {
                    OtpLogging.log(OtpLogLevel.Info, ctx, "RestartApplication: signal ApplicationController")
                }
            }
        }.onFailure { t ->
            OtpLogging.log(OtpLogLevel.Error, ctx, "instruction failed: $instruction — ${t.message}", t)
        }
    }
}
