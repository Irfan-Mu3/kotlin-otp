package org.otpstudy.hotcode

import org.otpstudy.genserver.GenServer
import org.otpstudy.supervisor.ChildSpec

/**
 * A single instruction in a formal upgrade or downgrade script.
 *
 * Mirrors the instruction terms in an OTP `.appup` file:
 *   {load_module, Mod}, {update, Mod, {advanced, Extra}}, {restart_application, App}, etc.
 *
 * OTP source: lib/sasl/src/release_handler.erl — do_upgrade/2, do_downgrade/2
 */
sealed class AppupInstruction {
    /** Reload the named module in-place. No-op on the JVM (class already loaded). */
    data class LoadModule(val name: String) : AppupInstruction()

    /** Call [GenServer.codeChange] on the named actor; state migrates to [newVersion]. */
    data class UpdateActor(
        val name: String,
        val oldVersion: String,
        val newVersion: String,
        val newClass: Class<out GenServer<*>>,
    ) : AppupInstruction()

    /** Restart the named actor entirely (state is lost; supervisor re-inits with [newClass]). */
    data class RestartActor(val name: String) : AppupInstruction()

    /** Add a new child to the named DynamicSupervisor. */
    data class AddActor(val supervisorName: String, val spec: ChildSpec) : AppupInstruction()

    /** Remove a child from the named DynamicSupervisor. */
    data class RemoveActor(val supervisorName: String, val childId: String) : AppupInstruction()

    /** Restart the whole application (signals ApplicationController). */
    data object RestartApplication : AppupInstruction()
}

/**
 * An upgrade/downgrade script spanning two application versions.
 *
 * Analogous to an OTP `.appup` file:
 * ```erlang
 * {"2.0", [{"1.0", [...upgrade instructions...]}], [{"1.0", [...downgrade instructions...]}]}.
 * ```
 */
data class AppupScript(
    val fromVersion: String,
    val toVersion: String,
    val upgrade: List<AppupInstruction>,
    val downgrade: List<AppupInstruction> = emptyList(),
)
