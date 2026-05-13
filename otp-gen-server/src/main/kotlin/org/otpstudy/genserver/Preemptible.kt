package org.otpstudy.genserver

import kotlinx.coroutines.currentCoroutineContext

/**
 * Marks a class or function for automatic reduction injection by the IR compiler plugin.
 *
 * When the `org.otpstudy.reduction-plugin` Gradle plugin is applied, the compiler
 * transformer injects a [reduce] call at every loop back-edge in annotated code.
 *
 * Without the plugin, this annotation is a no-op — reductions are still counted if
 * you call [reduce] explicitly.
 *
 * OTP source: erts/emulator/beam/erl_process.c — process_main, the reduction loop
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Preemptible(val reductionsPerCall: Int = 1)

/**
 * Top-level convenience function: burn [n] reductions from the [ReductionBudget]
 * in the current coroutine context. No-op if no budget is installed.
 *
 * Injected automatically at loop back-edges by the compiler plugin when
 * `@Preemptible` is present. Can also be called manually in hot paths.
 */
suspend fun reduce(n: Int = 1) {
    currentCoroutineContext()[ReductionBudget]?.reduce(n)
}
