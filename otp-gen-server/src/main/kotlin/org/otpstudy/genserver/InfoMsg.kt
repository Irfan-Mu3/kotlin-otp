package org.otpstudy.genserver

/**
 * Base type for info messages delivered to [GenServer.handleInfo].
 *
 * Implement this interface on your own message types to participate in [handleInfo]:
 *
 * ```kotlin
 * sealed interface MyMsg : InfoMsg {
 *     data object Tick : MyMsg
 *     data class Data(val payload: String) : MyMsg
 * }
 * ```
 *
 * Library-provided subtypes ([Down], [TimerTick], [InternalControl]) are always available.
 *
 * JVM/OTP difference: In OTP, `handle_info` receives any Erlang term that is not a
 * `$gen_call` / `$gen_cast`. Here, only messages explicitly sent as [GenServerMsg.Info]
 * reach `handleInfo`; there is no implicit catch-all for stray terms.
 */
interface InfoMsg

/**
 * Analogous to OTP `'DOWN'` monitor signal: the monitored job/process completed.
 *
 * Delivered when you install a [`ProcessMonitor`](../../otp-core) and the watched
 * job finishes. See `linkJobs` for the link-style variant.
 */
data class Down(
    /** Identity of the monitored process/job (e.g. [org.otpstudy.core.OtpProcessId] or a `Job`). */
    val id: Any,
    /** Completion reason; `null` means normal exit. */
    val reason: Throwable?,
) : InfoMsg

/** Fired by an internal timer registered through the server's own timer mechanism. */
data class TimerTick(val ref: Any) : InfoMsg

/**
 * Internal control signal delivered via the high-priority control channel.
 * Use this to interrupt the server's main mailbox processing for urgent operations.
 *
 * JVM/OTP difference: This is not BEAM selective receive; the control channel is
 * drained with `tryReceive` before each main-mailbox message, providing a best-effort
 * priority guarantee, not a hard guarantee.
 */
data class InternalControl(val payload: Any) : InfoMsg

data class DeadLetterMsg(
    val original: Any,
    val target: org.otpstudy.core.OtpProcessId,
) : InfoMsg
