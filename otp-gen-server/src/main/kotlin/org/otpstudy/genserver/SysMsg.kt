package org.otpstudy.genserver

import kotlinx.coroutines.CompletableDeferred
import org.otpstudy.core.OtpProcessId

sealed class SysMsg {
    data class GetState(val reply: CompletableDeferred<Any?>) : SysMsg()
    data class GetStatus(val reply: CompletableDeferred<SysStatus>) : SysMsg()
    data class ReplaceState(
        val transform: (Any?) -> Any?,
        val reply: CompletableDeferred<Any?>,
    ) : SysMsg()
    data class Suspend(val reply: CompletableDeferred<Unit>) : SysMsg()
    data class Resume(val reply: CompletableDeferred<Unit>) : SysMsg()
    /**
     * Apply a hot code upgrade synchronously via the sys channel.
     *
     * Analogous to OTP's `sys:change_code/4` which sends a `{system, From, {change_code, ...}}`
     * message processed in order by the sys handler, giving deterministic timing.
     * The registry-based polling ([CodeChangeRegistry]) is the batch variant for deployments;
     * this variant is for explicit, synchronous upgrade in tests and tooling.
     */
    data class CodeChange(
        val newClass: Class<out GenServer<*>>,
        val oldVersion: String,
        val newVersion: String,
        val reply: CompletableDeferred<Unit>,
    ) : SysMsg()
}

data class SysStatus(
    val id: OtpProcessId,
    val name: String?,
    val module: String,
    val state: Any?,
    val messageQueueLength: Int,
)
