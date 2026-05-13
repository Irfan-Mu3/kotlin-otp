package org.otpstudy.supervisor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import kotlin.time.Duration.Companion.seconds

/**
 * Opaque handle passed to bridge blocks for signalling the supervisor with a structured exit reason.
 * Analogous to OTP supervisor_bridge's ability to call exit/1 with a reason.
 */
class BridgeRef internal constructor(
    private val exitChannel: Channel<BridgeExit>,
) {
    /**
     * Signal the supervisor that this bridge is exiting with [reason].
     * Use [BridgeExit.Normal] for clean shutdown, [BridgeExit.Failure] for errors.
     */
    fun exit(reason: BridgeExit) {
        exitChannel.trySend(reason)
    }
}

sealed class BridgeExit {
    data object Normal : BridgeExit()
    data class Failure(val cause: Throwable) : BridgeExit()
}

/**
 * Wraps an arbitrary suspend block as a supervised child.
 *
 * The block receives a [BridgeRef] it can use to signal a clean or abnormal exit.
 * If the block simply returns normally, it is treated as [BridgeExit.Normal].
 * If it throws, it is treated as an abnormal exit (triggering restart per the child spec).
 *
 * Analogous to OTP [supervisor_bridge](https://www.erlang.org/doc/man/supervisor_bridge.html).
 * The main difference from a plain [ChildSpec]: [BridgeRef] makes the exit reason explicit
 * and integrates with the supervisor's restart semantics without requiring a GenServer shell.
 */
object SupervisorBridge {

    /**
     * Create a [ChildSpec] that wraps [block] as a bridge child.
     * [block] receives a [BridgeRef] for signalling exit reason.
     */
    fun childSpec(
        id: String,
        restart: Restart = Restart.Permanent,
        shutdown: Shutdown = Shutdown.Timeout(5.seconds),
        block: suspend CoroutineScope.(bridge: BridgeRef) -> Unit,
    ): ChildSpec {
        return ChildSpec(
            id = id,
            restart = restart,
            shutdown = shutdown,
            start = {
                val exitChannel = Channel<BridgeExit>(1)
                val bridgeRef = BridgeRef(exitChannel)
                block(bridgeRef)
            },
        )
    }
}
