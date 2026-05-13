package org.otpstudy.registry

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import kotlin.coroutines.CoroutineContext

/**
 * Registry lifecycle helpers: start a [GenServer], register it, and auto-unregister
 * when the server's job completes.
 *
 * ### Pattern A — [startAndRegister] (direct wiring)
 *
 * ```kotlin
 * val registry = ProcessRegistry()
 * val ref = RegistryLifecycle.startAndRegister(scope, "counter", CounterServer(), registry)
 * ```
 *
 * ### Pattern B — inline in ChildSpec (supervisor integration)
 *
 * The supervisor restarts the child by re-running the start lambda. Re-invoking
 * [startAndRegister] from within the lambda automatically re-registers on restart:
 *
 * ```kotlin
 * val registry = ProcessRegistry()
 * ChildSpec("counter", Restart.Permanent, Shutdown.BrutalKill) {
 *     val ref = RegistryLifecycle.startAndRegister(this, "counter", CounterServer(), registry)
 *     ref.job.join()
 * }
 * ```
 *
 * JVM/OTP difference: OTP processes re-register on restart because a new PID replaces
 * the old one in the global name table atomically. Here, the supervisor's ChildSpec lambda
 * runs again on restart — the auto-unregister hook (installed by [ProcessRegistry.register])
 * removes the old entry, and the new call to [startAndRegister] registers the fresh ref.
 */
object RegistryLifecycle {

    /**
     * Start [server] under [parent], register it as [name] in [registry], and return the ref.
     *
     * The registry entry is removed automatically when the server job completes
     * (via the hook installed by [ProcessRegistry.register]).
     */
    fun <S> startAndRegister(
        parent: CoroutineScope,
        name: String,
        server: GenServer<S>,
        registry: ProcessRegistry,
        context: CoroutineContext = Dispatchers.Default,
    ): GenServerRef<S> {
        val ref = GenServers.startLink(parent, server, context, name)
        registry.register(name, ref)
        return ref
    }
}
