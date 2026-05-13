package org.otpstudy.supervisor

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.core.ChildType
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import kotlin.time.Duration.Companion.seconds

private fun <S> genServerStart(server: GenServer<S>): suspend CoroutineScope.() -> Unit =
    {
        val ref = GenServers.startLink(this, server)
        ref.job.join()
    }

/**
 * Builds a [ChildSpec] that runs a [GenServer] until its job completes.
 */
fun <S> genServerChild(
    id: String,
    restart: Restart,
    shutdown: Shutdown = Shutdown.Timeout(5.seconds),
    type: ChildType = ChildType.Worker,
    server: GenServer<S>,
): ChildSpec =
    ChildSpec(
        id = id,
        restart = restart,
        shutdown = shutdown,
        type = type,
        start = genServerStart(server),
    )
