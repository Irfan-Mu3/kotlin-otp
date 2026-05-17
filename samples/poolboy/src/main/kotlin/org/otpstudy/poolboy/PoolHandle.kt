package org.otpstudy.poolboy

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Job
import org.otpstudy.genserver.GenServerRef

/**
 * Borrow / return / introspect a pool whether it is held locally or reached over
 * [org.otpstudy.distribution.NodeTransport].
 */
interface PoolHandle<W> {
    val poolName: String?

    suspend fun checkout(
        block: Boolean = true,
        timeout: Duration = 5.seconds,
        borrower: Job? = null,
    ): PooledWorker<W>?

    fun checkin(worker: PooledWorker<W>)

    suspend fun status(): PoolStatus

    suspend fun stop()
}

/**
 * Borrow-then-return helper for any [PoolHandle].
 */
suspend fun <W, T> PoolHandle<W>.transaction(
    timeout: Duration = 5.seconds,
    block: suspend (PooledWorker<W>) -> T,
): T {
    val worker =
        checkout(block = true, timeout = timeout)
            ?: error("pool returned null worker for blocking checkout")
    var primary: Throwable? = null
    try {
        return block(worker)
    } catch (t: Throwable) {
        primary = t
        throw t
    } finally {
        try {
            worker.checkin()
        } catch (t: Throwable) {
            if (primary == null) throw t else primary.addSuppressed(t)
        }
    }
}
