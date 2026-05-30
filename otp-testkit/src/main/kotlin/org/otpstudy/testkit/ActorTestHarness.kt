package org.otpstudy.testkit

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.genserver.GenServerRef

/**
 * Start a long-lived actor in tests, run [block], then stop the actor in `finally`.
 *
 * This prevents `runBlocking` from hanging on structured child actors that outlive
 * the test body.
 */
suspend fun <S, T> CoroutineScope.withGenServer(
    start: suspend CoroutineScope.() -> GenServerRef<S>,
    block: suspend (GenServerRef<S>) -> T,
): T {
    val ref = start()
    return try {
        block(ref)
    } finally {
        ref.stop()
    }
}

