package org.otpstudy.application

import kotlinx.coroutines.CoroutineScope

/**
 * OTP [application](https://www.erlang.org/doc/design_principles/applications.html)-like lifecycle:
 * start returns a supervision root; stop tears it down (reverse order is handled by cancellation v0).
 */
interface OtpApplication {
    val name: String

    suspend fun start(parent: CoroutineScope): ApplicationStartResult

    suspend fun stop()
}
