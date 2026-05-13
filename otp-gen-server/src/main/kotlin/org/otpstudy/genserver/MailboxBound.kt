package org.otpstudy.genserver

import kotlinx.coroutines.channels.BufferOverflow

sealed class OverflowPolicy {
    data object Block : OverflowPolicy()
    data object DropOldest : OverflowPolicy()
    data object DropNew : OverflowPolicy()
    data object CrashSender : OverflowPolicy()
    data class DeadLetterTo(val sink: GenServerRef<*>) : OverflowPolicy()
}

data class MailboxBound(
    val capacity: Int,
    val policy: OverflowPolicy = OverflowPolicy.Block,
)

class MailboxFullException(message: String) : RuntimeException(message)
