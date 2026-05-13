package org.otpstudy.mailbox

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel

/**
 * Single-owner mailbox built on a [Channel]. Not BEAM selective receive;
 * ordering is FIFO with optional backpressure via bounded capacity.
 */
class ProcessMailbox<T>(
    capacity: Int = Channel.UNLIMITED,
) {
    private val channel = Channel<T>(capacity)

    val asReceiveChannel: ReceiveChannel<T> get() = channel

    val asSendChannel: SendChannel<T> get() = channel

    suspend fun send(element: T) {
        channel.send(element)
    }

    fun trySend(element: T) = channel.trySend(element)

    fun close(cause: Throwable? = null): Boolean = channel.close(cause)
}
