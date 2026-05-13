package org.otpstudy.mailbox

import kotlinx.coroutines.channels.Channel

/**
 * FIFO channel with a "save" list for deferred messages.
 *
 * Messages that do not match the current pattern are held in a saved list and
 * re-considered on the next [receive] call, preserving FIFO order within saved messages.
 *
 * Analogous to Erlang's selective receive (BEAM keeps one per-process queue with a scan
 * pointer; this implementation materialises the saved list explicitly and replays it).
 *
 * JVM / OTP difference: O(n) replay cost over saved messages is visible. Do not use for
 * hot paths with large message volumes. For priority separation, consider a bounded mailbox
 * with two channels instead.
 *
 * OTP source reference: erts/emulator/beam/erl_message.c (erts_msgq_peek_msg, erts_msgq_unlink_msg)
 */
class SelectiveMailbox<T>(private val channel: Channel<T>) {

    private val saved = ArrayDeque<T>()

    /**
     * Receive the next message matching [matches].
     * Non-matching messages are saved and reconsidered on future calls.
     */
    suspend fun receive(matches: (T) -> Boolean): T {
        val iter = saved.iterator()
        while (iter.hasNext()) {
            val msg = iter.next()
            if (matches(msg)) {
                iter.remove()
                return msg
            }
        }
        while (true) {
            val msg = channel.receive()
            if (matches(msg)) return msg
            saved.addLast(msg)
        }
    }

    /**
     * Flush all saved messages back to the channel (e.g., after a state change where
     * previously irrelevant messages become relevant again).
     */
    suspend fun flushSaved() {
        val toFlush = saved.toList()
        saved.clear()
        for (msg in toFlush) channel.send(msg)
    }

    /** Number of messages currently in the save queue (not yet matched). */
    val savedSize: Int get() = saved.size

    /** Peek at the first saved message without removing it. */
    fun peekSaved(): T? = saved.firstOrNull()
}
