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
     * Record the current saved-list boundary as a scan mark.
     *
     * Use together with [receiveFrom] to skip messages that arrived before the mark.
     * This mirrors OTP's recv_mark optimization: a reply to a call can only arrive
     * after the call was sent, so all messages before the mark are irrelevant and
     * can be skipped in O(1) rather than re-scanned.
     *
     * Typical usage:
     * ```
     * val m = mailbox.mark()        // snapshot before dispatching the request
     * channel.send(request)
     * val reply = mailbox.receiveFrom(m) { it is Reply && it.ref == ref }
     * ```
     */
    fun mark(): Int = saved.size

    /**
     * Receive the next message matching [matches], scanning only saved messages
     * at index >= [from] and any new channel arrivals.
     *
     * Messages in saved[0..<from] are never examined, giving O(k) cost where k
     * is the number of messages that arrived after [mark] was called rather than
     * O(total saved). Non-matching channel messages are appended to saved as usual.
     */
    suspend fun receiveFrom(from: Int, matches: (T) -> Boolean): T {
        var i = from
        while (i < saved.size) {
            val msg = saved[i]
            if (matches(msg)) {
                saved.removeAt(i)
                return msg
            }
            i++
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
