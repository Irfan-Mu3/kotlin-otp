package org.otpstudy.jobs

import java.util.TreeMap

/**
 * In-memory queue — mirrors `jobs_queue` fifo/lifo behaviour using ordered timestamps.
 */
internal class JobsQueueStorage {
    private val fifo = TreeMap<Long, QueueEntry>()
    private val lifoStack = ArrayDeque<QueueEntry>()

    fun isFifo(type: QueueType): Boolean =
        when (type) {
            is QueueType.Fifo, is QueueType.Passive -> true
            is QueueType.Lifo -> false
            is QueueType.Action -> true
        }

    fun enqueue(entry: QueueEntry, lifo: Boolean) {
        if (lifo) {
            lifoStack.addLast(entry)
        } else {
            fifo[entry.timestampUs] = entry
        }
    }

    fun peekOldest(): Long? =
        when {
            fifo.isNotEmpty() -> fifo.firstKey()
            lifoStack.isNotEmpty() -> lifoStack.first().timestampUs
            else -> null
        }

    fun size(): Int = fifo.size + lifoStack.size

    fun poll(n: Int, lifo: Boolean): List<QueueEntry> {
        if (n <= 0) return emptyList()
        val out = mutableListOf<QueueEntry>()
        if (lifo) {
            repeat(minOf(n, lifoStack.size)) {
                lifoStack.removeLastOrNull()?.let(out::add)
            }
        } else {
            val keys = fifo.keys.take(n).toList()
            for (k in keys) {
                fifo.remove(k)?.let(out::add)
            }
        }
        return out
    }

    fun all(): List<QueueEntry> {
        val fifoEntries = fifo.values.toList()
        return fifoEntries + lifoStack.toList()
    }

    fun clear() {
        fifo.clear()
        lifoStack.clear()
    }

    fun findExpired(nowUs: Long, maxTimeUs: Long): List<QueueEntry> {
        val threshold = maxTimeUs
        val expired = mutableListOf<QueueEntry>()
        if (fifo.isNotEmpty()) {
            val iter = fifo.entries.iterator()
            while (iter.hasNext()) {
                val e = iter.next()
                if (nowUs - e.key > threshold) {
                    expired.add(e.value)
                    iter.remove()
                } else {
                    break
                }
            }
        }
        return expired
    }
}
