package org.otpstudy.memory

import java.lang.foreign.Arena
import java.lang.foreign.MemoryLayout
import java.lang.foreign.MemorySegment
import java.lang.foreign.SegmentAllocator
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * Off-heap memory arena scoped to a single actor's lifetime.
 *
 * State allocated here lives outside the GC heap. When the actor's Job completes
 * (normally, cancelled, or crashed), [close] is called and all allocations are freed
 * deterministically — without waiting for a GC cycle.
 *
 * This approximates OTP's per-process heap for performance-critical data.
 * Lifetime management is deterministic and isolated to the actor that owns the arena.
 *
 * Usage: pass `withArena = true` to `GenServers.startLink`. Inside any callback,
 * retrieve via `coroutineContext[ActorArena]?.allocate(layout)`.
 *
 * OTP source: erts/emulator/beam/erl_alloc.c — ERTS_ALC_T_HEAP
 */
class ActorArena : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<ActorArena>
    override val key get() = Key

    private val arena: Arena = Arena.ofShared()
    val allocator: SegmentAllocator get() = arena

    private val _bytesAllocated = AtomicLong(0L)
    val bytesAllocated: Long get() = _bytesAllocated.get()

    fun allocate(layout: MemoryLayout): MemorySegment {
        val seg = arena.allocate(layout)
        _bytesAllocated.addAndGet(layout.byteSize())
        return seg
    }

    fun allocateBytes(byteSize: Long): MemorySegment {
        val seg = arena.allocate(byteSize)
        _bytesAllocated.addAndGet(byteSize)
        return seg
    }

    fun close() {
        runCatching { arena.close() }
    }
}
