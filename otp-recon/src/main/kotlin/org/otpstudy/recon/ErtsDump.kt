package org.otpstudy.recon

import java.lang.reflect.Modifier
import java.util.Collections
import java.util.IdentityHashMap

/**
 * Approximate the deep size of an object graph — analogous to erts_debug:size/1.
 *
 * Uses a breadth-first traversal via reflection, skipping already-visited nodes.
 * Returns an estimate in bytes; does not account for JVM object header overhead precisely
 * but is accurate enough for ranking processes by memory footprint.
 *
 * OTP source: erts/emulator/beam/erl_debug.c — size_object/1
 */
object ErtsDump {
    fun size(obj: Any?): Long {
        if (obj == null) return 0L
        val visited = Collections.newSetFromMap(IdentityHashMap<Any, Boolean>())
        val queue = ArrayDeque<Any>()
        queue.add(obj)
        var total = 0L
        while (queue.isNotEmpty()) {
            val current = queue.removeFirst()
            if (!visited.add(current)) continue
            total += shallowSizeOf(current)
            for (child in objectChildren(current)) queue.add(child)
        }
        return total
    }

    private fun shallowSizeOf(obj: Any): Long = when (obj) {
        is ByteArray    -> obj.size.toLong() + 16L
        is IntArray     -> obj.size * 4L + 16L
        is LongArray    -> obj.size * 8L + 16L
        is DoubleArray  -> obj.size * 8L + 16L
        is Array<*>     -> obj.size * 8L + 16L
        is String       -> obj.length * 2L + 40L
        else            -> 16L
    }

    private fun objectChildren(obj: Any): List<Any> {
        if (obj is Array<*>) return obj.filterNotNull()
        val fields = allInstanceFields(obj.javaClass)
        return fields.mapNotNull { f ->
            runCatching {
                f.isAccessible = true
                val v = f.get(obj)
                if (v != null && v !is Class<*>) v else null
            }.getOrNull()
        }
    }

    private fun allInstanceFields(cls: Class<*>): List<java.lang.reflect.Field> = buildList {
        var c: Class<*>? = cls
        while (c != null && c != Any::class.java) {
            addAll(c.declaredFields.filter { !Modifier.isStatic(it.modifiers) })
            c = c.superclass
        }
    }
}
