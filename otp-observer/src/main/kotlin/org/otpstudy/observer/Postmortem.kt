package org.otpstudy.observer

import java.time.Instant

/**
 * JVM-oriented runtime snapshot (see [THE_HADAL_ZONE.md] §8). Not Erlang crash dumps.
 */
data class JvmSummary(
    val javaVersion: String,
    val maxMemoryBytes: Long,
    val totalMemoryBytes: Long,
    val freeMemoryBytes: Long,
)

data class RuntimeSnapshot(
    val capturedAt: Instant,
    val jvm: JvmSummary,
    val processes: List<ProcessInfo>,
)

object Postmortem {
    fun capture(now: Instant = Instant.now()): RuntimeSnapshot {
        val rt = Runtime.getRuntime()
        return RuntimeSnapshot(
            capturedAt = now,
            jvm = JvmSummary(
                javaVersion = System.getProperty("java.version") ?: "?",
                maxMemoryBytes = rt.maxMemory(),
                totalMemoryBytes = rt.totalMemory(),
                freeMemoryBytes = rt.freeMemory(),
            ),
            processes = ProcessTable.all(),
        )
    }
}
