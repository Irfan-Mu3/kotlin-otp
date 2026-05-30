package org.otpstudy.jobs

import kotlin.time.Duration
import kotlinx.coroutines.Job
import org.otpstudy.genserver.ReplyHandle

/** Public opaque returned from [Jobs.ask] — pass to [Jobs.done]. */
data class RegObj(
    internal val token: Long,
    internal val counters: List<CounterRelease> = emptyList(),
)

data class CounterRelease(val name: String, val increment: Int)

sealed class JobsResult<out T> {
    data class Ok<T>(val value: T) : JobsResult<T>()
    data class Error(val reason: JobsError) : JobsResult<Nothing>()
}

enum class JobsError {
    Rejected,
    Timeout,
    NotFound,
    Full,
    BadArg,
}

sealed class QueueType {
    data object Fifo : QueueType()
    data object Lifo : QueueType()
    data class Passive(val fifo: Boolean = true) : QueueType()
    data class Action(val approve: Boolean) : QueueType()
}

sealed class RegulatorSpec {
    data class Rate(val limitPerSecond: Int) : RegulatorSpec()
    data class Counter(val limit: Int, val increment: Int = 1) : RegulatorSpec()
}

data class QueueOptions(
    val type: QueueType = QueueType.Fifo,
    val regulators: List<RegulatorSpec> = emptyList(),
    val maxTime: Duration? = null,
    val maxSize: Int? = null,
    val linkOwner: Job? = null,
) {
    companion object {
        fun standardRate(limitPerSecond: Int): QueueOptions =
            QueueOptions(regulators = listOf(RegulatorSpec.Rate(limitPerSecond)))

        fun standardCounter(limit: Int): QueueOptions =
            QueueOptions(regulators = listOf(RegulatorSpec.Counter(limit)))

        fun approve(): QueueOptions = QueueOptions(type = QueueType.Action(approve = true))

        fun reject(): QueueOptions = QueueOptions(type = QueueType.Action(approve = false))

        fun passiveFifo(): QueueOptions = QueueOptions(type = QueueType.Passive(fifo = true))
    }
}

data class QueueSpec(val name: String, val options: QueueOptions)

data class JobsConfig(
    val queues: List<QueueSpec> = emptyList(),
    val defaultQueue: String? = null,
)

internal data class RateRegulator(
    val limitPerSecond: Int,
    var presetLimit: Int = limitPerSecond,
    var activeLimit: Int = limitPerSecond,
) {
    val intervalMs: Double?
        get() = if (activeLimit > 0) 1000.0 / activeLimit else null
}

internal data class CounterRegulator(
    val name: String,
    val limit: Int,
    val increment: Int,
    var value: Int = 0,
    val shared: Boolean = false,
    val queueOwners: MutableSet<String> = mutableSetOf(),
)

internal sealed class QueueEntry {
    abstract val timestampUs: Long

    data class Waiter(
        override val timestampUs: Long,
        val handle: ReplyHandle<JobsServerState>,
    ) : QueueEntry()

    data class PassiveItem(
        override val timestampUs: Long,
        val item: Any?,
    ) : QueueEntry()
}

internal data class JobQueue(
    val name: String,
    val type: QueueType,
    val rateRegulator: RateRegulator? = null,
    val counterRegulator: CounterRegulator? = null,
    /** Max wait in queue; compared as `(nowUs - oldestUs) > maxTimeMs * 1000` (OTP jobs). */
    var maxTimeMs: Long? = null,
    var maxSize: Int? = null,
    val linkOwner: Job? = null,
    val storage: JobsQueueStorage = JobsQueueStorage(),
    var latestDispatchUs: Long = 0,
    var approved: Long = 0,
    var queued: Int = 0,
    var oldestJobUs: Long? = null,
    var checkCounter: Int = 0,
    var empty: Boolean = true,
    var depleted: Boolean = false,
    val passiveWaiters: MutableList<ReplyHandle<JobsServerState>> = mutableListOf(),
)

internal data class ActiveGrant(
    val token: Long,
    val queueName: String,
    val counters: List<CounterRelease>,
    val callerJob: Job?,
)

data class QueueInfo(
    val name: String,
    val type: QueueType,
    val approved: Long,
    val queued: Int,
    val rateLimit: Int?,
    val counterLimit: Int?,
    val counterValue: Int?,
)
