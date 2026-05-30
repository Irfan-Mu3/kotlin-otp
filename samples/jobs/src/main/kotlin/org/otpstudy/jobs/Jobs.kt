package org.otpstudy.jobs

import kotlinx.coroutines.CoroutineScope

/**
 * Load regulator — port of [uwiger/jobs](https://github.com/uwiger/jobs) public API.
 *
 * OTP source: `jobs.erl`, `jobs_server.erl`
 */
object Jobs {
    private var installed: JobsRef? = null

    suspend fun startLink(scope: CoroutineScope, config: JobsConfig = JobsConfig()): JobsRef {
        installed?.ref?.stop()
        installed = null
        val ref = JobsServers.startLink(scope, config)
        installed = ref
        return ref
    }

    fun ref(): JobsRef = installed ?: error("jobs not started — call Jobs.startLink first")

    suspend fun ask(queue: String? = null): JobsResult<RegObj> = ref().ask(queue)

    suspend fun done(opaque: RegObj) = ref().done(opaque)

    suspend fun run(queue: String? = null, block: suspend () -> Unit) {
        val granted =
            when (val r = ask(queue)) {
                is JobsResult.Ok -> r.value
                is JobsResult.Error -> throw JobsException(r.reason)
            }
        try {
            block()
        } finally {
            done(granted)
        }
    }

    suspend fun <T> runReturning(queue: String? = null, block: suspend () -> T): T {
        val granted =
            when (val r = ask(queue)) {
                is JobsResult.Ok -> r.value
                is JobsResult.Error -> throw JobsException(r.reason)
            }
        return try {
            block()
        } finally {
            done(granted)
        }
    }

    suspend fun addQueue(name: String, options: QueueOptions) =
        ref().addQueue(QueueSpec(name, options))

    suspend fun deleteQueue(name: String): Boolean = ref().deleteQueue(name)

    suspend fun modifyQueue(name: String, maxTime: kotlin.time.Duration? = null, maxSize: Int? = null) =
        ref().modifyQueue(name, maxTime, maxSize)

    suspend fun enqueue(queue: String, item: Any?): JobsResult<Unit> = ref().enqueue(queue, item)

    suspend fun dequeue(queue: String, n: Int): List<Pair<Long, Any?>> = ref().dequeue(queue, n)

    suspend fun queueInfo(name: String): QueueInfo? = ref().queueInfo(name)

    internal fun resetForTests() {
        installed = null
    }
}

class JobsException(val reason: JobsError) : Exception("jobs: $reason")
