package org.otpstudy.jobs

import org.otpstudy.genserver.GenServerRef

class JobsRef internal constructor(internal val ref: GenServerRef<JobsServerState>) {
    suspend fun ask(queue: String? = null): JobsResult<RegObj> =
        when (val r = ref.call<Any?>(JobsRequest.Ask(queue))) {
            is RegObj -> JobsResult.Ok(r)
            is JobsError -> JobsResult.Error(r)
            null -> JobsResult.Error(JobsError.BadArg)
            else -> JobsResult.Error(JobsError.BadArg)
        }

    suspend fun done(opaque: RegObj) {
        if (opaque.token == 0L) return
        ref.cast(JobsCast.Done(opaque.token, opaque.counters))
    }

    suspend fun addQueue(spec: QueueSpec) {
        ref.call<Unit>(JobsRequest.AddQueue(spec))
    }

    suspend fun deleteQueue(name: String): Boolean =
        ref.call(JobsRequest.DeleteQueue(name))

    suspend fun modifyQueue(name: String, maxTime: kotlin.time.Duration? = null, maxSize: Int? = null) {
        val maxTimeMs = maxTime?.inWholeMilliseconds
        ref.call<Unit>(JobsRequest.ModifyQueue(name, maxTimeMs, maxSize))
    }

    suspend fun enqueue(queue: String?, item: Any?): JobsResult<Unit> =
        when (val r = ref.call<Any?>(JobsRequest.Enqueue(queue, item))) {
            Unit -> JobsResult.Ok(Unit)
            JobsError.Full -> JobsResult.Error(JobsError.Full)
            JobsError.BadArg -> JobsResult.Error(JobsError.BadArg)
            else -> JobsResult.Error(JobsError.BadArg)
        }

    suspend fun dequeue(queue: String?, n: Int): List<Pair<Long, Any?>> =
        ref.call(JobsRequest.Dequeue(queue, n))

    suspend fun queueInfo(name: String): QueueInfo? =
        ref.call(JobsRequest.QueueInfo(name))

    suspend fun stop() {
        ref.stop()
    }
}
