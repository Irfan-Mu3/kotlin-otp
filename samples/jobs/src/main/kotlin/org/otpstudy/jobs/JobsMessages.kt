package org.otpstudy.jobs

import org.otpstudy.genserver.InfoMsg
import org.otpstudy.genserver.ReplyHandle

internal sealed class JobsRequest {
    data class Ask(val queue: String?) : JobsRequest()
    data class AddQueue(val spec: QueueSpec) : JobsRequest()
    data class DeleteQueue(val name: String) : JobsRequest()
    data class ModifyQueue(val name: String, val maxTimeMs: Long?, val maxSize: Int?) : JobsRequest()
    data class Enqueue(val queue: String?, val item: Any?) : JobsRequest()
    data class Dequeue(val queue: String?, val n: Int) : JobsRequest()
    data class QueueInfo(val name: String) : JobsRequest()
    data object InfoQueues : JobsRequest()
}

internal sealed class JobsCast {
    data class Done(val token: Long, val counters: List<CounterRelease>) : JobsCast()
}

internal sealed class JobsInfo : InfoMsg {
    data class CheckQueue(val name: String) : JobsInfo()
    data class RevisitQueue(val name: String) : JobsInfo()
    data class OwnerDown(val queueName: String) : JobsInfo()
    data class BorrowerDown(val token: Long) : JobsInfo()
}

internal data class JobsServerState(
    val queues: MutableMap<String, JobQueue> = linkedMapOf(),
    var defaultQueue: String? = null,
    val activeGrants: MutableMap<Long, ActiveGrant> = mutableMapOf(),
    var nextToken: Long = 1,
    val ownerHooks: MutableMap<String, kotlinx.coroutines.DisposableHandle> = mutableMapOf(),
)
