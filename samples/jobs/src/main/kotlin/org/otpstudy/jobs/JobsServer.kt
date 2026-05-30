package org.otpstudy.jobs

import kotlin.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InfoMsg
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.OtpTimers
import org.otpstudy.genserver.ReplyHandle
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.TerminateReason
import org.otpstudy.genserver.TimerTick
import org.otpstudy.registry.GlobalProcessRegistry
import kotlin.time.Duration.Companion.milliseconds

internal class JobsServer(
    private val config: JobsConfig,
) : GenServer<JobsServerState> {
    private var selfRef: GenServerRef<JobsServerState>? = null
    private val checkTimers = mutableMapOf<String, org.otpstudy.genserver.TimerRef>()

    override suspend fun init(self: GenServerRef<JobsServerState>): InitResult<JobsServerState> {
        selfRef = self
        val state = JobsServerState()
        for (spec in config.queues) {
            addQueueToState(state, spec)
        }
        state.defaultQueue = config.defaultQueue ?: config.queues.firstOrNull()?.name
        return InitResult.Ok(state)
    }

    override suspend fun handleCall(request: Any, state: JobsServerState): ReplyResult<JobsServerState> =
        ReplyResult.Reply(JobsError.BadArg, state)

    override suspend fun handleCallFrom(
        request: Any,
        state: JobsServerState,
        from: ReplyHandle<JobsServerState>,
    ): ReplyResult<JobsServerState> =
        when (val req = request) {
            is JobsRequest.Ask -> handleAsk(req.queue, state, from)
            is JobsRequest.AddQueue -> handleAddQueue(req.spec, state)
            is JobsRequest.DeleteQueue -> handleDeleteQueue(req.name, state)
            is JobsRequest.ModifyQueue -> handleModifyQueue(req, state)
            is JobsRequest.Enqueue -> handleEnqueue(req, state)
            is JobsRequest.Dequeue -> handleDequeue(req, state, from)
            is JobsRequest.QueueInfo -> {
                val info = state.queues[req.name]?.toInfo()
                ReplyResult.Reply(info, state)
            }
            is JobsRequest.InfoQueues ->
                ReplyResult.Reply(state.queues.keys.toList(), state)
            else -> ReplyResult.Reply(JobsError.BadArg, state)
        }

    override suspend fun handleCast(request: Any, state: JobsServerState): NoreplyResult<JobsServerState> {
        if (request is JobsCast.Done) {
            val s1 = restoreCounters(state, request.counters)
            state.activeGrants.remove(request.token)
            return NoreplyResult.Noreply(revisitAll(s1))
        }
        return NoreplyResult.Noreply(state)
    }

    override suspend fun handleInfo(msg: InfoMsg, state: JobsServerState): NoreplyResult<JobsServerState> =
        when (msg) {
            is JobsInfo.CheckQueue -> {
                val q = state.queues[msg.name] ?: return NoreplyResult.Noreply(state)
                checkTimers.remove(msg.name)?.cancel()
                val (s1, _) = performQueueCheck(q, state)
                NoreplyResult.Noreply(s1)
            }
            is JobsInfo.RevisitQueue -> {
                val q = state.queues[msg.name] ?: return NoreplyResult.Noreply(state)
                val (s1, _) = performQueueCheck(q, state)
                NoreplyResult.Noreply(s1)
            }
            is JobsInfo.OwnerDown -> {
                val q = state.queues.remove(msg.queueName) ?: return NoreplyResult.Noreply(state)
                rejectAllWaiters(q, state)
                state.ownerHooks.remove(msg.queueName)?.dispose()
                NoreplyResult.Noreply(state)
            }
            is JobsInfo.BorrowerDown -> {
                val grant = state.activeGrants.remove(msg.token) ?: return NoreplyResult.Noreply(state)
                val s1 = restoreCounters(state, grant.counters)
                NoreplyResult.Noreply(revisitAll(s1))
            }
            is TimerTick -> NoreplyResult.Noreply(state)
            else -> NoreplyResult.Noreply(state)
        }

    override suspend fun terminate(reason: TerminateReason, state: JobsServerState) {
        checkTimers.values.forEach { it.cancel() }
        for (q in state.queues.values) {
            rejectAllWaiters(q, state)
        }
    }

    private fun handleAsk(
        queueName: String?,
        state: JobsServerState,
        from: ReplyHandle<JobsServerState>,
    ): ReplyResult<JobsServerState> {
        val name = resolveQueue(queueName, state) ?: return ReplyResult.Reply(JobsError.BadArg, state)
        val q = state.queues[name] ?: return ReplyResult.Reply(JobsError.BadArg, state)
        return when (val t = q.type) {
            is QueueType.Action -> {
                if (!t.approve) {
                    return ReplyResult.Reply(JobsError.Rejected, state)
                }
                q.approved++
                ReplyResult.Reply(RegObj(token = 0), state)
            }
            is QueueType.Passive -> ReplyResult.Reply(JobsError.BadArg, state)
            else -> {
                val ts = JobsTimestamp.now()
                val maxSz = q.maxSize
                val atCapacity = maxSz != null && q.storage.size() >= maxSz
                if (atCapacity) {
                    return ReplyResult.Reply(JobsError.Rejected, state)
                }
                val lifo = !q.storage.isFifo(q.type)
                q.storage.enqueue(QueueEntry.Waiter(ts, from), lifo)
                q.queued++
                q.oldestJobUs = minOf(q.oldestJobUs ?: ts, ts)
                q.empty = false
                val (s1, immediate) = performQueueCheck(q, state, eager = from)
                if (immediate != null) {
                    ReplyResult.Reply(immediate, s1)
                } else {
                    ReplyResult.DeferReply(from, s1)
                }
            }
        }
    }

    private fun handleAddQueue(spec: QueueSpec, state: JobsServerState): ReplyResult<JobsServerState> {
        if (state.queues.containsKey(spec.name)) {
            return ReplyResult.Reply(JobsError.BadArg, state)
        }
        addQueueToState(state, spec)
        state.defaultQueue = state.defaultQueue ?: spec.name
        selfRef?.let { scheduleCheck(it, spec.name, state) }
        return ReplyResult.Reply(Unit, revisitAll(state))
    }

    private fun handleDeleteQueue(name: String, state: JobsServerState): ReplyResult<JobsServerState> {
        val q = state.queues.remove(name) ?: return ReplyResult.Reply(false, state)
        rejectAllWaiters(q, state)
        checkTimers.remove(name)?.cancel()
        state.ownerHooks.remove(name)?.dispose()
        if (state.defaultQueue == name) {
            state.defaultQueue = state.queues.keys.firstOrNull()
        }
        return ReplyResult.Reply(true, state)
    }

    private fun handleModifyQueue(req: JobsRequest.ModifyQueue, state: JobsServerState): ReplyResult<JobsServerState> {
        val q = state.queues[req.name] ?: return ReplyResult.Reply(JobsError.NotFound, state)
        if (req.maxTimeMs != null) q.maxTimeMs = req.maxTimeMs
        if (req.maxSize != null) q.maxSize = req.maxSize
        return ReplyResult.Reply(Unit, state)
    }

    private fun handleEnqueue(req: JobsRequest.Enqueue, state: JobsServerState): ReplyResult<JobsServerState> {
        val name = resolveQueue(req.queue, state) ?: return ReplyResult.Reply(JobsError.BadArg, state)
        val q = state.queues[name] ?: return ReplyResult.Reply(JobsError.BadArg, state)
        val passive = q.type as? QueueType.Passive
            ?: return ReplyResult.Reply(JobsError.BadArg, state)
        val maxSz = q.maxSize
        if (maxSz != null && q.storage.size() >= maxSz) {
            return ReplyResult.Reply(JobsError.Full, state)
        }
        if (q.passiveWaiters.isNotEmpty()) {
            val waiter = q.passiveWaiters.removeAt(0)
            val ts = JobsTimestamp.now()
            waiter.reply(listOf(ts to req.item))
            return ReplyResult.Reply(Unit, state)
        }
        val ts = JobsTimestamp.now()
        val lifo = !passive.fifo
        q.storage.enqueue(QueueEntry.PassiveItem(ts, req.item), lifo)
        q.queued++
        q.oldestJobUs = minOf(q.oldestJobUs ?: ts, ts)
        q.empty = false
        return ReplyResult.Reply(Unit, state)
    }

    private fun handleDequeue(
        req: JobsRequest.Dequeue,
        state: JobsServerState,
        from: ReplyHandle<JobsServerState>,
    ): ReplyResult<JobsServerState> {
        val name = resolveQueue(req.queue, state) ?: return ReplyResult.Reply(JobsError.BadArg, state)
        val q = state.queues[name] ?: return ReplyResult.Reply(JobsError.BadArg, state)
        if (q.type !is QueueType.Passive) {
            return ReplyResult.Reply(JobsError.BadArg, state)
        }
        val lifo = !(q.type as QueueType.Passive).fifo
        val items = q.storage.poll(req.n, lifo).filterIsInstance<QueueEntry.PassiveItem>()
        if (items.isEmpty()) {
            q.passiveWaiters.add(from)
            return ReplyResult.DeferReply(from, state)
        }
        val result = items.map { it.timestampUs to it.item }
        q.queued = maxOf(0, q.queued - items.size)
        q.oldestJobUs = q.storage.peekOldest()
        if (q.storage.size() == 0) q.empty = true
        return ReplyResult.Reply(result, state)
    }

    private fun performQueueCheck(
        q: JobQueue,
        state: JobsServerState,
        eager: ReplyHandle<JobsServerState>? = null,
    ): Pair<JobsServerState, Any?> {
        val now = JobsTimestamp.now()
        var queue = q
        queue = expireTimedOut(queue, now, state)
        val n = computeDispatchCount(queue, now)
        if (n <= 0) {
            scheduleCheckIfNeeded(queue)
            state.queues[queue.name] = queue
            return state to null
        }
        val lifo = !queue.storage.isFifo(queue.type)
        val entries = queue.storage.poll(n, lifo).filterIsInstance<QueueEntry.Waiter>()
        var s = state
        var immediate: Any? = null
        for (entry in entries) {
            val (reg, s2) = dispatchWaiter(queue, entry, now, s)
            s = s2
            queue = s.queues[queue.name] ?: break
            if (entry.handle === eager) immediate = reg
        }
        queue.checkCounter = 0
        queue.queued = queue.storage.size()
        queue.oldestJobUs = queue.storage.peekOldest()
        queue.empty = queue.storage.size() == 0
        s.queues[queue.name] = queue
        scheduleCheckIfNeeded(queue)
        return s to immediate
    }

    private fun expireTimedOut(q: JobQueue, now: Long, @Suppress("UNUSED_PARAMETER") state: JobsServerState): JobQueue {
        val maxUs = q.maxTimeMs?.times(1000) ?: return q
        val expired = q.storage.findExpired(now, maxUs)
        for (entry in expired) {
            when (entry) {
                is QueueEntry.Waiter -> {
                    entry.handle.reply(JobsError.Timeout)
                    q.queued = maxOf(0, q.queued - 1)
                }
                is QueueEntry.PassiveItem -> q.queued = maxOf(0, q.queued - 1)
            }
        }
        q.oldestJobUs = q.storage.peekOldest()
        if (q.storage.size() == 0) q.empty = true
        return q
    }

    private fun computeDispatchCount(q: JobQueue, now: Long): Int {
        if (q.type is QueueType.Action || q.type is QueueType.Passive) return 0
        if (q.storage.size() == 0) return 0
        var n = Int.MAX_VALUE
        q.rateRegulator?.let { rate ->
            val intervalMs = rate.intervalMs
            if (intervalMs == null || intervalMs <= 0) {
                n = 0
            } else {
                val allowed =
                    if (q.latestDispatchUs == 0L) {
                        1
                    } else {
                        val elapsedUs = now - q.latestDispatchUs
                        (elapsedUs / (intervalMs * 1000)).toInt()
                    }
                n = minOf(n, maxOf(allowed, 0))
            }
        }
        q.counterRegulator?.let { counter ->
            val available = (counter.limit - counter.value) / counter.increment
            n = minOf(n, maxOf(available, 0))
        }
        if (q.rateRegulator == null && q.counterRegulator == null) {
            n = q.storage.size()
        }
        return minOf(n, q.storage.size())
    }

    private fun dispatchWaiter(
        q: JobQueue,
        entry: QueueEntry.Waiter,
        now: Long,
        state: JobsServerState,
    ): Pair<RegObj, JobsServerState> {
        val counters = mutableListOf<CounterRelease>()
        q.counterRegulator?.let { c ->
            c.value += c.increment
            counters.add(CounterRelease(c.name, c.increment))
        }
        val token = state.nextToken++
        val regObj = RegObj(token = token, counters = counters.toList())
        state.activeGrants[token] =
            ActiveGrant(
                token = token,
                queueName = q.name,
                counters = counters.toList(),
                callerJob = entry.handle.callerJob,
            )
        entry.handle.callerJob?.invokeOnCompletion {
            selfRef?.sendInfo(JobsInfo.BorrowerDown(token))
        }
        entry.handle.reply(regObj)
        q.approved++
        q.latestDispatchUs = now
        q.queued = maxOf(0, q.queued - 1)
        state.queues[q.name] = q
        return regObj to state
    }

    private fun restoreCounters(state: JobsServerState, releases: List<CounterRelease>): JobsServerState {
        for (rel in releases) {
            for (q in state.queues.values) {
                val c = q.counterRegulator ?: continue
                if (c.name == rel.name) {
                    c.value = maxOf(0, c.value - rel.increment)
                }
            }
        }
        return state
    }

    private fun revisitAll(state: JobsServerState): JobsServerState {
        var s = state
        for (name in s.queues.keys.toList()) {
            val q = s.queues[name] ?: continue
            if (q.type is QueueType.Action || q.type is QueueType.Passive) continue
            s = performQueueCheck(q, s).first
        }
        return s
    }

    private fun rejectAllWaiters(q: JobQueue, state: JobsServerState) {
        val lifo = !q.storage.isFifo(q.type)
        val all = q.storage.all()
        q.storage.clear()
        for (entry in all) {
            if (entry is QueueEntry.Waiter) {
                entry.handle.reply(JobsError.Rejected)
            }
        }
        for (w in q.passiveWaiters) {
            w.reply(JobsError.Rejected)
        }
        q.passiveWaiters.clear()
    }

    private fun resolveQueue(name: String?, state: JobsServerState): String? =
        name ?: state.defaultQueue

    private fun addQueueToState(state: JobsServerState, spec: QueueSpec) {
        val opts = spec.options
        var rate: RateRegulator? = null
        var counter: CounterRegulator? = null
        for (reg in opts.regulators) {
            when (reg) {
                is RegulatorSpec.Rate -> rate = RateRegulator(reg.limitPerSecond)
                is RegulatorSpec.Counter ->
                    counter =
                        CounterRegulator(
                            name = "${spec.name}_counter",
                            limit = reg.limit,
                            increment = reg.increment,
                        )
            }
        }
        if (rate != null && counter == null) {
            // allow rate-only
        }
        val q =
            JobQueue(
                name = spec.name,
                type = opts.type,
                rateRegulator = rate,
                counterRegulator = counter,
                maxTimeMs = opts.maxTime?.inWholeMilliseconds,
                maxSize = opts.maxSize,
                linkOwner = opts.linkOwner,
            )
        opts.linkOwner?.invokeOnCompletion {
            selfRef?.sendInfo(JobsInfo.OwnerDown(spec.name))
        }?.let { state.ownerHooks[spec.name] = it }
        state.queues[spec.name] = q
    }

    private fun scheduleCheck(ref: GenServerRef<JobsServerState>, name: String, state: JobsServerState) {
        val q = state.queues[name] ?: return
        scheduleCheckIfNeeded(q, ref)
    }

    private fun scheduleCheckIfNeeded(q: JobQueue, ref: GenServerRef<JobsServerState>? = selfRef) {
        val r = ref ?: return
        if (q.type is QueueType.Action || q.type is QueueType.Passive) return
        if (q.storage.size() == 0 && q.maxTimeMs == null) {
            checkTimers.remove(q.name)?.cancel()
            return
        }
        checkTimers[q.name]?.cancel()
        val delayMs =
            when {
                q.maxTimeMs != null && q.oldestJobUs != null -> {
                    val now = JobsTimestamp.now()
                    val elapsedMs = (now - q.oldestJobUs!!) / 1000
                    val maxMs = q.maxTimeMs!!
                    maxOf(0L, maxMs - elapsedMs).coerceAtMost(maxMs)
                }
                q.rateRegulator?.intervalMs != null -> q.rateRegulator!!.intervalMs!!.toLong().coerceAtLeast(1)
                else -> 50L
            }
        val timerScope = CoroutineScope(r.job)
        checkTimers[q.name] =
            OtpTimers.sendAfter(
                timerScope,
                delayMs.milliseconds,
                r,
                JobsInfo.CheckQueue(q.name),
            )
    }

    private fun JobQueue.toInfo(): QueueInfo =
        QueueInfo(
            name = name,
            type = type,
            approved = approved,
            queued = storage.size(),
            rateLimit = rateRegulator?.activeLimit,
            counterLimit = counterRegulator?.limit,
            counterValue = counterRegulator?.value,
        )
}

internal object JobsServers {
    fun startLink(
        scope: CoroutineScope,
        config: JobsConfig,
        name: String = "jobs_server",
    ): JobsRef {
        val ref = GenServers.startLink(scope, JobsServer(config), name = name)
        GlobalProcessRegistry.register(name, ref)
        return JobsRef(ref)
    }
}
