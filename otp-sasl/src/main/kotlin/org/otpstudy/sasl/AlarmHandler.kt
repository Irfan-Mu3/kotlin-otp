package org.otpstudy.sasl

import kotlinx.coroutines.CoroutineScope
import org.otpstudy.genevent.BackPressurePolicy
import org.otpstudy.genevent.GenEventHandler
import org.otpstudy.genevent.GenEventManagerRef
import org.otpstudy.genevent.GenEventManagers
import java.util.concurrent.ConcurrentHashMap

/**
 * Events emitted by [AlarmHandler] to registered [GenEventHandler]s.
 */
sealed class AlarmEvent {
    /** Fired the first time an alarm is set (idempotent: not fired on subsequent set_alarm calls). */
    data class Set(val alarmId: String, val description: Any?) : AlarmEvent()
    /** Fired when an alarm is cleared. */
    data class Cleared(val alarmId: String) : AlarmEvent()
}

/**
 * Operational alarm registry — OTP SASL's `alarm_handler` in kotlin-otp.
 *
 * An alarm is a **persistent condition** (disk full, CPU hot, queue overflowing), not a
 * transient one-shot event. [setAlarm] is idempotent: setting the same alarm twice fires
 * the handler only once. [clearAlarm] removes it and fires the Cleared event.
 *
 * Subscribers register [GenEventHandler]s via [addHandler]. The underlying event manager
 * is a [GenEventManagerRef] started with [init].
 *
 * OTP source: lib/sasl/src/alarm_handler.erl
 */
object AlarmHandler {
    private val alarms = ConcurrentHashMap<String, Any?>()
    private var manager: GenEventManagerRef<AlarmEvent>? = null

    /**
     * Start the underlying gen_event manager. Must be called before [setAlarm]/[addHandler].
     * Idempotent when called multiple times with the same scope.
     */
    fun init(scope: CoroutineScope) {
        if (manager == null) {
            manager = GenEventManagers.startLink(scope, BackPressurePolicy.Block, name = "alarm_handler")
        }
    }

    /** Register an alarm. Idempotent: the handler is notified only the first time per alarm ID. */
    suspend fun setAlarm(alarmId: String, description: Any? = null) {
        val isNew = alarms.putIfAbsent(alarmId, description ?: "") == null
        if (isNew) manager?.notify(AlarmEvent.Set(alarmId, description))
    }

    /** Clear an alarm. If it was set, the handler is notified exactly once. */
    suspend fun clearAlarm(alarmId: String) {
        if (alarms.remove(alarmId) != null) {
            manager?.notify(AlarmEvent.Cleared(alarmId))
        }
    }

    fun getAlarms(): Map<String, Any?> = alarms.toMap()

    fun isAlarmSet(alarmId: String): Boolean = alarms.containsKey(alarmId)

    suspend fun addHandler(handler: GenEventHandler<AlarmEvent>) {
        manager?.addHandler(handler) ?: error("AlarmHandler not initialised — call init(scope) first")
    }

    suspend fun removeHandler(handler: GenEventHandler<AlarmEvent>) {
        manager?.deleteHandler(handler)
    }

    suspend fun reset() {
        alarms.clear()
        manager?.shutdown()
        manager = null
    }
}
