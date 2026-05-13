package org.otpstudy.application

/**
 * Typed facade over an immutable `Map<String, Any?>` for application configuration.
 *
 * Analogous to OTP application environment (`application:get_env/2`) but without the
 * global process dictionary — env is passed explicitly to start phases and callbacks.
 *
 * Usage:
 * ```kotlin
 * val env = ApplicationEnv(mapOf("db.url" to "jdbc:...", "workers" to 4))
 * val url: String = env.require("db.url")
 * val workers: Int = env.get("workers") ?: 1
 * ```
 */
class ApplicationEnv(private val map: Map<String, Any?> = emptyMap()) {

    /** Returns the value for [key] cast to [T], or null if absent or null. */
    @Suppress("UNCHECKED_CAST")
    fun <T> get(key: String): T? = map[key] as T?

    /** Returns the value for [key] cast to [T], throwing if absent or null. */
    fun <T> require(key: String): T =
        get(key) ?: error("ApplicationEnv: required key '$key' is missing")

    /** Returns a new [ApplicationEnv] with [extra] entries merged in. */
    operator fun plus(extra: Map<String, Any?>): ApplicationEnv = ApplicationEnv(map + extra)

    /** Returns a new [ApplicationEnv] with the single [key]/[value] pair added. */
    fun with(key: String, value: Any?): ApplicationEnv = ApplicationEnv(map + (key to value))

    override fun toString(): String = "ApplicationEnv($map)"

    companion object {
        val EMPTY = ApplicationEnv()
    }
}
