package org.otpstudy.core

import java.util.concurrent.ThreadLocalRandom

/**
 * Opt-in stderr traces for hard-to-debug flows (tests, distribution). No file I/O, no HTTP.
 *
 * Enable with JVM property **`org.otpstudy.debug=true`** or environment **`OTPSTUDY_DEBUG=true`** (case-insensitive).
 *
 * This is separate from [OtpLogging], which defaults to a no-op in tests unless a logger is installed.
 */
object OtpStudyDebug {
    private const val PROP = "org.otpstudy.debug"
    private const val ENV = "OTPSTUDY_DEBUG"

    val enabled: Boolean by lazy {
        propTrue(System.getProperty(PROP)) || propTrue(System.getenv(ENV))
    }

    private fun propTrue(s: String?): Boolean =
        s != null && s.equals("true", ignoreCase = true)

    fun trace(message: String) {
        if (enabled) System.err.println("[otpstudy-debug] $message")
    }

    inline fun trace(message: () -> String) {
        if (enabled) System.err.println("[otpstudy-debug] ${message()}")
    }

    /** Short id for correlating log lines within one logical operation (e.g. multiCall). */
    fun newOpId(): String {
        val n = ThreadLocalRandom.current().nextLong()
        return java.lang.Long.toUnsignedString(n, 16).take(12).padStart(12, '0')
    }
}
