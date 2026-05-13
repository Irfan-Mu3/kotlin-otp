package org.otpstudy.genserver

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.io.InputStream
import java.io.OutputStream

/**
 * OTP-style port: OS subprocess bridged into the actor system via [ProcessBuilder].
 */
class OtpPort(
    val command: List<String>,
    val owner: GenServerRef<*>,
    scope: CoroutineScope,
) : AutoCloseable {

    private val process: Process = ProcessBuilder(command)
        .redirectErrorStream(true)
        .start()

    val isAlive: Boolean get() = process.isAlive
    val exitCode: Int? get() = if (!process.isAlive) process.exitValue() else null

    init {
        scope.launch(Dispatchers.IO) {
            val buf = ByteArray(4096)
            val stream: InputStream = process.inputStream
            try {
                while (isAlive) {
                    val n = stream.read(buf)
                    if (n < 0) break
                    if (n > 0 && owner.job.isActive) {
                        owner.sendInfo(PortData(this@OtpPort, buf.copyOf(n)))
                    }
                }
            } finally {
                val code = runCatching { process.waitFor() }.getOrDefault(-1)
                if (owner.job.isActive) owner.sendInfo(PortExit(this@OtpPort, code))
            }
        }
    }

    fun command(bytes: ByteArray) {
        val out: OutputStream = process.outputStream
        out.write(bytes)
        out.flush()
    }

    fun commandLine(line: String) = command((line + "\n").toByteArray(Charsets.UTF_8))

    override fun close() {
        runCatching { process.destroyForcibly() }
    }
}

data class PortData(val port: OtpPort, val bytes: ByteArray) : InfoMsg {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PortData) return false
        if (port != other.port) return false
        if (!bytes.contentEquals(other.bytes)) return false
        return true
    }

    override fun hashCode(): Int {
        var result = port.hashCode()
        result = 31 * result + bytes.contentHashCode()
        return result
    }
}

data class PortExit(val port: OtpPort, val exitCode: Int) : InfoMsg
