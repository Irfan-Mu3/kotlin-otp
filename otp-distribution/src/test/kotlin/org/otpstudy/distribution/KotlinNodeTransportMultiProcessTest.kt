package org.otpstudy.distribution

import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import org.otpstudy.registry.ProcessRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@ExtendWith(OtpStudyDebugTestExtension::class)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class KotlinNodeTransportMultiProcessTest {

    @Test
    fun `subprocess JVM server soak from test JVM client`() {
        val java = Paths.get(System.getProperty("java.home") ?: error("java.home"), "bin", "java").toString()
        val cp = System.getProperty("java.class.path") ?: error("java.class.path")
        val proc =
            ProcessBuilder(java, "-cp", cp, "org.otpstudy.distribution.DistEchoServerMainKt")
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start()
        val port: Int =
            BufferedReader(InputStreamReader(proc.inputStream)).use { reader ->
                val line = reader.readLine() ?: error("no PORT line from subprocess")
                Regex("PORT (\\d+)").find(line)?.groupValues?.get(1)?.toInt()
                    ?: error("bad PORT line: $line")
            }
        try {
            runBlocking {
                val nodeLocal = NodeId("test", "proc")
                val nodeRemote = NodeId("remote", "proc")
                val reg = ProcessRegistry()
                val ta = KotlinNodeTransport(nodeLocal, "", 0)
                try {
                    ta.startAccepting(this, reg)
                    ta.connectOut(this, reg, nodeRemote, "127.0.0.1", port)
                    repeat(25) { i ->
                        @Suppress("UNCHECKED_CAST")
                        val r = ta.call(nodeRemote, "svc", "m$i", 10.seconds) as String
                        assertEquals("echo:m$i", r)
                    }
                } finally {
                    ta.close()
                    coroutineContext.job.cancelChildren()
                }
            }
        } finally {
            proc.destroy()
            assertTrue(proc.waitFor(30, TimeUnit.SECONDS), "subprocess did not exit")
        }
    }
}
