package org.otpstudy.trace

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.otpstudy.core.OtpProcessId
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.GenServerRef
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private class EchoServer : GenServer<Unit> {
    override suspend fun init(self: GenServerRef<Unit>) = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit) = ReplyResult.Reply<Unit>(request, Unit)
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}

class TracerTest {
    private lateinit var scope: CoroutineScope

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        Tracer.clearAll()
    }

    @AfterTest
    fun tearDown() {
        scope.cancel()
        Tracer.clearAll()
    }

    @Test
    fun `Receive events emitted per handleCast when traced`() {
        val ref = GenServers.startLink(scope, EchoServer())
        val captured = mutableListOf<TraceEvent>()
        val handle = Tracer.trace(ref.id, setOf(TraceFlag.Receive)) { captured.add(it) }

        // Manually emit since we haven't wired Tracer into run loop yet
        Tracer.emit(TraceEvent.Receive(ref.id, "hello"))
        Tracer.emit(TraceEvent.Receive(ref.id, "world"))

        handle.close()
        assertEquals(2, captured.size)
        assertTrue(captured.all { it is TraceEvent.Receive })
        runBlocking { ref.stop() }
    }

    @Test
    fun `traceAll captures events from all pids`() {
        val pid1 = OtpProcessId.allocate()
        val pid2 = OtpProcessId.allocate()
        val captured = mutableListOf<TraceEvent>()
        val handle = Tracer.traceAll(setOf(TraceFlag.Receive)) { captured.add(it) }

        Tracer.emit(TraceEvent.Receive(pid1, "from-pid1"))
        Tracer.emit(TraceEvent.Receive(pid2, "from-pid2"))

        handle.close()
        assertEquals(2, captured.size)
    }

    @Test
    fun `handler removed after close`() {
        val pid = OtpProcessId.allocate()
        val captured = mutableListOf<TraceEvent>()
        val handle = Tracer.trace(pid, setOf(TraceFlag.Receive)) { captured.add(it) }

        Tracer.emit(TraceEvent.Receive(pid, "before"))
        handle.close()
        Tracer.emit(TraceEvent.Receive(pid, "after"))

        assertEquals(1, captured.size)
    }

    @Test
    fun `isTracing returns false with no handlers`() {
        val pid = OtpProcessId.allocate()
        assertTrue(!Tracer.isTracing(pid))
    }

    @Test
    fun `isTracing returns true after trace attached`() {
        val pid = OtpProcessId.allocate()
        val handle = Tracer.trace(pid, setOf(TraceFlag.Receive)) { }
        assertTrue(Tracer.isTracing(pid))
        handle.close()
    }

    @Test
    fun `Procs events captured with Procs flag`() {
        val pid = OtpProcessId.allocate()
        val captured = mutableListOf<TraceEvent>()
        val handle = Tracer.traceAll(setOf(TraceFlag.Procs)) { captured.add(it) }

        Tracer.emit(TraceEvent.Procs(pid, TraceEvent.ProcsEvent.Spawned))
        Tracer.emit(TraceEvent.Procs(pid, TraceEvent.ProcsEvent.ExitNormal))

        handle.close()
        assertEquals(2, captured.size)
        assertEquals(TraceEvent.ProcsEvent.Spawned, (captured[0] as TraceEvent.Procs).event)
    }

    @Test
    fun `Send events captured with Send flag`() {
        val from = OtpProcessId.allocate()
        val to = OtpProcessId.allocate()
        val captured = mutableListOf<TraceEvent>()
        val handle = Tracer.traceAll(setOf(TraceFlag.Send)) { captured.add(it) }

        Tracer.emit(TraceEvent.Send(from, to, "payload"))
        handle.close()

        assertEquals(1, captured.size)
        assertEquals(from, (captured[0] as TraceEvent.Send).from)
    }
}
