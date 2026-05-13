package org.otpstudy.demo

import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.otpstudy.application.ApplicationStartResult
import org.otpstudy.application.SupervisorApplication
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.supervisor.ChildSpec
import org.otpstudy.supervisor.SupervisorFlags
import kotlin.time.Duration.Companion.seconds

private data class EchoState(val label: String)

private class EchoServer : GenServer<EchoState> {
    override suspend fun init(): InitResult<EchoState> = InitResult.Ok(EchoState("echo"))

    override suspend fun handleCall(
        request: Any,
        state: EchoState,
    ): ReplyResult<EchoState> = ReplyResult.Reply("${state.label}:$request", state)

    override suspend fun handleCast(
        request: Any,
        state: EchoState,
    ): NoreplyResult<EchoState> = NoreplyResult.Noreply(state)
}

private data class FlakyState(
    val boomAfterCalls: Int,
    var calls: Int = 0,
)

private class FlakyServer(
    private val boomAfterCalls: Int,
) : GenServer<FlakyState> {
    override suspend fun init(): InitResult<FlakyState> {
        println("FlakyServer init (supervisor (re)start)")
        return InitResult.Ok(FlakyState(boomAfterCalls))
    }

    override suspend fun handleCall(
        request: Any,
        state: FlakyState,
    ): ReplyResult<FlakyState> {
        state.calls++
        if (state.calls >= state.boomAfterCalls) {
            error("simulated crash after ${state.calls} call(s)")
        }
        return ReplyResult.Reply("flaky-ok:$request", state)
    }

    override suspend fun handleCast(
        request: Any,
        state: FlakyState,
    ): NoreplyResult<FlakyState> = NoreplyResult.Noreply(state)
}

private fun echoChildStart(echoReady: CompletableDeferred<GenServerRef<EchoState>>): suspend CoroutineScope.() -> Unit =
    {
        val ref = GenServers.startLink(this, EchoServer(), name = "echo")
        echoReady.complete(ref)
        ref.job.join()
    }

private fun flakyChildStart(
    currentRef: AtomicReference<GenServerRef<FlakyState>?>,
    firstInstanceReady: CompletableDeferred<Unit>,
): suspend CoroutineScope.() -> Unit =
    {
        val ref = GenServers.startLink(this, FlakyServer(boomAfterCalls = 2), name = "flaky")
        currentRef.set(ref)
        if (!firstInstanceReady.isCompleted) {
            firstInstanceReady.complete(Unit)
        }
        ref.job.join()
    }

fun main(): Unit =
    runBlocking {
        val echoReady = CompletableDeferred<GenServerRef<EchoState>>()
        val flakyCurrent = AtomicReference<GenServerRef<FlakyState>?>()
        val flakyFirstReady = CompletableDeferred<Unit>()

        val children =
            listOf(
                ChildSpec(
                    id = "echo",
                    restart = Restart.Permanent,
                    shutdown = Shutdown.Timeout(5.seconds),
                    start = echoChildStart(echoReady),
                ),
                ChildSpec(
                    id = "flaky",
                    restart = Restart.Permanent,
                    shutdown = Shutdown.Timeout(5.seconds),
                    start = flakyChildStart(flakyCurrent, flakyFirstReady),
                ),
            )

        val app =
            SupervisorApplication(
                name = "demo",
                flags =
                    SupervisorFlags(
                        intensity = 5,
                        period = 60.seconds,
                    ),
                children = children,
            )

        when (val started = app.start(this)) {
            is ApplicationStartResult.Error -> {
                println("start failed: ${started.cause.message}")
                return@runBlocking
            }
            is ApplicationStartResult.PhaseError -> {
                println("phase '${started.phase}' failed: ${started.cause.message}")
                return@runBlocking
            }
            is ApplicationStartResult.Ok -> {
                val echo = echoReady.await()
                flakyFirstReady.await()
                fun flaky() = checkNotNull(flakyCurrent.get()) { "flaky not started" }
                println(echo.call<String>("hello"))
                println(flaky().call<String>("ping"))
                try {
                    println(flaky().call<String>("ping"))
                } catch (e: Exception) {
                    println("client saw flaky failure: ${e.message}")
                }
                println("(supervisor should restart flaky — watch for a second init log)")
                delay(500)
                println(flaky().call<String>("after-restart"))
                println(echo.call<String>("still-here"))
                app.stop()
                println("stopped")
            }
        }
    }
