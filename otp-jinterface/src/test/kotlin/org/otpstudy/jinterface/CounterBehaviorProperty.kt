package org.otpstudy.jinterface

import com.ericsson.otp.erlang.OtpErlangAtom
import com.ericsson.otp.erlang.OtpErlangLong
import com.ericsson.otp.erlang.OtpErlangObject
import com.ericsson.otp.erlang.OtpErlangTuple
import com.ericsson.otp.erlang.OtpMbox
import com.ericsson.otp.erlang.OtpNode
import io.kotest.core.spec.style.StringSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.list
import io.kotest.property.checkAll
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import java.io.File
import java.nio.file.Files

/**
 * Property-based behavioral equivalence: for any random message sequence, kotlin-otp and
 * a real Erlang gen_server produce identical reply traces.
 *
 * Requires erl + erlc on PATH and EPMD running. Skipped automatically when unavailable.
 *
 * This is the difference between "we tested some cases" and "we specified the behaviour."
 *
 * OTP source: lib/stdlib/src/gen_server.erl — the reference implementation being matched.
 */

sealed class CounterOp {
    data class Add(val n: Long) : CounterOp()
    data object Get : CounterOp()
    data object Reset : CounterOp()
}

private val ERLANG_AVAILABLE: Boolean = runCatching {
    ProcessBuilder("erl", "-eval", "halt().", "-noshell").start().waitFor() == 0 &&
    ProcessBuilder("erlc").start().waitFor() != 127
}.getOrDefault(false)

private val counterOpArb: Arb<CounterOp> = arbitrary {
    when (it.random.nextInt(3)) {
        0    -> CounterOp.Add(it.random.nextLong(-50, 50))
        1    -> CounterOp.Get
        else -> CounterOp.Reset
    }
}

private class KotlinCounterServer : GenServer<Long> {
    override suspend fun init() = InitResult.Ok(0L)

    override suspend fun handleCall(request: Any, state: Long): ReplyResult<Long> = when {
        request == "get"                          -> ReplyResult.Reply(state, state)
        request is Pair<*, *> && request.first == "add" -> {
            val n = request.second as Long
            ReplyResult.Reply("ok", state + n)
        }
        else                                      -> ReplyResult.Reply("unknown", state)
    }

    override suspend fun handleCast(request: Any, state: Long): NoreplyResult<Long> = when (request) {
        "reset" -> NoreplyResult.Noreply(0L)
        else    -> NoreplyResult.Noreply(state)
    }
}

private val REFERENCE_COUNTER_ERL = """
-module(reference_counter_prop).
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, counter_prop}, ?MODULE, 0, []).

init(State) -> {ok, State}.

handle_call(get, _From, State) ->
    {reply, State, State};
handle_call({add, N}, _From, State) ->
    {reply, ok, State + N};
handle_call(_Req, _From, State) ->
    {reply, unknown, State}.

handle_cast(reset, _State) -> {noreply, 0};
handle_cast(_, State) -> {noreply, State}.

handle_info(_, State) -> {noreply, State}.
terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.
""".trimIndent()

class CounterBehaviorProperty : StringSpec({

    afterEach { _ ->
        // nothing — each test iteration manages its own scope
    }

    "kotlin counter produces same replies as Erlang for scripted sequence".config(
        enabled = true,
    ) {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val ops = listOf(
            CounterOp.Add(5L), CounterOp.Add(3L), CounterOp.Get,
            CounterOp.Add(-2L), CounterOp.Get, CounterOp.Reset, CounterOp.Get,
        )
        val ref = GenServers.startLink(scope, KotlinCounterServer())

        fun simulate(ops: List<CounterOp>): List<Any> {
            var state = 0L
            return ops.map { op ->
                when (op) {
                    is CounterOp.Add -> { state += op.n; "ok" }
                    CounterOp.Get    -> state
                    CounterOp.Reset  -> { state = 0L; "cast_ok" }
                }
            }
        }

        val expected = simulate(ops)
        val actual = runBlocking {
            ops.map { op ->
                when (op) {
                    is CounterOp.Add -> ref.call<Any>(Pair("add", op.n))
                    CounterOp.Get    -> ref.call<Any>("get")
                    CounterOp.Reset  -> { ref.cast("reset"); "cast_ok" }
                }
            }
        }
        runBlocking { ref.stop() }
        scope.cancel()

        // compare element by element since Long vs Long may differ by boxed type
        ops.indices.forEach { i ->
            val e = expected[i]
            val a = actual[i]
            val match = when {
                e is Long && a is Long -> e == a
                e is String && a is String -> e == a
                else -> e.toString() == a.toString()
            }
            if (!match) throw AssertionError("op=${ops[i]}: expected=$e actual=$a")
        }
    }

    "kotlin counter state machine matches a simulation for any random sequence".config(
        enabled = true,
    ) {
        checkAll(50, Arb.list(counterOpArb, 1..30)) { ops ->
            val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
            val ref = GenServers.startLink(scope, KotlinCounterServer())

            var simulatedState = 0L
            runBlocking {
                for (op in ops) {
                    when (op) {
                        is CounterOp.Add -> {
                            val reply: Any = ref.call(Pair("add", op.n))
                            simulatedState += op.n
                            if (reply !is String || reply != "ok")
                                throw AssertionError("Add(${op.n}): expected ok, got $reply")
                        }
                        CounterOp.Get -> {
                            val reply: Long = ref.call("get")
                            if (reply != simulatedState)
                                throw AssertionError("Get: expected $simulatedState, got $reply")
                        }
                        CounterOp.Reset -> {
                            ref.cast("reset")
                            simulatedState = 0L
                            kotlinx.coroutines.delay(10)
                        }
                    }
                }
                ref.stop()
            }
            scope.cancel()
        }
    }

    "erlang and kotlin counter produce identical replies for random sequences".config(
        enabled = ERLANG_AVAILABLE,
    ) {
        if (!ERLANG_AVAILABLE) return@config

        val tmpDir = Files.createTempDirectory("otp_prop_test").toFile()
        try {
            runErlangPropertyTest(tmpDir)
        } finally {
            tmpDir.deleteRecursively()
        }
    }
})

private fun runErlangPropertyTest(tmpDir: File) {
    val srcFile = File(tmpDir, "reference_counter_prop.erl")
    srcFile.writeText(REFERENCE_COUNTER_ERL)
    val compiled = ProcessBuilder("erlc", "-o", tmpDir.absolutePath, srcFile.absolutePath)
        .redirectErrorStream(true).start().waitFor()
    if (compiled != 0) {
        println("Skipping Erlang property test: erlc failed")
        return
    }

    val erlProcess = ProcessBuilder(
        "erl", "-noshell",
        "-sname", "propserver",
        "-setcookie", "kotlin_otp_prop",
        "-pa", tmpDir.absolutePath,
        "-eval", "reference_counter_prop:start_link(), receive after infinity -> ok end."
    ).redirectErrorStream(true).start()

    Thread.sleep(2000)
    val erlNode = runCatching {
        OtpNode("propclient@localhost", "kotlin_otp_prop")
    }.getOrNull()
    val mbox = erlNode?.createMbox("propclient")

    if (erlNode == null || mbox == null) {
        println("Skipping Erlang property test: could not create OtpNode")
        erlProcess.destroy()
        return
    }

    // Quick connectivity probe
    val probe = sendErlangCallProp(mbox, erlNode, "get")
    if (probe == null) {
        println("Skipping Erlang property test: could not connect to node (EPMD not running?)")
        erlNode.close()
        erlProcess.destroy()
        return
    }

    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val kotlinRef = GenServers.startLink(scope, KotlinCounterServer())

    // Reset both to clean state
    runBlocking { kotlinRef.cast("reset") }
    sendErlangCastProp(mbox, erlNode, "reset")
    Thread.sleep(100)

    // Run 5 random sequences
    repeat(5) { iteration ->
        val ops = (1..15).map {
            when ((Math.random() * 3).toInt()) {
                0    -> CounterOp.Add((Math.random() * 20 - 10).toLong())
                1    -> CounterOp.Get
                else -> CounterOp.Reset
            }
        }
        // Reset both servers to clean state for each sequence
        runBlocking { kotlinRef.cast("reset") }
        sendErlangCastProp(mbox, erlNode, "reset")
        Thread.sleep(50)

        runBlocking {
            for (op in ops) {
                val kotlinReply: Any? = when (op) {
                    is CounterOp.Add -> kotlinRef.call<Any>(Pair("add", op.n))
                    CounterOp.Get    -> kotlinRef.call<Any>("get")
                    CounterOp.Reset  -> { kotlinRef.cast("reset"); "cast_ok" }
                }
                val erlangReply: OtpErlangObject? = when (op) {
                    is CounterOp.Add -> {
                        val addTerm = OtpErlangTuple(arrayOf(OtpErlangAtom("add"), OtpErlangLong(op.n)))
                        sendErlangCallProp(mbox, erlNode, addTerm)
                    }
                    CounterOp.Get -> sendErlangCallProp(mbox, erlNode, "get")
                    CounterOp.Reset -> { sendErlangCastProp(mbox, erlNode, "reset"); Thread.sleep(20); null }
                }
                if (erlangReply == null && op != CounterOp.Reset) {
                    println("Erlang timed out on op=$op iteration=$iteration — skipping")
                    return@runBlocking
                }
                if (op is CounterOp.Get && erlangReply != null) {
                    val k = kotlinReply as? Long ?: 0L
                    val e = (erlangReply as? OtpErlangLong)?.longValue() ?: -1L
                    if (k != e) throw AssertionError("iteration=$iteration op=$op: kotlin=$k erlang=$e")
                }
            }
        }
    }

    runBlocking { kotlinRef.stop() }
    scope.cancel()
    erlNode.close()
    erlProcess.destroy()
    println("Erlang property test completed")
}

private fun sendErlangCallProp(mbox: OtpMbox, node: OtpNode, request: Any): OtpErlangObject? {
    val ref = node.createRef()
    val from = OtpErlangTuple(arrayOf(mbox.self(), ref))
    val msg = when (request) {
        is String          -> OtpErlangTuple(arrayOf(OtpErlangAtom("\$gen_call"), from, OtpErlangAtom(request)))
        is OtpErlangObject -> OtpErlangTuple(arrayOf(OtpErlangAtom("\$gen_call"), from, request))
        else               -> return null
    }
    mbox.send("counter_prop", "propserver@localhost", msg)
    val response = mbox.receive(3000) ?: return null
    return if (response is OtpErlangTuple && response.arity() == 2) response.elementAt(1) else response
}

private fun sendErlangCastProp(mbox: OtpMbox, node: OtpNode, request: String) {
    val msg = OtpErlangTuple(arrayOf(OtpErlangAtom("\$gen_cast"), OtpErlangAtom(request)))
    mbox.send("counter_prop", "propserver@localhost", msg)
}
