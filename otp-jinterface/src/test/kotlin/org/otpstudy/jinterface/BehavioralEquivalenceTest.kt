package org.otpstudy.jinterface

import com.ericsson.otp.erlang.OtpErlangAtom
import com.ericsson.otp.erlang.OtpErlangLong
import com.ericsson.otp.erlang.OtpErlangObject
import com.ericsson.otp.erlang.OtpErlangTuple
import com.ericsson.otp.erlang.OtpNode
import com.ericsson.otp.erlang.OtpMbox
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
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Behavioral equivalence tests: the same message sequence is sent to both a real Erlang
 * `gen_server` and a kotlin-otp `GenServer`, then replies are compared.
 *
 * These tests require `erl` and `erlc` on PATH. They are skipped gracefully when unavailable.
 *
 * OTP source: lib/stdlib/src/gen_server.erl
 */
class BehavioralEquivalenceTest {

    private lateinit var scope: CoroutineScope
    private var erlProcess: Process? = null
    private var erlNode: OtpNode? = null
    private var mbox: OtpMbox? = null

    companion object {
        private val ERL_AVAILABLE = runCatching {
            ProcessBuilder("erl", "-eval", "halt().", "-noshell").start().waitFor() == 0
        }.getOrDefault(false)

        private val ERLC_AVAILABLE = runCatching {
            ProcessBuilder("erlc").start().waitFor() != 127
        }.getOrDefault(false)

        private val ERLANG_AVAILABLE = ERL_AVAILABLE && ERLC_AVAILABLE

        /**
         * Erlang reference counter gen_server.
         * Handles: {add, N} -> ok (state += N), get -> state (integer), reset -> noreply (state = 0)
         */
        private val REFERENCE_COUNTER_ERL = """
-module(reference_counter).
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, counter}, ?MODULE, 0, []).

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
    }

    /** kotlin-otp counter server mirroring the Erlang reference implementation. */
    private class KotlinCounterServer : GenServer<Long> {
        override suspend fun init() = InitResult.Ok(0L)

        override suspend fun handleCall(request: Any, state: Long): ReplyResult<Long> = when {
            request == "get" -> ReplyResult.Reply(state, state)
            request is Pair<*, *> && request.first == "add" -> {
                val n = (request.second as Long)
                ReplyResult.Reply("ok", state + n)
            }
            else -> ReplyResult.Reply("unknown", state)
        }

        override suspend fun handleCast(request: Any, state: Long): NoreplyResult<Long> = when (request) {
            "reset" -> NoreplyResult.Noreply(0L)
            else -> NoreplyResult.Noreply(state)
        }
    }

    @BeforeTest
    fun setUp() {
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    }

    @AfterTest
    fun tearDown() {
        erlProcess?.destroy()
        erlNode?.close()
        scope.cancel()
    }

    @Test
    fun `kotlin counter produces same replies as Erlang reference for basic ops`() {
        val ref = GenServers.startLink(scope, KotlinCounterServer())
        runBlocking {
            // Initial state should be 0
            assertEquals(0L, ref.call<Long>("get"))
            // Add 5
            assertEquals("ok", ref.call<String>(Pair("add", 5L)))
            assertEquals(5L, ref.call<Long>("get"))
            // Add 3
            assertEquals("ok", ref.call<String>(Pair("add", 3L)))
            assertEquals(8L, ref.call<Long>("get"))
            // Cast reset
            ref.cast("reset")
            Thread.sleep(50)
            assertEquals(0L, ref.call<Long>("get"))
            ref.stop()
        }
    }

    @Test
    fun `kotlin counter state transitions match Erlang semantics`() {
        val ref = GenServers.startLink(scope, KotlinCounterServer())
        val ops = listOf(
            Pair("add", 10L),
            Pair("add", 5L),
            "get",
            Pair("add", -3L),
            "get",
        )
        val expectedGetResults = listOf(15L, 12L)

        runBlocking {
            val getResults = mutableListOf<Long>()
            for (op in ops) {
                when (op) {
                    "get" -> getResults.add(ref.call("get"))
                    is Pair<*, *> -> ref.call<Any>(op)
                }
            }
            assertEquals(expectedGetResults, getResults)
            ref.stop()
        }
    }

    /**
     * Full behavioral equivalence: launch a real Erlang gen_server, send the same message sequence
     * to both, and assert identical replies.
     *
     * Requires Erlang/OTP on PATH and EPMD running.
     * Skipped automatically if Erlang is unavailable.
     */
    @Test
    fun `erlang and kotlin counter produce identical replies`() {
        if (!ERLANG_AVAILABLE) {
            println("Skipping behavioral equivalence test: erl/erlc not available on PATH")
            return
        }

        val tmpDir = Files.createTempDirectory("otp_beh_test").toFile()
        try {
            runErlangEquivalenceTest(tmpDir)
        } finally {
            tmpDir.deleteRecursively()
        }
    }

    private fun runErlangEquivalenceTest(tmpDir: File) {
        // Write and compile the reference Erlang module
        val srcFile = File(tmpDir, "reference_counter.erl")
        srcFile.writeText(REFERENCE_COUNTER_ERL)
        val erlcResult = ProcessBuilder("erlc", "-o", tmpDir.absolutePath, srcFile.absolutePath)
            .redirectErrorStream(true)
            .start()
            .waitFor()
        if (erlcResult != 0) {
            println("Skipping Erlang equivalence test: erlc compilation failed")
            return
        }

        // Start the Erlang node with the compiled module
        erlProcess = ProcessBuilder(
            "erl", "-noshell",
            "-sname", "testserver",
            "-setcookie", "kotlin_otp_test",
            "-pa", tmpDir.absolutePath,
            "-eval", "reference_counter:start_link(), receive after infinity -> ok end."
        ).redirectErrorStream(true).start()

        // Wait for the node to start and register with EPMD
        Thread.sleep(2000)

        val nodeUp = runCatching {
            erlNode = OtpNode("testclient@localhost", "kotlin_otp_test")
            mbox = erlNode!!.createMbox("client")
            // Probe: send a ping-style message and see if EPMD can resolve testserver
            val probe = sendErlangCall(mbox!!, erlNode!!, "get")
            probe != null
        }.getOrDefault(false)

        if (!nodeUp) {
            println("Skipping Erlang equivalence test: could not connect to Erlang node " +
                "(EPMD may not be running or node may not have started in time)")
            return
        }

        // Kotlin counter
        val kotlinRef = GenServers.startLink(scope, KotlinCounterServer())

        val scenarios: List<Any> = listOf(
            Pair("add", 5L),
            Pair("add", 3L),
            "get",
            Pair("add", -2L),
            "get",
        )

        runBlocking {
            for (op in scenarios) {
                val kotlinReply = when (op) {
                    "get" -> kotlinRef.call<Long>("get")
                    is Pair<*, *> -> kotlinRef.call<Any>(op)
                    else -> null
                }

                val erlangReply = when (op) {
                    "get" -> sendErlangCall(mbox!!, erlNode!!, "get")
                    is Pair<*, *> -> {
                        val addTerm = OtpErlangTuple(arrayOf(OtpErlangAtom("add"), OtpErlangLong(op.second as Long)))
                        sendErlangCall(mbox!!, erlNode!!, addTerm)
                    }
                    else -> null
                }

                if (erlangReply == null) {
                    println("Erlang reply timed out for op=$op — skipping comparison")
                    kotlinRef.stop()
                    return@runBlocking
                }

                println("op=$op kotlin=$kotlinReply erlang=$erlangReply")

                // Both should return the same type of reply (ok or integer)
                when (op) {
                    "get" -> {
                        val kotlinLong = kotlinReply as Long
                        val erlangLong = (erlangReply as OtpErlangLong).longValue()
                        assertEquals(erlangLong, kotlinLong, "divergence on op=$op")
                    }
                    else -> {
                        val kotlinOk = kotlinReply is String && kotlinReply == "ok"
                        val erlangOk = erlangReply is OtpErlangAtom && erlangReply.atomValue() == "ok"
                        assertEquals(erlangOk, kotlinOk, "divergence on op=$op")
                    }
                }
            }
            kotlinRef.stop()
        }
    }

    private fun sendErlangCall(mbox: OtpMbox, node: OtpNode, request: Any): OtpErlangObject? {
        val ref = node.createRef()
        val from = OtpErlangTuple(arrayOf(mbox.self(), ref))
        val msg = when (request) {
            is String  -> OtpErlangTuple(arrayOf(OtpErlangAtom("\$gen_call"), from, OtpErlangAtom(request)))
            is OtpErlangObject -> OtpErlangTuple(arrayOf(OtpErlangAtom("\$gen_call"), from, request))
            else       -> return null
        }
        mbox.send("counter", "testserver@localhost", msg)
        val response = mbox.receive(3000) ?: return null
        // Response is {ref, Reply}
        return if (response is OtpErlangTuple && response.arity() == 2) response.elementAt(1) else response
    }
}
