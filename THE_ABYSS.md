# The Abyss: compiler tooling, runtime introspection, and true distribution

> **Status: NOT STARTED (2026-05-04).** [BEYOND_THE_DEEP_END.md](docs/archive/roadmaps/BEYOND_THE_DEEP_END.md) is complete (all 9 sections, 186 tests passing).

This is the seventh roadmap. Three things distinguish it from what came before.

**First:** the one section explicitly deferred from BEYOND_THE_DEEP_END is now unblocked. The Kotlin IR compiler plugin for automatic reduction injection (§1 below) has everything it needs: `ReductionBudget` is wired, `@Preemptible` is designed, and the build infrastructure pattern exists. It belongs here.

**Second:** we have been building the *application layer* that sits on top of the BEAM. The items in this document reach back *down* — into the runtime, into the VM scheduler contract, into the distribution wire protocol. `erlang:hibernate/3`, `erlang:trace/3`, `dets`, and `gen_server:call` flush semantics are not library conveniences; they are runtime contracts that every serious Erlang system depends on.

**Third:** the last gap between behavioral equivalence as a claim and behavioral equivalence as a proof is property-based testing. §9 below uses `kotest-property` to generate thousands of random message sequences and verify that kotlin-otp and real Erlang produce identical reply traces. This is what transforms a study library into a specification.

---

## 1. Kotlin IR compiler plugin — automatic reduction injection (Level 2)

### The real gap

BEYOND_THE_DEEP_END §2 was deferred because it required plugin build infrastructure that felt like scope creep at the time. The plugin is now the only item in the table that cannot be done in a single library source file. Every other section below is library work.

### Module structure

```
buildSrc/
  src/main/kotlin/
    ReductionPlugin.kt          ← CompilerPluginRegistrar
    ReductionInjectionExtension.kt
    ReductionInjectionTransformer.kt
  build.gradle.kts              ← kotlin("jvm"), compilerPlugins dependency
```

### `buildSrc/build.gradle.kts`

```kotlin
plugins {
    kotlin("jvm") version "2.2.10"
}

dependencies {
    compileOnly("org.jetbrains.kotlin:kotlin-compiler-embeddable:2.2.10")
}
```

### Transformer: inject `reduce(1)` at every loop back-edge

```kotlin
class ReductionInjectionTransformer(
    private val ctx: IrPluginContext,
) : IrElementTransformerVoid() {

    private val reduceSymbol: IrSimpleFunctionSymbol by lazy {
        ctx.referenceFunctions(
            CallableId(FqName("org.otpstudy.genserver"), Name.identifier("reduce"))
        ).firstOrNull()
            ?: error("org.otpstudy.genserver.reduce not found on classpath — add otp-gen-server dependency")
    }

    override fun visitLoop(loop: IrLoop): IrExpression {
        val transformed = super.visitLoop(loop) as IrLoop
        val reduceCall = IrCallImpl(
            UNDEFINED_OFFSET, UNDEFINED_OFFSET,
            ctx.irBuiltIns.unitType,
            reduceSymbol,
            typeArgumentsCount = 0,
            valueArgumentsCount = 1,
        ).apply {
            putValueArgument(0, IrConstImpl.int(UNDEFINED_OFFSET, UNDEFINED_OFFSET,
                ctx.irBuiltIns.intType, 1))
        }
        // Wrap the existing loop body in a block that appends the reduce call.
        return IrBlockImpl(loop.startOffset, loop.endOffset, loop.type).apply {
            statements += transformed
        }.also {
            transformed.body = IrBlockImpl(UNDEFINED_OFFSET, UNDEFINED_OFFSET,
                ctx.irBuiltIns.unitType).apply {
                statements += transformed.body!!
                statements += reduceCall
            }
        }
    }
}
```

### Annotation and wiring

```kotlin
// In otp-gen-server:
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.SOURCE)
annotation class Preemptible(val reductionsPerCall: Int = 1)

// In any actor module's build.gradle.kts:
plugins { id("org.otpstudy.reduction-plugin") }
otpReductions {
    autoInjectPackages = listOf("org.myapp.actors")
}
```

### What before/after looks like

```kotlin
// Source — annotated:
@Preemptible
suspend fun processAll(items: List<Work>): Int {
    var count = 0
    for (item in items) {   // ← plugin injects reduce(1) here
        doWork(item)
        count++
    }
    return count
}

// Bytecode-equivalent after transformation:
suspend fun processAll(items: List<Work>): Int {
    var count = 0
    for (item in items) {
        doWork(item)
        count++
        coroutineContext[ReductionBudget]?.reduce(1)  // injected
    }
    return count
}
```

**OTP source:** `erts/emulator/beam/erl_process.c` — `process_main`, the BEAM's scheduler deduction loop. The plugin is the Kotlin-layer analogue.

---

## 2. `erlang:hibernate/3` — actor hibernation (`otp-gen-server` extension)

### The real gap

`erlang:hibernate(Module, Function, Args)` suspends a process with an empty stack and a GC-friendlier heap. When the next message arrives, the process is resumed by calling `{Module, Function, Args}`. The key invariant: no stack frames survive hibernation; this is how OTP applications keep thousands of idle connections at near-zero memory cost.

The BEAM's per-process heap makes this cheap. On the JVM, we cannot replicate the heap compaction, but we can release the actor's mailbox backlog and yield in a way that discards the call stack.

### `HibernateResult` + `hibernate` callback

```kotlin
/**
 * Signal the run loop to enter hibernation: drain the mailbox, yield until the next
 * message arrives, then resume by calling [onWake].
 *
 * While hibernated the actor consumes no CPU. Its memory footprint is bounded by whatever
 * [state] holds — there are no pending coroutine frames.
 *
 * OTP source: erts/emulator/beam/erl_process.c — hibernate/3, erts_hibernate
 */
sealed class NoreplyResult<out S> {
    data class Noreply<S>(val newState: S) : NoreplyResult<S>()
    data class Stop<S>(val reason: TerminateReason, val newState: S) : NoreplyResult<S>()
    /** Enter hibernation after this message; wake by calling [onWake] with the first message. */
    data class Hibernate<S>(
        val newState: S,
        val onWake: suspend (Any, S) -> NoreplyResult<S> = { _, s -> Noreply(s) },
    ) : NoreplyResult<S>()
}
```

Run-loop handling:

```kotlin
is NoreplyResult.Hibernate -> {
    state = r.newState
    // Block until one message arrives; no timeout, no sys draining.
    val wakeMsg = mailbox.receive()
    queueLen.decrementAndGet()
    val wakeResult = when (wakeMsg) {
        is GenServerMsg.Cast -> r.onWake(wakeMsg.request, state)
        is GenServerMsg.Info -> currentServer.handleInfo(wakeMsg.msg, state)
        else -> NoreplyResult.Noreply(state)
    }
    if (applyNoreply(wakeResult)) break@outer
}
```

### Pattern: idle connection actor

```kotlin
class ConnectionActor : GenServer<ConnectionState> {
    override suspend fun handleCast(request: Any, state: ConnectionState) =
        when (request) {
            is SendData   -> { send(state, request.bytes); NoreplyResult.Noreply(state) }
            is GoIdle     -> NoreplyResult.Hibernate(state) { msg, s ->
                // First message after hibernation — resume normally
                handleCast(msg, s)
            }
            else -> NoreplyResult.Noreply(state)
        }
}
```

**OTP source:** `lib/stdlib/src/gen_server.erl` — `hibernate` is checked after each `handle_cast`/`handle_call` return.

---

## 3. `gen_server:call` flush semantics — the late-reply safety contract

### The real gap

OTP's `gen_server:call/3` has a subtle but critical safety property: if the call times out, the reply that arrives *after* the timeout is **flushed** from the caller's mailbox before the next user code runs. Without flushing, a late reply contaminates the caller's message stream.

Our current `GenServerRef.call` uses `withTimeout` + `CompletableDeferred`. If the server replies after the timeout window, the `CompletableDeferred` is already awaited-past and the completion is silently dropped. That's safe because `CompletableDeferred` is not a mailbox. We don't have the contamination problem.

However, there is a related gap: `call` does not handle the case where the server process **exits** during a call. OTP detects this via a monitor on the server's pid; the caller receives `{'DOWN', Ref, process, Pid, Reason}` and raises `{noproc, ...}` or `{nodedown, Node}`.

### Server-exit detection during call

```kotlin
suspend fun <R> call(request: Any, timeout: Duration = 5.seconds): R =
    withTimeout(timeout) {
        val reply = CompletableDeferred<Any?>()
        val msg = GenServerMsg.Call(request, reply)
        when (mailboxBound?.policy) {
            OverflowPolicy.CrashSender -> {
                if (!mailbox.trySend(msg).isSuccess)
                    throw MailboxFullException("mailbox full for $id")
                queueLen.incrementAndGet()
            }
            else -> { mailbox.send(msg); queueLen.incrementAndGet() }
        }
        // If the server job dies while we are waiting, complete the reply with
        // a ServerDownException rather than hanging until timeout.
        val deathWatcher = job.invokeOnCompletion { cause ->
            reply.completeExceptionally(
                ServerDownException(id, cause ?: CancellationException("server stopped"))
            )
        }
        try {
            @Suppress("UNCHECKED_CAST")
            reply.await() as R
        } finally {
            deathWatcher.dispose()
        }
    }

class ServerDownException(val pid: OtpProcessId, cause: Throwable) :
    Exception("server $pid exited during call", cause)
```

**OTP source:** `lib/stdlib/src/gen_server.erl` — `do_call/4`, the `{'DOWN', ...}` receive clause.

---

## 4. Trace framework — `erlang:trace/3` equivalent (`otp-trace` module)

### The real gap

`erlang:trace(Pid, true, [call, send, 'receive', procs])` attaches a tracer to any process. Trace events are sent as messages to the tracer process. `dbg:tracer/0` is the standard dev-tool interface; `recon_trace` wraps it for production use.

We have `OtpObserver` and `Recon`, which query *snapshots*. A trace framework emits *events in real time*, which is a fundamentally different (and more powerful) observability model.

### `otp-trace` module

```kotlin
/**
 * Trace flag set — mirrors erlang:trace/3 flag atoms.
 *
 * OTP source: erts/emulator/beam/erl_bif_trace.c — trace_process/4
 */
enum class TraceFlag {
    /** Emits [TraceEvent.Send] whenever the actor sends a message. */
    Send,
    /** Emits [TraceEvent.Receive] whenever the actor's handleCast/handleCall is called. */
    Receive,
    /** Emits [TraceEvent.Call] when a user-tagged function is entered. */
    Call,
    /** Emits [TraceEvent.Procs] on actor start, stop, and name registration. */
    Procs,
}

sealed class TraceEvent {
    val timestamp: Instant get() = Instant.now()

    data class Send(val from: OtpProcessId, val to: OtpProcessId?, val message: Any) : TraceEvent()
    data class Receive(val pid: OtpProcessId, val message: Any) : TraceEvent()
    data class Call(val pid: OtpProcessId, val function: String, val args: List<Any?>) : TraceEvent()
    data class Procs(val pid: OtpProcessId, val event: ProcsEvent) : TraceEvent()
    data class Return(val pid: OtpProcessId, val function: String, val result: Any?) : TraceEvent()

    enum class ProcsEvent { Spawned, ExitNormal, ExitShutdown, ExitCrash, NameRegistered, NameUnregistered }
}

fun interface TraceHandler {
    fun onEvent(event: TraceEvent)
}

object Tracer {
    private val handlers = ConcurrentHashMap<OtpProcessId, MutableList<TraceHandler>>()
    private val globalHandlers = CopyOnWriteArrayList<Pair<Set<TraceFlag>, TraceHandler>>()

    /**
     * Attach [handler] to [pid] for the given [flags].
     * Analogous to erlang:trace(Pid, true, Flags).
     */
    fun trace(pid: OtpProcessId, flags: Set<TraceFlag>, handler: TraceHandler): AutoCloseable {
        handlers.getOrPut(pid) { CopyOnWriteArrayList() }.add(handler)
        return AutoCloseable { handlers[pid]?.remove(handler) }
    }

    /**
     * Attach [handler] globally — receives events from all traced actors.
     * Analogous to dbg:tracer/0 with a global match spec.
     */
    fun traceAll(flags: Set<TraceFlag>, handler: TraceHandler): AutoCloseable {
        val entry = flags to handler
        globalHandlers.add(entry)
        return AutoCloseable { globalHandlers.remove(entry) }
    }

    internal fun emit(event: TraceEvent) {
        val pid = when (event) {
            is TraceEvent.Send    -> event.from
            is TraceEvent.Receive -> event.pid
            is TraceEvent.Call    -> event.pid
            is TraceEvent.Procs   -> event.pid
            is TraceEvent.Return  -> event.pid
        }
        handlers[pid]?.forEach { runCatching { it.onEvent(event) } }
        globalHandlers.forEach { (_, h) -> runCatching { h.onEvent(event) } }
    }

    fun clearAll() { handlers.clear(); globalHandlers.clear() }
}
```

### Wiring into the run loop

Each `handleCast` / `handleCall` invocation emits `TraceEvent.Receive` and `TraceEvent.Return` when a tracer is attached to the actor. Wired as an opt-in via `GenServerHooks`:

```kotlin
// In GenServers.runLoop, wrapping handleCast:
if (Tracer.isTracing(id)) Tracer.emit(TraceEvent.Receive(id, msg.request))
val result = currentServer.handleCast(msg.request, state)
if (Tracer.isTracing(id)) Tracer.emit(TraceEvent.Return(id, "handleCast", null))
```

### Usage: capture all messages to a slow actor

```kotlin
// In production: capture 100 messages from the slow actor, then stop
val captured = mutableListOf<TraceEvent>()
val handle = Tracer.trace(slowActorRef.id, setOf(TraceFlag.Receive)) { event ->
    captured.add(event)
    if (captured.size >= 100) handle.close()   // self-removing after 100 events
}
```

**OTP source:** `erts/emulator/beam/erl_bif_trace.c`, `lib/runtime_tools/src/dbg.erl`

---

## 5. `dets` — disk-backed tables (`otp-dets` module)

### The real gap

`dets:open_file(Tab, [{file, "path.dets"}, {type, set}])` opens a table whose contents survive process restarts. `dets:insert/2`, `dets:lookup/2`, `dets:sync/1`. Every non-trivial Erlang system uses `dets` or Mnesia (which uses `dets` under the hood for disk copies).

Our `OtpTable` is in-memory. The gap is persistence across actor restarts.

### Design: `DetsTable` backed by a memory-mapped file

```kotlin
/**
 * Disk-backed ETS-style table.
 *
 * Data is stored in a memory-mapped file. Writes are buffered in a dirty set and
 * flushed to disk on [sync] or when the table is closed. The file format is a simple
 * append log; on open, the log is replayed to rebuild the in-memory index.
 *
 * This teaches dets semantics: durability, crash recovery, and sync points.
 * It deliberately avoids full dets wire compatibility (which uses a custom binary term format).
 *
 * OTP source: lib/stdlib/src/dets.erl, lib/stdlib/src/dets_v9.erl
 */
class DetsTable<K : Any, V : Any>(
    val name: String,
    val file: Path,
    val type: TableType = TableType.Set,
    val keySerializer: (K) -> ByteArray,
    val valueSerializer: (V) -> ByteArray,
    val keyDeserializer: (ByteArray) -> K,
    val valueDeserializer: (ByteArray) -> V,
) : AutoCloseable {
    private val memTable = OtpTable<K, V>(name, type)
    private val dirty = ConcurrentHashMap.newKeySet<K>()   // keys with unsync'd writes
    private val logChannel: FileChannel = FileChannel.open(file,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)

    init { replay() }

    fun insert(key: K, value: V) {
        memTable.insert(key, value)
        dirty.add(key)
    }

    fun lookup(key: K): List<V> = memTable.lookup(key)

    fun delete(key: K) {
        memTable.delete(key)
        dirty.add(key)
    }

    /**
     * Flush dirty entries to disk.
     * Analogous to dets:sync/1.
     */
    fun sync() {
        for (key in dirty) {
            val values = memTable.lookup(key)
            appendLogEntry(key, values)
        }
        logChannel.force(true)
        dirty.clear()
    }

    override fun close() {
        sync()
        logChannel.close()
    }

    private fun replay() { /* read log file, rebuild memTable */ }
    private fun appendLogEntry(key: K, values: List<V>) { /* write WAL entry */ }
}
```

### JSON-serialized variant for quick use

```kotlin
// Helper for tables whose keys and values are kotlin.String:
fun openStringDets(name: String, file: Path): DetsTable<String, String> =
    DetsTable(
        name = name, file = file,
        keySerializer = String::toByteArray, valueSerializer = String::toByteArray,
        keyDeserializer = ::String, valueDeserializer = ::String,
    )
```

### Mnesia-`dets` integration

`MnesiaTransaction` in `otp-mnesia` can be extended to include `DetsTable` keys in the read/write sets, giving you atomic cross-table commits that span in-memory and on-disk tables:

```kotlin
// In MnesiaTransaction — dets tables participate in the same commit protocol:
fun <K : Any, V : Any> write(table: DetsTable<K, V>, key: K, value: V) {
    writes[table to key] = WriteRecord(value, isTombstone = false)
}
```

**OTP source:** `lib/stdlib/src/dets.erl` — `open_file/2`, `sync/1`, the WAL append protocol.

---

## 6. `application_controller` — central application registry (`otp-application` extension)

### The real gap

OTP's `application_controller` is a single gen_server that owns the global application state:
- **loaded** — `application:load/1` parsed the `.app` spec, dependencies are known
- **started** — the supervisor tree is running
- **which_applications/0** — currently running apps
- **loaded_applications/0** — loaded but possibly not started

Our `OtpApplication` and `SupervisorApplication` model individual apps. There is no central controller that tracks them, enforces start ordering, or propagates stops.

### `ApplicationController`

```kotlin
/**
 * Central application registry — OTP's application_controller in kotlin-otp.
 *
 * Tracks the loaded/started distinction, enforces dependency ordering,
 * and propagates application stops.
 *
 * OTP source: lib/kernel/src/application_controller.erl
 */
object ApplicationController {
    private val loaded  = ConcurrentHashMap<String, ApplicationSpec>()
    private val running = ConcurrentHashMap<String, RunningApp>()

    data class ApplicationSpec(
        val name: String,
        val vsn: String,
        val description: String = "",
        val applications: List<String> = emptyList(),  // dependencies
        val startModule: String? = null,
    )

    data class RunningApp(
        val spec: ApplicationSpec,
        val ref: Any,   // the root supervisor ref or OtpApplication instance
        val startedAt: Instant = Instant.now(),
    )

    fun load(spec: ApplicationSpec) { loaded[spec.name] = spec }

    fun unload(name: String) {
        check(name !in running) { "cannot unload running application '$name'" }
        loaded.remove(name)
    }

    suspend fun start(name: String, scope: CoroutineScope): RunningApp {
        val spec = loaded[name] ?: error("application '$name' not loaded")
        // Start dependencies first
        for (dep in spec.applications) {
            if (dep !in running) start(dep, scope)
        }
        val app = RunningApp(spec, Unit)  // actual ref injected by OtpApplication.start
        running[name] = app
        return app
    }

    fun stop(name: String) {
        running.remove(name)
        // Dependents that listed this app must also be stopped
        val dependents = running.values.filter { name in it.spec.applications }
        for (dep in dependents) stop(dep.spec.name)
    }

    fun whichApplications(): List<RunningApp> = running.values.toList()
    fun loadedApplications(): List<ApplicationSpec> = loaded.values.toList()

    fun reset() { loaded.clear(); running.clear() }
}
```

### Dependency ordering example

```kotlin
ApplicationController.load(ApplicationSpec("db",  "1.0", applications = emptyList()))
ApplicationController.load(ApplicationSpec("web", "1.0", applications = listOf("db")))
ApplicationController.load(ApplicationSpec("api", "1.0", applications = listOf("web", "db")))

// Starting "api" automatically starts "db" then "web" first:
scope.launch { ApplicationController.start("api", scope) }
```

**OTP source:** `lib/kernel/src/application_controller.erl` — `handle_call({load_application,...})`, `start/2`, the dependency walk.

---

## 7. `release_handler` appup — formal upgrade scripts (`otp-hotcode` extension)

### The real gap

Hot code upgrades in BEYOND_THE_DEEP_END are triggered by name (string). OTP's `release_handler` uses structured **appup files** to script the upgrade sequence across an application:

```erlang
%% myapp-2.0.appup
{"2.0",
 [{"1.0", [
     {load_module, counter_utils},
     {update, counter_server, {advanced, []}},
     {restart_application, myapp}
 ]}],
 [{"1.0", [
     {load_module, counter_utils}
 ]}]
}.
```

Each instruction maps to a different upgrade strategy. Our `ReleaseHandler` only supports the `update` equivalent (`sysCodeChange`). The others are missing.

### Formal upgrade instructions

```kotlin
sealed class AppupInstruction {
    /** Reload the named module in-place (stateless, like a utility module). */
    data class LoadModule(val name: String) : AppupInstruction()

    /** Call codeChange on the named actor's GenServer; state migrates. */
    data class UpdateActor(
        val name: String,
        val oldVersion: String,
        val newVersion: String,
        val newClass: Class<out GenServer<*>>,
    ) : AppupInstruction()

    /** Restart the named actor entirely (lose state, re-init with new class). */
    data class RestartActor(val name: String) : AppupInstruction()

    /** Add a new child to a supervisor at runtime (DynamicSupervisor.startChild). */
    data class AddActor(val supervisorName: String, val spec: ChildSpec) : AppupInstruction()

    /** Remove a child from a supervisor (DynamicSupervisor.terminateChild). */
    data class RemoveActor(val supervisorName: String, val childId: String) : AppupInstruction()

    /** Restart the whole application cleanly. */
    data object RestartApplication : AppupInstruction()
}

data class AppupScript(
    val fromVersion: String,
    val toVersion: String,
    val upgrade: List<AppupInstruction>,
    val downgrade: List<AppupInstruction>,
)

object AppupRunner {
    suspend fun runUpgrade(script: AppupScript, registry: ProcessRegistry) {
        for (instruction in script.upgrade) {
            when (instruction) {
                is AppupInstruction.UpdateActor -> {
                    val ref = registry.lookup<Any>(instruction.name)
                        ?: error("actor '${instruction.name}' not found for upgrade")
                    ref.sysCodeChange(instruction.newClass, instruction.oldVersion, instruction.newVersion)
                }
                is AppupInstruction.RestartActor -> {
                    val ref = registry.lookup<Any>(instruction.name)
                    ref?.stop()
                    // Re-start is the supervisor's responsibility; trigger via supervisor restart
                }
                is AppupInstruction.LoadModule -> {
                    // No-op on JVM: classes are already loaded by the classloader
                    OtpLogging.log(OtpLogLevel.Info, OtpLogContext("appup"),
                        "LoadModule '${instruction.name}' — no-op on JVM (class already loaded)")
                }
                is AppupInstruction.AddActor -> { /* DynamicSupervisor.startChild */ }
                is AppupInstruction.RemoveActor -> { /* DynamicSupervisor.terminateChild */ }
                AppupInstruction.RestartApplication -> { /* signal ApplicationController */ }
            }
        }
    }
}
```

**OTP source:** `lib/sasl/src/release_handler.erl` — `do_upgrade/2`, `do_downgrade/2`, the instruction interpreter.

---

## 8. Property-based behavioral equivalence — `kotest-property` + real Erlang

### The real gap

§9 of BEYOND_THE_DEEP_END established the *architecture* for behavioral equivalence tests: launch a real Erlang node, send messages, compare. The tests there are *scripted* (a fixed list of ops). The real proof is *property-based*: given any random message sequence, kotlin-otp and Erlang produce identical replies.

This is the difference between "we tested some cases" and "we specified the behaviour."

### `kotest-property` dependency

```kotlin
// otp-jinterface/build.gradle.kts:
testImplementation("io.kotest:kotest-runner-junit5:5.9.1")
testImplementation("io.kotest:kotest-property:5.9.1")
```

### Counter server property

```kotlin
sealed class CounterOp {
    data class Add(val n: Long) : CounterOp()
    data object Get : CounterOp()
    data object Reset : CounterOp()
}

private val Arb.Companion.counterOp: Arb<CounterOp> get() = arbitrary {
    when (it.random.nextInt(3)) {
        0    -> CounterOp.Add(it.random.nextLong(-100, 100))
        1    -> CounterOp.Get
        else -> CounterOp.Reset
    }
}

class CounterBehaviorProperty : StringSpec({
    "kotlin-otp counter produces identical replies to Erlang for any message sequence".config(
        enabled = erlangAvailable(),
    ) {
        checkAll(Arb.list(Arb.counterOp, 1..50)) { ops ->
            withErlangNode { erlNode ->
                val kotlinRef = GenServers.startLink(testScope, KotlinCounterServer())
                val kotlinReplies = ops.map { op ->
                    when (op) {
                        is CounterOp.Add -> kotlinRef.call<Any>(Pair("add", op.n))
                        CounterOp.Get    -> kotlinRef.call<Any>("get")
                        CounterOp.Reset  -> { kotlinRef.cast("reset"); "cast_ok" }
                    }
                }
                val erlangReplies = ops.map { op -> sendToErlang(erlNode, op) }
                kotlinReplies shouldBe erlangReplies
                kotlinRef.stop()
            }
        }
    }
})
```

### Why this matters

When a behavioral divergence exists — wrong crash reason encoding, off-by-one in restart counts, wrong state after a `reset` followed by `get` — a fixed test sequence may not catch it. The property test will. This is the point where kotlin-otp stops being "OTP-shaped" and starts being *OTP-correct*.

**OTP source:** the spec is the Erlang source itself. `checkAll` with 1000 iterations is a partial but practical substitute for a full formal proof.

---

## 9. `erts_debug` surface — object graph sizing and scheduler introspection (`otp-recon` extension)

### The real gap

`erts_debug:size(Term)` counts the number of Erlang words a term occupies. `erts_debug:flat_size(Term)` counts the shallow size. These are used by `recon` to identify memory-heavy processes and by Mnesia to estimate table sizes.

`erlang:statistics(scheduler_wall_time)` returns per-scheduler utilisation; this is what monitoring systems poll to detect hot schedulers.

### JVM equivalents

```kotlin
/**
 * Approximate the size in bytes of [obj]'s object graph.
 *
 * Uses a breadth-first traversal via reflection, skipping already-visited objects.
 * Returns an estimate — it does not account for JVM object header overhead precisely
 * but is accurate enough for ranking processes by memory footprint.
 *
 * Analogous to erts_debug:size/1 (which counts BEAM words, not bytes).
 *
 * OTP source: erts/emulator/beam/erl_debug.c — size_object/1
 */
object ErtsDump {
    fun size(obj: Any?): Long {
        if (obj == null) return 0L
        val visited = Collections.newSetFromMap(IdentityHashMap<Any, Boolean>())
        val queue: ArrayDeque<Any> = ArrayDeque()
        queue.add(obj)
        var total = 0L
        while (queue.isNotEmpty()) {
            val current = queue.removeFirst()
            if (!visited.add(current)) continue
            total += shallowSizeOf(current)
            for (field in allFields(current.javaClass)) {
                field.isAccessible = true
                val child = runCatching { field.get(current) }.getOrNull()
                if (child != null && child !is Class<*>) queue.add(child)
            }
        }
        return total
    }

    private fun shallowSizeOf(obj: Any): Long = when (obj) {
        is ByteArray    -> obj.size.toLong() + 16L
        is IntArray     -> obj.size * 4L + 16L
        is LongArray    -> obj.size * 8L + 16L
        is Array<*>     -> obj.size * 8L + 16L
        is String       -> obj.length * 2L + 40L
        else            -> 16L  // object header estimate
    }

    private fun allFields(cls: Class<*>): List<java.lang.reflect.Field> =
        buildList {
            var c: Class<*>? = cls
            while (c != null && c != Any::class.java) {
                addAll(c.declaredFields.filter { !java.lang.reflect.Modifier.isStatic(it.modifiers) })
                c = c.superclass
            }
        }
}

/**
 * Per-scheduler utilisation — analogous to erlang:statistics(scheduler_wall_time).
 *
 * Returns a snapshot of JVM thread pool utilisation for the coroutine dispatchers.
 * Two snapshots taken [windowMs] apart give a utilisation ratio.
 */
object SchedulerStats {
    data class Sample(val timestamp: Long, val activeThreads: Int, val poolSize: Int)

    fun sample(): Sample {
        val mxBean = java.lang.management.ManagementFactory.getThreadMXBean()
        val active = mxBean.threadCount
        val available = Runtime.getRuntime().availableProcessors()
        return Sample(System.nanoTime(), active, available)
    }

    fun utilisation(before: Sample, after: Sample): Double =
        if (after.poolSize == 0) 0.0
        else after.activeThreads.toDouble() / after.poolSize.toDouble()
}
```

### Integration with `Recon`

```kotlin
// Extension on Recon:
fun processSize(attribute: ProcessAttribute, n: Int): List<Pair<ProcessInfo, Long>> =
    ProcessTable.all()
        .map { it to ErtsDump.size(it) }  // size the ProcessInfo snapshot itself
        .sortedByDescending { it.second }
        .take(n)
```

**OTP source:** `erts/emulator/beam/erl_debug.c` — `size_object`, `flat_size_object`; `erts/emulator/beam/erl_process.c` — `statistics_scheduler_wall_time`.

---

## 10. `net_kernel` basics — nodeup / nodedown signals (`otp-distribution` extension)

### The real gap

`net_kernel:connect_node(Node)` initiates a distribution connection. When a node connects or disconnects, all processes that called `erlang:monitor_node(Node, true)` receive `{nodeup, Node}` or `{nodedown, Node}`. This is the heartbeat of cluster membership.

Our `InMemoryTransport` connects nodes without lifecycle signals. When a remote `LocalNode` stops, connected actors don't know about it.

### `NodeMonitor` and lifecycle signals

```kotlin
/**
 * Erlang node lifecycle event — analogous to {nodeup, Node} / {nodedown, Node}.
 *
 * OTP source: lib/kernel/src/net_kernel.erl — nodeup/nodedown delivery
 */
sealed class NodeEvent : InfoMsg {
    data class NodeUp(val nodeId: NodeId) : NodeEvent()
    data class NodeDown(val nodeId: NodeId, val reason: String) : NodeEvent()
}

/**
 * Subscribe to lifecycle events for [nodeId].
 * The calling actor's handleInfo receives [NodeEvent.NodeUp] and [NodeEvent.NodeDown].
 *
 * Analogous to erlang:monitor_node/2.
 */
object NodeMonitor {
    private val subscribers = ConcurrentHashMap<NodeId, CopyOnWriteArrayList<GenServerRef<*>>>()

    fun monitorNode(nodeId: NodeId, watcher: GenServerRef<*>): AutoCloseable {
        subscribers.getOrPut(nodeId) { CopyOnWriteArrayList() }.add(watcher)
        watcher.job.invokeOnCompletion { demonitorNode(nodeId, watcher) }
        return AutoCloseable { demonitorNode(nodeId, watcher) }
    }

    fun demonitorNode(nodeId: NodeId, watcher: GenServerRef<*>) {
        subscribers[nodeId]?.remove(watcher)
    }

    internal fun notifyUp(nodeId: NodeId) {
        subscribers[nodeId]?.forEach { it.sendControl(NodeEvent.NodeUp(nodeId)) }
    }

    internal fun notifyDown(nodeId: NodeId, reason: String) {
        subscribers[nodeId]?.forEach { it.sendControl(NodeEvent.NodeDown(nodeId, reason)) }
        subscribers.remove(nodeId)
    }
}
```

### Wire into `InMemoryTransport`

```kotlin
// In InMemoryTransport.connect / disconnect:
fun connect(a: LocalNode, b: LocalNode) {
    // ... existing transport wiring ...
    NodeMonitor.notifyUp(a.id)
    NodeMonitor.notifyUp(b.id)
}

fun disconnect(nodeId: NodeId, reason: String = "disconnected") {
    // ... existing cleanup ...
    NodeMonitor.notifyDown(nodeId, reason)
}
```

### Pattern: cluster-aware supervisor

```kotlin
class ClusterAwareSupervisor : GenServer<Set<NodeId>> {
    override suspend fun init(): InitResult<Set<NodeId>> {
        NodeMonitor.monitorNode(NodeId("peer@host"), ref)
        return InitResult.Ok(emptySet())
    }

    override suspend fun handleInfo(msg: InfoMsg, state: Set<NodeId>): NoreplyResult<Set<NodeId>> =
        when (msg) {
            is NodeEvent.NodeUp   -> NoreplyResult.Noreply(state + msg.nodeId)
            is NodeEvent.NodeDown -> {
                // Trigger failover: restart children that had affinity for the lost node
                NoreplyResult.Noreply(state - msg.nodeId)
            }
            else -> NoreplyResult.Noreply(state)
        }
}
```

**OTP source:** `lib/kernel/src/net_kernel.erl` — `handle_nodeup/1`, `monitor_node/2`; `erts/emulator/beam/dist.c` — `erts_do_net_exits`.

---

## Suggested order

```
§3  (call flush / ServerDownException)  → One-hour fix; high correctness value.
§2  (hibernate)                         → Self-contained run-loop extension.
§10 (net_kernel / NodeMonitor)          → Wires into existing InMemoryTransport.
§6  (ApplicationController)             → Builds on existing OtpApplication API.
§4  (Tracer)                            → New module; zero external deps.
§9  (erts_debug / SchedulerStats)       → Extend otp-recon; pure library work.
§5  (dets)                              → Requires careful WAL design; one sprint.
§7  (appup / AppupRunner)               → Extends otp-hotcode; builds on §6.
§8  (property-based equivalence)        → Requires kotest-property + Erlang node.
§1  (compiler plugin)                   → Largest scope; own module; save for last.
```

---

## What the library becomes at the end of this roadmap

At the completion of THE_ABYSS, kotlin-otp closes the last meaningful gap between "OTP-shaped" and "OTP-correct":

- **Formally verified behaviour** — property tests prove identical reply traces to real Erlang under any message sequence, not just scripted ones.
- **Compiler-enforced preemption** — reduction injection is automatic for annotated packages; no manual `reduce()` calls required.
- **Full crash safety** — `ServerDownException` propagates server exits to callers; no silent hangs.
- **Memory efficiency** — actor hibernation releases call stack frames on idle actors, approximating BEAM heap compaction.
- **On-disk tables** — `DetsTable` persists across restarts with WAL recovery.
- **Formal upgrade scripts** — `AppupRunner` interprets structured upgrade instructions rather than ad-hoc class swaps.
- **Cluster membership** — `NodeMonitor` delivers `nodeup`/`nodedown` signals, completing the distributed supervision story.
- **Runtime introspection** — object graph sizing and scheduler utilisation complete the `recon`/`erts_debug` surface.
- **Production tracing** — the trace framework enables real-time observability without polling.

The remaining semantic gap after this roadmap is genuine and irreducible: **shared JVM GC** (no per-process heap compaction), **no BEAM-level instruction stepping** (Kotlin suspend points are coarser than BEAM reductions), and **no real Erlang distribution wire protocol** (we use jinterface as a bridge, not a full DIST implementation). These are JVM constraints, not library limitations.

Update [TRACEABILITY.md](TRACEABILITY.md) and [LIMITATIONS.md](LIMITATIONS.md) as each section lands.
