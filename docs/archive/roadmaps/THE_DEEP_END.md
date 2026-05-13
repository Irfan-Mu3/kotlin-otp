# The deep end: making kotlin-otp go all the way

> **Status: NOT STARTED (2026-05-02).** [DEEPER_STILL.md](DEEPER_STILL.md) is complete (all 13 sections). Several sections here are now unblocked by that work: §4 (hot code) requires `sys.replace_state` ✓, §5 (match spec DSL) requires `OtpTable` ✓, §6 (`process_info`) requires the sys channel ✓ and links ✓. See per-section status markers below.

This is the fourth roadmap. It follows [DEEPER_STILL.md](DEEPER_STILL.md) and exists specifically to correct the mistake at the end of that document, where five items were labelled "out of scope for a pure-library approach."

They are not out of scope. They are **deep library extensions** — harder and more engineering-intensive than everything before, but each is implementable as a kotlin-otp module. Nothing here requires forking the JVM or abandoning Kotlin. The work is large; the ambition is to eventually make kotlin-otp actors semantically indistinguishable from Erlang processes to anything measuring their behaviour, not just their structure.

---

## 1. Per-actor memory discipline (`otp-memory`) — READY

### The real gap

OTP processes have per-process heaps. Each process allocates independently; a crash clears only that process's heap; GC pauses in one process do not stop others. The JVM has none of this — all actors share one heap, one GC. You cannot change that without forking OpenJDK.

What you *can* do is enforce the **discipline** that makes per-process GC valuable, and instrument the **accounting** that makes heap-per-process observable:

1. **Ownership enforcement via `Isolated<T>`:** prevent shared mutable state at the library level.
2. **Off-heap actor state via Panama** (`java.lang.foreign.MemorySegment`, Java 21+): actor state lives in an explicit arena that is freed when the actor terminates — actual isolated memory, not on the GC heap.
3. **Per-actor allocation accounting** via `java.lang.instrument.Instrumentation`: count bytes allocated per coroutine, surface them in `sys.get_status`.

### `Isolated<T>` — ownership enforcement

```kotlin
/**
 * A value that may only be read by the actor that owns it.
 * Attempting to read it from a different coroutine throws.
 *
 * Enforce the OTP discipline: don't share mutable state between actors.
 * Pass immutable copies (data classes) in messages instead.
 */
class Isolated<T>(
    private val value: T,
    private val ownerJob: Job,
) {
    fun get(): T {
        check(coroutineContext[Job] == ownerJob) {
            "Isolated<T> read from wrong actor — pass an immutable copy in the message instead"
        }
        return value
    }
}

// In GenServers.startLink, bind Isolated values to the server's job
fun <T> GenServerRef<S>.isolated(value: T): Isolated<T> =
    Isolated(value, job)
```

This does not give isolated GC. It gives the **discipline** check: if you ever accidentally share a mutable `Isolated` across actor boundaries, you get a clear runtime error instead of a silent data race. The Erlang compiler enforces this with immutable terms at the language level; we enforce it at the library level with a wrapper.

### Off-heap actor arena (`otp-memory` module)

Panama's `MemorySegment` / `Arena` (Java 21+) lets you allocate memory that is **not on the GC heap** and explicitly freed:

```kotlin
/**
 * A memory arena scoped to a single actor's lifetime.
 *
 * When the actor's Job completes (normally or via exception), the arena is
 * closed and all off-heap memory is freed — independently of the GC.
 * This approximates OTP's per-process heap: the memory is isolated, and
 * its cleanup is deterministic.
 */
class ActorArena : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<ActorArena>
    override val key get() = Key

    private val arena: Arena = Arena.ofShared()
    val allocator: SegmentAllocator get() = arena

    fun close() { arena.close() }
}

// Attach to a GenServer's coroutine context
fun <S> GenServers.startLink(
    parent: CoroutineScope,
    server: GenServer<S>,
    context: CoroutineContext = Dispatchers.Default,
    name: String? = null,
    withArena: Boolean = false,
): GenServerRef<S> {
    val arenaCtx = if (withArena) ActorArena() else EmptyCoroutineContext
    val job = parent.launch(context + arenaCtx) {
        try {
            runLoop(...)
        } finally {
            coroutineContext[ActorArena]?.close()  // arena freed on actor death
        }
    }
    ...
}
```

Actor state that is performance-critical or must not cause GC pauses can be stored in the arena's native memory (structs via Panama's `MemoryLayout`, direct buffers for I/O, etc.). When the actor dies — whether normally, from a crash, or via supervisor shutdown — the arena is closed deterministically, without waiting for the GC. The rest of the heap is still shared, but the arena-resident part is fully isolated in its lifetime.

**What this unlocks:** actors doing heavy I/O (network buffers, file mappings, SIMD processing) can allocate in their arena without GC pressure. A supervisor that kills a misbehaving child cleans up all its off-heap allocations at the same moment.

### Per-actor allocation accounting

Use `java.lang.instrument.Instrumentation` (requires a `-javaagent` at JVM startup) to get `getObjectSize`:

```kotlin
// otp-memory-agent: a tiny Java agent
class AllocationTracker {
    companion object {
        private var instrumentation: Instrumentation? = null

        fun install(inst: Instrumentation) { instrumentation = inst }

        /** Approximate shallow size of obj on this actor's heap. */
        fun sizeOf(obj: Any): Long =
            instrumentation?.getObjectSize(obj) ?: 0L
    }
}

// In OtpLogContext / SysStatus (DEEPER_STILL §3):
data class SysStatus(
    ...
    val heapBytes: Long,   // shallow; requires agent
)
```

Without the agent, this degrades gracefully to 0. With it, `sys:get_status` returns a real heap size estimate, enabling `observer`-style memory monitoring per actor.

---

## 2. Cooperative reduction counting (`otp-scheduler`) — READY

### The real gap

The BEAM preempts processes after approximately 2 000 reductions — each BIF call, pattern match, and function call burns a few reductions. When the budget is exhausted, the scheduler saves the process's continuation and runs another. This guarantees **fairness**: no single process starves the scheduler. On the JVM, no such mechanism exists for arbitrary Kotlin code.

But "no mechanism exists" does not mean "cannot be added". There are three implementation levels:

### Level 1: Cooperative reduction budget (zero tooling, purely library)

Add a `ReductionBudget` to the coroutine context. Actors that want to be fair call `reduce()` in their hot paths:

```kotlin
/**
 * Coroutine-scoped reduction counter.
 *
 * Call [reduce] in loops or after each significant unit of work.
 * When the budget is exhausted, [reduce] yields to the coroutine scheduler,
 * giving other actors a chance to run.
 *
 * JVM / OTP difference: OTP reductions are counted automatically by the BEAM
 * for every BIF and function call. Here, the actor must call [reduce] explicitly.
 * Level 2 (compiler plugin) or Level 3 (Java agent) automate the injection.
 */
class ReductionBudget(
    val limit: Int = 2_000,
) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<ReductionBudget>
    override val key get() = Key

    private var count = 0

    suspend fun reduce(n: Int = 1) {
        count += n
        if (count >= limit) {
            count = 0
            yield()
        }
    }
}

// Install in startLink context
// Usage inside a GenServer callback:
override suspend fun handleCast(request: Any, state: S): NoreplyResult<S> {
    val budget = coroutineContext[ReductionBudget]
    repeat(10_000) { i ->
        budget?.reduce()
        // ... work ...
    }
    return NoreplyResult.Noreply(state)
}
```

The run loop itself should call `reduce(1)` after each message processed, making the scheduler fairness automatic for the loop overhead even without annotation.

### Level 2: Kotlin compiler plugin (automatic injection)

A Kotlin compiler plugin (IR-level) transforms functions annotated with `@Preemptible` or inside packages matching a configurable glob to inject `reduce()` calls at every loop back-edge and suspend point:

```kotlin
// Plugin annotation
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class Preemptible(val reductionsPerCall: Int = 1)

// Before plugin:
@Preemptible
suspend fun heavyCompute(n: Int): Long {
    var acc = 0L
    for (i in 0..n) acc += i
    return acc
}

// After plugin transforms (conceptually):
suspend fun heavyCompute(n: Int): Long {
    val budget = coroutineContext[ReductionBudget]
    var acc = 0L
    for (i in 0..n) {
        budget?.reduce(1)   // injected at loop back-edge
        acc += i
    }
    return acc
}
```

**Implementation path:** use the Kotlin IR backend's `IrGenerationExtension`. At each `IrLoop` back-edge, inject a call to `ReductionBudget.reduce()` retrieved via `coroutineContext`. This is the same technique Kotlin's suspend-function transformation uses — you are adding another pass in the same IR pipeline. The Kotlin compiler plugin API is stable from Kotlin 2.0.

### Level 3: Java agent with Byte Buddy (automatic, no source changes)

The most faithful approximation of BEAM reduction counting. A Byte Buddy Java agent transforms all class files whose names start with `org.otpstudy` (configurable) at load time, injecting `reduce()` calls without any source annotation:

```kotlin
// otp-agent module (separate JAR, -javaagent:otp-agent.jar)
class ReductionCountingAgent {
    companion object {
        @JvmStatic
        fun premain(args: String?, instrumentation: Instrumentation) {
            AgentBuilder.Default()
                .type(ElementMatchers.nameStartsWith("org.otpstudy"))
                .transform { builder, _, _, _, _ ->
                    builder.visit(
                        Advice.to(ReduceAdvice::class.java)
                            .on(ElementMatchers.isMethod())
                    )
                }
                .installOn(instrumentation)
        }
    }
}

class ReduceAdvice {
    companion object {
        @Advice.OnMethodEnter
        @JvmStatic
        fun onEnter() {
            // Increment per-coroutine counter; yield if budget exceeded
            ReductionBudget.currentOrNull()?.reduceSync(1)
        }
    }
}
```

`reduceSync` is a non-suspend variant that uses `runBlocking { yield() }` on budget exhaust — acceptable overhead for the rare preemption event. The rest of the time it is one atomic increment, matching BEAM's per-reduction cost model.

**What this gives you:** any kotlin-otp actor running under this agent is automatically preempted after ~2 000 method calls, with zero source changes. Tight CPU loops no longer starve other actors on the same thread. This is the closest the JVM gets to BEAM scheduling without patching the VM.

---

## 3. Real Erlang interop via `jinterface` (`otp-jinterface`) — READY

### The real gap

Wire-compatible `erl_dist` from scratch is 10 000+ lines of Erlang distribution protocol, EPMD, and handshake. You do not need to write that: **Erlang ships a supported Java library that does it for you.**

`jinterface` is an official Erlang/OTP application (`lib/jinterface/`) that implements the full distribution protocol in pure Java. It has been stable since OTP R9. Its classes are in `com.ericsson.otp.erlang.*` and the jar is `jinterface.jar` (bundled in every OTP installation).

With `jinterface`, a kotlin-otp actor can:

- Be visible as a real Erlang process to `observer` and any OTP cluster.
- Receive `gen_server:call` requests from Erlang code.
- Send messages to real Erlang PIDs.
- Be supervised by an OTP supervisor on an Erlang node.
- Participate in a distributed supervision tree spanning JVM and BEAM nodes.

### `otp-jinterface` module design

```kotlin
/**
 * A kotlin-otp actor node that is a real participant in an Erlang cluster.
 *
 * Start an [OtpErlangNode] to join the cluster; then use [expose] to make
 * individual [GenServerRef]s visible as named registered processes on this node.
 *
 * Erlang code can then call:
 *     gen_server:call({my_server, 'myapp@localhost'}, Request)
 * and reach the Kotlin [GenServer] directly.
 */
class OtpErlangNode(
    val nodeName: String,         // e.g. "myapp@localhost"
    val cookie: String,           // Erlang distribution cookie
    private val scope: CoroutineScope,
) {
    private val node = com.ericsson.otp.erlang.OtpNode(nodeName, cookie)

    /**
     * Expose [ref] as a registered process named [erlangName] on this node.
     *
     * Incoming Erlang gen_server calls are decoded, dispatched to [ref.call],
     * and the reply is sent back as an Erlang term.
     * Incoming casts are dispatched to [ref.cast].
     *
     * [codec] translates between [OtpErlangObject] and your Kotlin request/reply types.
     */
    fun <Req, Rep> expose(
        erlangName: String,
        ref: GenServerRef<*>,
        codec: ErlangCodec<Req, Rep>,
    ) {
        val mbox = node.createMbox(erlangName)
        scope.launch {
            while (isActive) {
                val msg = mbox.receive() ?: break
                handleErlangMessage(msg, ref, codec, mbox)
            }
        }
    }

    /**
     * Return a [GenServerRef] proxy that sends messages to a real Erlang process.
     * Calls are encoded as Erlang gen_server call tuples, sent over distribution,
     * and the reply decoded back.
     */
    fun <Req, Rep> remoteRef(
        erlangName: String,
        targetNode: String,
        codec: ErlangCodec<Req, Rep>,
    ): GenServerRef<Nothing> { ... }

    fun shutdown() { node.close() }
}

interface ErlangCodec<Req, Rep> {
    fun decodeRequest(term: OtpErlangObject): Req
    fun encodeReply(reply: Rep): OtpErlangObject
    fun encodeRequest(req: Req): OtpErlangObject
    fun decodeReply(term: OtpErlangObject): Rep
}
```

**`handleErlangMessage`** decodes the standard gen_server call wire format:
- `{'$gen_call', {CallerPid, Ref}, Payload}` → decode payload, call `ref.call(req)`, send `{Ref, Reply}` back.
- `{'$gen_cast', Payload}` → decode payload, call `ref.cast(req)`.
- `{'DOWN', Ref, process, Pid, Reason}` → deliver as `InfoMsg.Down`.

### What this opens up

- **Real `observer` visibility:** your kotlin-otp actors appear in Erlang's `observer:start()`, with their message queue depth, registered name, and status.
- **Mixed-language supervision trees:** an OTP supervisor on the Erlang side can have a kotlin-otp `GenServerRef` as a child via the `supervisor_bridge` equivalent.
- **Gradual migration:** Erlang systems can be migrated service-by-service to Kotlin while remaining in the same supervision hierarchy.
- **Testing against real OTP:** write property tests that start a real Erlang node in the test process (`Port` or `ErlangContainer`) and verify that your Kotlin actor behaves identically to a reference Erlang implementation.

**OTP source:** `lib/jinterface/java_src/com/ericsson/otp/erlang/` — `OtpNode`, `OtpMbox`, `OtpErlangPid`, `OtpErlangObject` are all there, well-documented and already shipped with every OTP installation.

---

## 4. Hot code upgrade (`otp-hotcode`) — READY (requires DEEPER_STILL §2 sys.replace_state ✓)

### The real gap

OTP's hot code upgrade is a first-class runtime feature: `release_handler` unpacks a new `.beam`, loads it alongside the old module (BEAM keeps two versions in memory simultaneously), and running processes stay on the old code until they make an external call, at which point they migrate to the new version and `code_change/3` is invoked to transform their state.

The JVM's `ClassLoader` gives you the building blocks for the same thing. You cannot do in-place instruction replacement, but you can:

1. Load the new class version alongside the old one.
2. On the next message boundary, check whether an upgrade is pending.
3. Call `codeChange` to transform the state from the old schema to the new.
4. Replace the `server` reference with a new instance of the upgraded class.

All of this is standard Java class-loading technique — OSGi, JRebel, and Quarkus dev mode all use it.

### `GenServer.codeChange` callback

Add one method to the `GenServer` interface:

```kotlin
interface GenServer<S> {
    ...
    /**
     * Analogous to OTP `code_change/3`.
     *
     * Called when a hot upgrade is installed while this actor is running.
     * Return the migrated state compatible with the new version.
     *
     * [oldVersion] and [newVersion] are arbitrary version strings from the
     * upgrade descriptor (analogous to OTP vsn attribute and .appup files).
     *
     * Default: state is unchanged (compatible upgrade).
     */
    suspend fun codeChange(
        oldVersion: String,
        newVersion: String,
        state: S,
    ): S = state
}
```

### `otp-hotcode` module

```kotlin
/**
 * Registry of pending hot upgrades.
 *
 * Usage:
 *   // At deploy time:
 *   CodeChangeRegistry.schedule(
 *       serverName = "counter",
 *       newClass = CounterServerV2::class.java,
 *       newVersion = "2.0",
 *   )
 *
 *   // The GenServer run loop checks this on each message boundary.
 */
object CodeChangeRegistry {
    private val pending = ConcurrentHashMap<String, PendingUpgrade>()

    data class PendingUpgrade(
        val newClass: Class<out GenServer<*>>,
        val oldVersion: String,
        val newVersion: String,
    )

    fun schedule(
        serverName: String,
        newClass: Class<out GenServer<*>>,
        oldVersion: String = "1.0",
        newVersion: String = "2.0",
    ) {
        pending[serverName] = PendingUpgrade(newClass, oldVersion, newVersion)
    }

    fun consumeUpgrade(serverName: String): PendingUpgrade? =
        pending.remove(serverName)
}
```

In the GenServer run loop, between messages:

```kotlin
// At top of each iteration in runLoop:
val upgrade = actorName?.let { CodeChangeRegistry.consumeUpgrade(it) }
if (upgrade != null) {
    val newInstance = upgrade.newClass.getDeclaredConstructor().newInstance()
    @Suppress("UNCHECKED_CAST")
    state = (newInstance as GenServer<S>).codeChange(
        upgrade.oldVersion, upgrade.newVersion, state,
    )
    server = newInstance   // swap the running implementation
    OtpLogging.log(Info, ctx, "hot code change applied: ${upgrade.newVersion}")
}
```

### Loading the new class

The `newClass` in `CodeChangeRegistry.schedule` comes from a `URLClassLoader` pointed at the new JAR:

```kotlin
fun loadUpgrade(jarPath: Path, className: String): Class<out GenServer<*>> {
    val loader = URLClassLoader(
        arrayOf(jarPath.toUri().toURL()),
        Thread.currentThread().contextClassLoader,
    )
    @Suppress("UNCHECKED_CAST")
    return loader.loadClass(className) as Class<out GenServer<*>>
}
```

The old class stays in memory until the old `server` reference is GC'd. The new class runs alongside it — exactly what BEAM does with two module versions.

### `.appup` analog: upgrade descriptors

OTP uses `.appup` files to describe upgrade/downgrade instructions. A Kotlin equivalent:

```kotlin
data class UpgradeDescriptor(
    val fromVersion: String,
    val toVersion: String,
    val steps: List<UpgradeStep>,
)

sealed class UpgradeStep {
    data class LoadModule(val jarPath: Path, val className: String) : UpgradeStep()
    data class ApplyCodeChange(val serverName: String) : UpgradeStep()
    data class RestartApplication(val appName: String) : UpgradeStep()
}

object ReleaseHandler {
    suspend fun apply(descriptor: UpgradeDescriptor) { ... }
    suspend fun rollback(descriptor: UpgradeDescriptor) { ... }
}
```

This is `release_handler` at the library level. `apply` walks the steps; `rollback` applies them in reverse. Not binary-compatible with OTP `.appup`, but semantically equivalent.

---

## 5. Match specs as a typed Kotlin DSL (`OtpTable` extension) — READY (requires DEEPER_STILL §11 OtpTable ✓)

### The real gap

OTP ETS match specs are a Prolog-like pattern-matching compiled language:

```erlang
ets:select(Tab, [{ {'$1','$2'}, [{'>', '$2', 5}], ['$1'] }])
```

This reads: "for all rows `{K, V}`, where `V > 5`, return `K`." Match specs are compiled by the BEAM into native code and run inside the ETS lock. Writing them by hand is famously unpleasant; `ms_transform` provides a parse-transform that lets you write them as Erlang fun syntax:

```erlang
ets:fun2ms(fun({K, V}) when V > 5 -> K end)
```

### Kotlin equivalent: type-safe match spec DSL

Kotlin's type system, reified generics, and lambda syntax give you something *better* than ETS match specs — compile-time type safety:

```kotlin
/**
 * Type-safe ETS-style match spec builder.
 *
 * Example:
 *   val spec = matchSpec<String, Int> {
 *       guard { (_, v) -> v > 5 }
 *       project { (k, _) -> k }
 *   }
 *   val keys: List<String> = table.select(spec)
 */
class MatchSpec<K, V, R>(
    val guard: ((K, V) -> Boolean)?,
    val projection: (K, V) -> R,
)

fun <K, V, R> matchSpec(
    block: MatchSpecBuilder<K, V, R>.() -> Unit,
): MatchSpec<K, V, R> {
    val builder = MatchSpecBuilder<K, V, R>()
    builder.block()
    return builder.build()
}

class MatchSpecBuilder<K, V, R> {
    private var guard: ((K, V) -> Boolean)? = null
    private var projection: ((K, V) -> R)? = null

    fun guard(predicate: (Pair<K, V>) -> Boolean) {
        guard = { k, v -> predicate(k to v) }
    }
    fun project(selector: (Pair<K, V>) -> R) {
        projection = { k, v -> selector(k to v) }
    }
    fun build() = MatchSpec(guard, checkNotNull(projection) { "project is required" })
}

// On OtpTable:
fun <R> select(spec: MatchSpec<K, V, R>): List<R>
fun <R> selectContinuation(spec: MatchSpec<K, V, R>, limit: Int): Pair<List<R>, Continuation?>
```

### Compiled match specs for `OrderedSet`

For `OtpTable(type = TableType.OrderedSet)`, the underlying `ConcurrentSkipListMap` supports efficient range scans. A `guard` of the form `{ (k, _) -> k in low..high }` can be recognised and optimised to `subMap(low, high)`:

```kotlin
// Specialised path for range queries on ordered tables
sealed class Guard<K, V> {
    data class Range<K : Comparable<K>, V>(val low: K, val high: K) : Guard<K, V>()
    data class Lambda<K, V>(val predicate: (K, V) -> Boolean) : Guard<K, V>()
}
```

OTP's `ets:select` on `ordered_set` does the same optimisation — it converts a match spec with key variable bounds into a range scan. The Kotlin version makes the optimisation explicit in the type rather than in a runtime analysis of a Prolog term.

---

## 6. `process_info` and per-actor observability hooks — BLOCKED on §1 (memory) + §2 (reductions); DEEPER_STILL deps met (sys ✓, links ✓)

### The gap

OTP's `erlang:process_info/2` is the single most powerful introspection primitive in the language. From any process, you can ask any other process:

```erlang
erlang:process_info(Pid, [message_queue_len, memory, current_function, status])
```

Everything in `observer` is built on this. It returns runtime data that the BEAM tracks per-process by default — queue length, heap size, stack trace, GC metadata, registered name, links, monitors, trap_exit flag, current execution point.

The JVM does not expose this per-thread/per-Job. But most of it can be approximated:

| `process_info` key | JVM source |
|---|---|
| `message_queue_len` | `Channel.isEmpty` / explicit counter |
| `memory` | `ActorArena.bytesAllocated` (§1) + estimate via instrumentation |
| `current_function` | `Thread.getStackTrace()` on the carrier thread |
| `status` | coroutine `Job.isActive / isCancelled / isCompleted` |
| `registered_name` | `ProcessRegistry.lookup` inverse |
| `links` | link table from §8 in DEEPER_STILL |
| `trap_exit` | `TrapExitHandler` presence flag |
| `reductions` | `ReductionBudget.totalReductions` (§2 this doc) |
| `garbage_collection` | not available without agent; use arena bytes as proxy |

### `ProcessInfo` data class and hook

```kotlin
data class ProcessInfo(
    val id: OtpProcessId,
    val registeredName: String?,
    val status: ProcessStatus,
    val messageQueueLen: Int,
    val reductions: Long,
    val memoryBytes: Long,
    val currentStackTrace: List<StackTraceElement>,
    val links: List<OtpProcessId>,
    val trapExit: Boolean,
    val supervisorPath: List<String>,
)

enum class ProcessStatus { Running, Waiting, Suspended, Dead }

// Global process info registry (populated by startLink, cleaned up on completion)
object ProcessTable {
    fun register(id: OtpProcessId, probe: () -> ProcessInfo)
    fun unregister(id: OtpProcessId)
    fun info(id: OtpProcessId): ProcessInfo?
    fun all(): List<ProcessInfo>
}
```

Each `startLink` registers a `probe` lambda that captures its `mailbox`, `job`, `ReductionBudget`, `ActorArena`, and link table. When `ProcessTable.info(id)` is called, the probe is invoked and the data is aggregated. This is O(1) per actor (no scanning), matching the BEAM's per-process metadata storage.

---

## Suggested order

These items are more independent than previous roadmaps. The dependencies are:

```
§3 (jinterface) → depends on nothing; highest external impact; do first.
§4 (hot code)   → requires sys.replace_state — DONE (DEEPER_STILL §2). Do now.
§5 (match spec) → requires OtpTable — DONE (DEEPER_STILL §11). Do now.
§2 level 1      → cooperative reduction budget; self-contained; do alongside §4.
§1 (memory)     → Isolated<T> first (easy); off-heap arena later (requires Panama).
§6 (process_info) → requires §1 + §2 counters; sys ✓ (DS §2) and links ✓ (DS §7).
§2 level 2/3    → compiler plugin / agent; last because it requires build tooling work.
```

**DEEPER_STILL is complete.** The previously blocked items (§4, §5) are now unblocked. Recommended milestone sequence from here:

1. **`otp-jinterface` module** — `OtpErlangNode.expose` + a working echo server test against a real OTP node. Immediate win: your actors are real Erlang processes visible in `observer`. Add to TRACEABILITY.
2. **`codeChange` callback + `CodeChangeRegistry`** (`otp-hotcode`) — test that hot-swaps a running counter server and verifies state migration. Previously blocked; now unblocked by `sys.replace_state`.
3. **Match spec DSL** on `OtpTable` — `select(matchSpec { guard {...}; project {...} })`. Previously blocked; now unblocked by `OtpTable`.
4. **`Isolated<T>`** wrapper (one file, no dependencies) — enforce ownership discipline across the library.
5. **Cooperative `ReductionBudget`** Level 1 — wire into all three run loops; adds fairness with zero new dependencies.
6. **`ProcessInfo` extended** + hook in `startLink` — queue length, reductions, stack trace, links; completes the observer surface started in DEEPER_STILL §13.
7. **Off-heap `ActorArena`** via Panama — real allocation isolation for performance-critical actors; builds on step 4.
8. **Compiler plugin (Level 2 reductions)** — one sprint of Kotlin IR plugin work.
9. **Java agent (Level 3 reductions)** — automatic preemption without source annotations; highest fidelity to BEAM scheduling.

---

## What this library becomes

At the end of this roadmap, kotlin-otp is no longer "OTP-shaped structure on coroutines." It is:

- **Actors visible in Erlang's `observer`** (via `otp-jinterface`).
- **Supervisable from a real OTP supervisor** on a remote Erlang node.
- **Upgradeable at runtime** without process restart (hot code).
- **Fairly scheduled** via cooperative reduction counting (with optional automatic injection).
- **Memory-disciplined** with ownership enforcement and optional off-heap actor arenas.
- **Fully introspectable** via `ProcessTable` + `sys` module down to queue length, reductions, and stack trace.

**Already in place from DEEPER_STILL:** `ProcessTable` (§13), `sys` channel with `sysGetStatus/ReplaceState/Suspend/Resume` (§2), OTP links + trap_exit (§7), `OtpTable` + `OtpTableRegistry` (§11), in-JVM distribution primitives (§12), `OtpObserver` data layer (§13), selective receive (§5), bounded mailboxes (§1), crash reporting (§8), supervisor bridge (§9), `persistent_term` (§10).

The remaining semantic gaps are narrow and well-documented: no per-actor GC (shared heap, but arenas for performance-critical data), no BEAM-level preemption (cooperative, with agent-assisted injection as the closest approximation). Everything else is now library semantics rather than JVM limitations.

Update [TRACEABILITY.md](../../TRACEABILITY.md) and [LIMITATIONS.md](../../LIMITATIONS.md) as each section lands.
