# Beyond the deep end: OTP's application layer and behavioral proof

> **Status: COMPLETE (2026-05-04).** All 10 sections implemented. 186 tests passing (was 110 at THE_DEEP_END). [THE_DEEP_END.md](THE_DEEP_END.md) was the prior roadmap.

This is the sixth roadmap. Three things distinguish it from what came before.

**First:** two items from THE_DEEP_END were explicitly deferred — the off-heap `ActorArena` (§1, required Panama) and the Kotlin compiler plugin for automatic reduction injection (§2 Level 2). They are unblocked and belong here.

**Second:** every roadmap so far has operated at the **primitive level** — the gen_server loop, the supervisor tree, the mailbox, the sys channel. The previous five documents are about building a BEAM-like runtime. This document is about the **application layer that runs on top of that runtime** — the OTP modules (`pg`, `global`, Mnesia, `sasl`, `logger`, `proc_lib`) that most Erlang programs use constantly but that sit above the VM primitives. These modules are not language features; they are shipped OTP applications. On JVM, they are library work, and they are now unblocked by everything beneath.

**Third:** the culmination. We now have `otp-jinterface`. That means we can write tests that run the same scenario against a real Erlang `gen_server` and a kotlin-otp `GenServer` and assert identical behavior. Behavioral equivalence stops being a claim and becomes a measurement.

---

## 1. Off-heap `ActorArena` (`otp-memory` extension) — READY (requires Java 21+, Panama)

### The real gap

THE_DEEP_END §1 described this but left it unbuilt: actor state that causes GC pressure (large I/O buffers, SIMD-width arrays, ring buffers) should be allocatable in memory that is **freed deterministically when the actor's Job completes**, without waiting for the GC.

The BEAM achieves this via per-process heaps. We cannot replicate per-process GC. But Panama's `MemorySegment` / `Arena` (stable since Java 21) gives us **explicit lifetime management for off-heap memory** — and an actor's Job is a perfect lifetime scope.

### `ActorArena` as a coroutine context element

```kotlin
/**
 * An off-heap memory arena scoped to a single actor's lifetime.
 *
 * State allocated in this arena lives outside the GC heap.
 * When the actor's Job completes (normally, cancelled, or crashed),
 * the arena is closed and all allocations are freed — deterministically,
 * without waiting for a GC cycle.
 *
 * This approximates OTP's per-process heap for performance-critical data:
 * the cleanup is deterministic and isolated to this actor.
 *
 * Usage: pass [withArena = true] to [GenServers.startLink]. Access via
 * [coroutineContext[ActorArena]?.allocator] inside any callback.
 *
 * OTP source: erts/emulator/beam/erl_alloc.c — ERTS_ALC_T_HEAP
 */
class ActorArena : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<ActorArena>
    override val key get() = Key

    private val arena: Arena = Arena.ofShared()
    val allocator: SegmentAllocator get() = arena

    private val _bytesAllocated = AtomicLong(0L)
    val bytesAllocated: Long get() = _bytesAllocated.get()

    fun allocate(layout: MemoryLayout): MemorySegment {
        val seg = arena.allocate(layout)
        _bytesAllocated.addAndGet(layout.byteSize())
        return seg
    }

    internal fun close() { arena.close() }
}
```

Wire into `GenServers.startLink` via a `withArena: Boolean = false` parameter:

```kotlin
fun <S> startLink(
    parent: CoroutineScope,
    server: GenServer<S>,
    context: CoroutineContext = Dispatchers.Default,
    name: String? = null,
    mailboxBound: MailboxBound? = null,
    reductionLimit: Int? = null,
    withArena: Boolean = false,              // new
): GenServerRef<S> {
    val arenaCtx: CoroutineContext = if (withArena) ActorArena() else EmptyCoroutineContext
    val budget = ReductionBudget(reductionLimit ?: 2_000)
    val job = parent.launch(context + jobName + budget + arenaCtx) {
        try {
            runLoop(...)
        } finally {
            coroutineContext[ActorArena]?.close()   // freed on actor death
        }
    }
    ...
}
```

### Usage: I/O buffer actor with arena-allocated direct memory

```kotlin
class NetworkBufferServer : GenServer<Unit> {
    private lateinit var buffer: MemorySegment

    override suspend fun init(): InitResult<Unit> {
        val arena = coroutineContext[ActorArena]
            ?: return InitResult.Stop(TerminateReason.Normal)
        // 64 KiB ring buffer; freed when this actor dies — no GC involvement
        buffer = arena.allocate(MemoryLayout.sequenceLayout(65_536, ValueLayout.JAVA_BYTE))
        return InitResult.Ok(Unit)
    }

    override suspend fun handleCast(request: Any, state: Unit): NoreplyResult<Unit> {
        // write to buffer.asByteBuffer() — zero-copy, off-heap
        return NoreplyResult.Noreply(Unit)
    }
}

val ref = GenServers.startLink(scope, NetworkBufferServer(), withArena = true)
```

### What `bytesAllocated` unlocks for `ProcessInfo`

Once `ActorArena.bytesAllocated` is wired, `ProcessInfo.memoryBytes` becomes non-zero for actors using arenas, and `sysGetStatus` can report it. This is the last field needed for a complete `erlang:process_info/2` surface:

```
erlang:process_info(Pid, memory)  →  ProcessInfo.memoryBytes
```

---

## 2. Kotlin compiler plugin — automatic reduction injection (Level 2)

### The real gap

THE_DEEP_END §2 Level 1 made reduction counting *cooperative*: actors must call `budget.reduce()` manually. Level 2 removes that burden. A Kotlin IR compiler plugin transforms annotated functions to inject `reduce()` calls automatically at every loop back-edge.

### Plugin structure

```kotlin
// In buildSrc or a separate plugin module
class ReductionInjectionPlugin : CompilerPluginRegistrar() {
    override val supportsK2 = true
    override fun ExtensionStorage.registerExtensions(configuration: CompilerConfiguration) {
        IrGenerationExtension.registerExtension(ReductionInjectionExtension())
    }
}

class ReductionInjectionExtension : IrGenerationExtension {
    override fun generate(moduleFragment: IrModuleFragment, pluginContext: IrPluginContext) {
        moduleFragment.transform(ReductionInjectionTransformer(pluginContext), null)
    }
}
```

The transformer visits every `IrLoop` (both `while` and `for`) inside functions annotated with `@Preemptible` or inside packages matching a configurable glob:

```kotlin
class ReductionInjectionTransformer(private val ctx: IrPluginContext) : IrElementTransformerVoid() {
    private val reduceSymbol = ctx.referenceFunctions(
        CallableId(FqName("org.otpstudy.genserver"), Name.identifier("reduce"))
    ).single()

    override fun visitLoop(loop: IrLoop): IrExpression {
        val transformed = super.visitLoop(loop) as IrLoop
        // Inject: budget?.reduce(1) at the back-edge
        val reduceCall = IrCallImpl(..., reduceSymbol).apply {
            putValueArgument(0, IrConstImpl.int(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.intType, 1))
        }
        transformed.body = IrBlockImpl(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.unitType).apply {
            statements += transformed.body!!
            statements += reduceCall
        }
        return transformed
    }
}
```

### Annotation and build wiring

```kotlin
// In otp-gen-server:
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.SOURCE)
annotation class Preemptible(val reductionsPerCall: Int = 1)

// In build.gradle.kts of any actor module:
plugins {
    id("org.otpstudy.reduction-plugin")
}
otpReductions {
    autoInjectPackages = listOf("org.myapp.actors")   // inject without annotation
}
```

### What before and after looks like

```kotlin
// Before plugin (source):
@Preemptible
suspend fun mergeSort(list: List<Int>): List<Int> {
    if (list.size <= 1) return list
    val mid = list.size / 2
    var left = mergeSort(list.subList(0, mid))
    var right = mergeSort(list.subList(mid, list.size))
    return merge(left, right)
}

// After plugin transforms (conceptually):
suspend fun mergeSort(list: List<Int>): List<Int> {
    coroutineContext[ReductionBudget]?.reduce(1)   // injected at function entry
    if (list.size <= 1) return list
    ...
}
```

**Key difference from Level 3 (Java agent):** the plugin works at Kotlin source level — it understands Kotlin suspend functions, null safety, and coroutine context. The Java agent (Level 3) works at bytecode and is language-agnostic but pays extra cost for suspend point detection.

**OTP source:** `erts/emulator/beam/erl_process.c` — the BEAM's scheduler deducts reductions in `process_main`. The plugin is the Kotlin-layer analogue of that.

---

## 3. `pg` — process groups (`otp-pg` module)

### The real gap

OTP's `pg` module (introduced as `pg2` in OTP 10, redesigned as `pg` in OTP 23) is the standard building block for fan-out patterns:

```erlang
pg:join(my_scope, my_group, self()),
Members = pg:get_members(my_scope, my_group),
[Pid ! {event, Data} || Pid <- Members].
```

Phoenix LiveView's broadcast, `libcluster`'s node tracking, RabbitMQ's routing — all use `pg`. The core abstraction: a **scope** is a named namespace (one `gen_server` per scope). Within a scope, processes join named **groups**. Group membership is eventually consistent across nodes.

### `otp-pg` module design

```kotlin
/**
 * Process groups — the OTP pg module for kotlin-otp.
 *
 * A scope is an independent namespace; each scope runs as its own GenServer.
 * Processes join and leave groups within a scope; queries return all current members.
 *
 * OTP source: lib/kernel/src/pg.erl
 */
object Pg {
    private val scopes = ConcurrentHashMap<String, PgScope>()

    fun start(scopeName: String, parent: CoroutineScope): PgScope =
        scopes.getOrPut(scopeName) { PgScope(scopeName, parent) }

    fun scope(scopeName: String): PgScope =
        scopes[scopeName] ?: error("pg scope '$scopeName' not started")

    fun stop(scopeName: String) { scopes.remove(scopeName)?.stop() }
}

class PgScope(val name: String, parent: CoroutineScope) {
    private val groups = ConcurrentHashMap<String, CopyOnWriteArrayList<GenServerRef<*>>>()

    fun join(group: String, member: GenServerRef<*>) {
        groups.getOrPut(group) { CopyOnWriteArrayList() }.add(member)
        // Remove on actor death
        member.job.invokeOnCompletion { leave(group, member) }
    }

    fun leave(group: String, member: GenServerRef<*>) {
        groups[group]?.remove(member)
    }

    fun getMembers(group: String): List<GenServerRef<*>> =
        groups[group]?.toList() ?: emptyList()

    fun whichGroups(): List<String> = groups.keys.toList()

    /** Broadcast a cast to all members of [group]. */
    fun broadcast(group: String, message: Any) {
        getMembers(group).forEach { it.cast(message) }
    }

    /** Broadcast an info message to all members of [group]. */
    fun broadcastInfo(group: String, message: InfoMsg) {
        getMembers(group).forEach { it.sendInfo(message) }
    }

    fun stop() { groups.clear() }
}
```

### Pattern: fan-out event bus

```kotlin
// At startup:
val events = Pg.start("events", scope)

// Each subscriber:
events.join("order.created", orderProcessorRef)
events.join("order.created", auditLogRef)
events.join("order.created", notificationRef)

// Publisher (any actor):
events.broadcast("order.created", OrderCreated(orderId, userId, total))
// All three subscribers receive the cast — identical to pg:get_members/broadcast
```

### Distributed `pg` across nodes

In OTP, `pg` replicates membership across all connected nodes using `net_kernel` monitors. Our `InMemoryTransport` from `otp-distribution` can carry `PgSync` messages between `LocalNode` instances:

```kotlin
// PgSync message: node-to-node membership synchronization
data class PgJoin(val scope: String, val group: String, val memberId: OtpProcessId) : DistributedMsg
data class PgLeave(val scope: String, val group: String, val memberId: OtpProcessId) : DistributedMsg

// PgScope.join sends PgJoin to all peer nodes; each peer updates its local replica.
// On node disconnect, all members from that node are removed (equivalent to pg's DOWN handling).
```

---

## 4. `global` — distributed name registry (`otp-global` module)

### The real gap

`global:register_name(Name, Pid)` registers a name cluster-wide. `global:whereis_name(Name)` resolves it from any node. Conflict resolution (`{M, F, A}` resolver) handles the case where two nodes register the same name before they become aware of each other.

Our `otp-registry` is node-local. The `global` gap is: a registry that uses `otp-distribution`'s transport layer so names registered on one node are visible on others.

```kotlin
/**
 * Distributed global name registry.
 *
 * In single-JVM use: an in-process registry identical to ProcessRegistry.
 * With otp-distribution wired in: name registrations are broadcast to all
 * connected nodes; conflict resolution picks the winner.
 *
 * OTP source: lib/kernel/src/global.erl
 */
object GlobalRegistry {
    private val names = ConcurrentHashMap<String, GenServerRef<*>>()
    var transport: NodeTransport? = null     // null = single-node mode
    var conflictResolver: ConflictResolver = ConflictResolver.KeepFirst

    fun registerName(name: String, ref: GenServerRef<*>): RegisterResult {
        val existing = names.putIfAbsent(name, ref)
        if (existing != null) {
            return when (conflictResolver) {
                ConflictResolver.KeepFirst -> RegisterResult.Conflict(existing)
                ConflictResolver.KeepLast  -> { names[name] = ref; RegisterResult.Ok }
                is ConflictResolver.Custom -> {
                    val winner = (conflictResolver as ConflictResolver.Custom).resolve(name, existing, ref)
                    names[name] = winner
                    RegisterResult.Ok
                }
            }
        }
        transport?.broadcast(GlobalRegistration(name, ref.id))
        ref.job.invokeOnCompletion { unregisterName(name) }
        return RegisterResult.Ok
    }

    fun unregisterName(name: String) { names.remove(name) }
    fun whereisName(name: String): GenServerRef<*>? = names[name]
    fun registeredNames(): Set<String> = names.keys.toSet()

    sealed class RegisterResult {
        object Ok : RegisterResult()
        data class Conflict(val existing: GenServerRef<*>) : RegisterResult()
    }

    sealed class ConflictResolver {
        object KeepFirst : ConflictResolver()
        object KeepLast : ConflictResolver()
        data class Custom(val resolve: (String, GenServerRef<*>, GenServerRef<*>) -> GenServerRef<*>) : ConflictResolver()
    }
}
```

### Conflict resolution — the subtle part

OTP `global`'s default resolver uses a hash of the PID to pick a winner deterministically. The real complexity is **netsplit recovery**: when a partition heals, both sides may have registered the same name. `global` detects this via a lock protocol and terminates one of the conflicting processes. Our `ConflictResolver.Custom` is where you'd implement the same.

---

## 5. Mnesia-inspired transactions — STM on `OtpTable` (`otp-mnesia` module)

### The real gap

Mnesia provides atomic multi-table transactions:

```erlang
mnesia:transaction(fun() ->
    V = mnesia:read(accounts, AccountId),
    mnesia:write(accounts, V#account{balance = V#account.balance - Amount}),
    mnesia:write(audit, #audit_entry{account = AccountId, delta = -Amount})
end).
```

If either write fails, both are rolled back. Mnesia uses MVCC internally: a transaction reads a snapshot of the database at transaction start and writes are buffered until commit. Conflicts are detected at commit time.

`OtpTable` has no transactions: `insert` on one table and `insert` on another are not atomic. This is the gap.

### Software transactional memory on `OtpTable`

```kotlin
/**
 * A transaction context over one or more [OtpTable]s.
 *
 * Reads see a snapshot taken at transaction start.
 * Writes are buffered locally. Commit applies all writes atomically
 * under the tables' write locks; aborts on conflict (a key changed since read).
 *
 * This is a simplified MVCC: version numbers per row, not full MVCC with GC.
 * It teaches the Mnesia commit protocol without the distribution layer.
 *
 * OTP source: lib/mnesia/src/mnesia_tm.erl — transaction/1, commit protocol
 */
class MnesiaTransaction {
    private data class ReadRecord(val version: Long, val value: Any?)
    private data class WriteRecord(val value: Any?)

    private val reads  = mutableMapOf<Pair<OtpTable<*, *>, Any?>, ReadRecord>()
    private val writes = mutableMapOf<Pair<OtpTable<*, *>, Any?>, WriteRecord>()

    @Suppress("UNCHECKED_CAST")
    fun <K, V> read(table: OtpTable<K, V>, key: K): V? {
        val entry = reads[table to key]
        if (entry != null) return entry.value as V?
        val (value, version) = table.readWithVersion(key)
        reads[table to key] = ReadRecord(version, value)
        return value
    }

    fun <K, V> write(table: OtpTable<K, V>, key: K, value: V) {
        writes[table to key] = WriteRecord(value)
    }

    fun <K> delete(table: OtpTable<K, *>, key: K) {
        writes[table to key] = WriteRecord(null)   // tombstone
    }

    fun commit(): CommitResult {
        // Sort tables to acquire locks in consistent order (prevent deadlock)
        val tables = (reads.keys + writes.keys).map { it.first }.distinct()
            .sortedBy { System.identityHashCode(it) }
        return lockAll(tables) {
            // Validate: verify no read key changed since we read it
            for ((tableKey, record) in reads) {
                val (table, key) = tableKey
                @Suppress("UNCHECKED_CAST")
                val currentVersion = (table as OtpTable<Any?, Any?>).currentVersion(key)
                if (currentVersion != record.version) return@lockAll CommitResult.Conflict(tableKey)
            }
            // Apply writes
            for ((tableKey, record) in writes) {
                val (table, key) = tableKey
                @Suppress("UNCHECKED_CAST")
                val t = table as OtpTable<Any?, Any?>
                if (record.value != null) t.insertVersioned(key, record.value)
                else t.delete(key)
            }
            CommitResult.Ok
        }
    }
}

sealed class CommitResult {
    object Ok : CommitResult()
    data class Conflict(val key: Pair<OtpTable<*, *>, Any?>) : CommitResult()
}

/** Run [block] in a transaction; retries on conflict up to [maxRetries] times. */
suspend fun <T> transaction(maxRetries: Int = 5, block: MnesiaTransaction.() -> T): T {
    repeat(maxRetries) {
        val tx = MnesiaTransaction()
        val result = tx.block()
        when (tx.commit()) {
            CommitResult.Ok -> return result
            is CommitResult.Conflict -> { /* retry */ }
        }
    }
    throw TransactionAbortedException("transaction failed after $maxRetries retries")
}
```

### Versioned rows on `OtpTable`

`OtpTable` needs a small extension: each entry tracks a version counter (incremented on every write):

```kotlin
// OtpTable additions:
private val versions = ConcurrentHashMap<K, AtomicLong>()    // per-key version

fun readWithVersion(key: K): Pair<V?, Long> = lock.read {
    val v = lookup(key).firstOrNull()
    val ver = versions[key]?.get() ?: 0L
    v to ver
}

fun currentVersion(key: K): Long = versions[key]?.get() ?: 0L

fun insertVersioned(key: K, value: V) = lock.write {
    insert(key, value)
    versions.getOrPut(key) { AtomicLong(0L) }.incrementAndGet()
}
```

### Usage — atomic transfer

```kotlin
val accounts = OtpTable<String, Long>("accounts", TableType.Set)
accounts.insert("alice", 1000L)
accounts.insert("bob", 500L)

transaction {
    val alice = read(accounts, "alice") ?: 0L
    val bob   = read(accounts, "bob") ?: 0L
    write(accounts, "alice", alice - 200L)
    write(accounts, "bob",   bob   + 200L)
}
// Both writes applied atomically, or neither.
```

---

## 6. Full `logger` framework (OTP 21+) (`otp-logger` extension)

### The real gap

OTP 21 replaced `error_logger` with a proper structured logging pipeline:

```
Log event → Primary filter → [Handler 1 filter → Handler 1 formatter → Handler 1 backend]
                           → [Handler 2 filter → Handler 2 formatter → Handler 2 backend]
```

Our `OtpLogging` is a single-handler thin wrapper. The real `logger` has:
- **Primary filter:** applied before any handler sees the event; can drop, pass, or stop.
- **Per-handler filters:** each handler has its own filter list.
- **Formatters:** `logger_formatter` converts structured metadata to a string. Formatters are per-handler; you can emit JSON to one handler and plain text to another.
- **Handlers:** `logger_std_h` (stdout/stderr/file), `logger_disk_log_h` (rotating), custom.
- **Metadata:** each log event carries a `metadata` map. `logger:set_process_metadata/1` attaches per-process metadata automatically.

```kotlin
/**
 * Log event with structured metadata — analogous to OTP logger's log event map.
 *
 * OTP source: lib/kernel/src/logger.erl — log/2
 */
data class LogEvent(
    val level: OtpLogLevel,
    val message: String,
    val metadata: Map<String, Any?>,
    val timestamp: Instant = Instant.now(),
)

/** Primary filter — applied before any handler. */
fun interface LogFilter {
    /** Return [LogFilterResult.Pass], [Stop], or [Ignore] (treat as if no filter matched). */
    fun filter(event: LogEvent): LogFilterResult
}

sealed class LogFilterResult {
    object Pass : LogFilterResult()
    object Stop : LogFilterResult()          // discard; no handler sees it
    object Ignore : LogFilterResult()        // let next filter decide
}

fun interface LogFormatter {
    fun format(event: LogEvent): String
}

fun interface LogHandlerBackend {
    fun emit(formatted: String)
}

data class LogHandler(
    val id: String,
    val filters: List<LogFilter> = emptyList(),
    val formatter: LogFormatter = defaultFormatter,
    val backend: LogHandlerBackend,
)

object OtpLogger {
    private val primaryFilters = CopyOnWriteArrayList<LogFilter>()
    private val handlers = ConcurrentHashMap<String, LogHandler>()
    private val processMetadata = ThreadLocal<Map<String, Any?>>()

    fun addPrimaryFilter(filter: LogFilter) { primaryFilters.add(filter) }
    fun addHandler(handler: LogHandler) { handlers[handler.id] = handler }
    fun removeHandler(id: String) { handlers.remove(id) }
    fun setProcessMetadata(metadata: Map<String, Any?>) { processMetadata.set(metadata) }

    fun log(level: OtpLogLevel, message: String, metadata: Map<String, Any?> = emptyMap()) {
        val event = LogEvent(level, message, metadata + (processMetadata.get() ?: emptyMap()))
        for (filter in primaryFilters) {
            when (filter.filter(event)) {
                LogFilterResult.Stop   -> return
                LogFilterResult.Pass   -> break
                LogFilterResult.Ignore -> continue
            }
        }
        for (handler in handlers.values) {
            if (passesHandlerFilters(handler, event)) {
                handler.backend.emit(handler.formatter.format(event))
            }
        }
    }

    private fun passesHandlerFilters(handler: LogHandler, event: LogEvent): Boolean {
        for (filter in handler.filters) {
            when (filter.filter(event)) {
                LogFilterResult.Stop   -> return false
                LogFilterResult.Pass   -> return true
                LogFilterResult.Ignore -> continue
            }
        }
        return true
    }

    val defaultFormatter = LogFormatter { event ->
        "${event.timestamp} [${event.level.name.padEnd(5)}] ${event.message}" +
            if (event.metadata.isEmpty()) "" else " | ${event.metadata}"
    }
}
```

### Wiring into `OtpLogContext`

Add `OtpLogger.setProcessMetadata` calls in `GenServers.startLink` so every log event from inside an actor automatically carries its `id`, `name`, and `reductions`:

```kotlin
// Inside runLoop, before the message loop:
OtpLogger.setProcessMetadata(mapOf(
    "actor.id"   to id.toString(),
    "actor.name" to actorName,
))
```

This mirrors `logger:set_process_metadata/1` which attaches per-process metadata to all log events emitted by that process — without passing context manually.

---

## 7. `proc_lib` semantics — OTP-compatible process creation

### The real gap

`proc_lib:start_link` does three things that `spawn_link` alone does not:

1. **Initial call recording:** it stores `{Module, Function, Arity}` in the process dictionary so `process_info(Pid, initial_call)` returns it. Our `ProcessInfo.module` is a string class name — good, but not structured as `{M, F, A}`.

2. **Synchronous init acknowledgment:** `proc_lib:start_link` blocks the parent until the child calls `proc_lib:init_ack(Parent, {ok, Pid})` or `{error, Reason}`. If the child crashes before calling `init_ack`, the parent gets `{error, {EXIT, Reason}}`. Our `InitResult.Stop` propagates back, but not through the parent's start call — the parent just sees the job die.

3. **`format_status` and error info:** `proc_lib` wraps the process to catch exit reasons and format them for `error_logger` / `logger` in a standard way (`** Generic server Name terminating` etc.). Our `CrashReport` does this.

### Synchronous init acknowledgment

The real gap in our `startLink` is (2): if `init()` returns `InitResult.Stop`, the parent doesn't get an error from `startLink`. It just gets a `GenServerRef` whose job is already dead. OTP would throw in the parent.

```kotlin
// Current:
val ref = GenServers.startLink(parent, MyServer())
// ref.job may already be dead if init() returned Stop — caller can't tell

// OTP-compatible:
val ref = GenServers.startLinkSync(parent, MyServer())
// throws InitFailedException if init() returned Stop or threw
```

Implementation using a `CompletableDeferred` as the init ack:

```kotlin
fun <S> startLinkSync(
    parent: CoroutineScope,
    server: GenServer<S>,
    context: CoroutineContext = Dispatchers.Default,
    name: String? = null,
): GenServerRef<S> = runBlocking {
    val initAck = CompletableDeferred<InitResult<S>>()
    val ref = startLink(parent, ProcLibWrapper(server, initAck), context, name)
    when (val result = initAck.await()) {
        is InitResult.Ok -> ref
        is InitResult.Stop -> throw InitFailedException(result.reason)
    }
}

private class ProcLibWrapper<S>(
    private val inner: GenServer<S>,
    private val ack: CompletableDeferred<InitResult<S>>,
) : GenServer<S> by inner {
    override suspend fun init(): InitResult<S> {
        val result = inner.init()
        ack.complete(result)
        return result
    }
}

class InitFailedException(val reason: TerminateReason) :
    Exception("GenServer init failed: $reason")
```

### Initial call in `ProcessInfo`

```kotlin
data class InitialCall(
    val module: String,
    val function: String,   // "init" for GenServer
    val arity: Int,         // 0 for GenServer init()
)

// Add to ProcessInfo:
data class ProcessInfo(
    ...
    val initialCall: InitialCall,
)
```

Set in `startLink` using `server::class.qualifiedName` + "init" + 0 — stored in the probe lambda and returned by `ProcessTable.info`.

---

## 8. `sasl` alarm handler (`otp-sasl` module)

### The real gap

SASL's `alarm_handler` is an OTP `gen_event` manager for operational alarms:

```erlang
alarm_handler:set_alarm({disk_full, #{node => node(), path => "/var/log"}}),
alarm_handler:get_alarms(),
alarm_handler:clear_alarm(disk_full).
```

It is used by `mnesia`, `os_mon`, and application-specific code to signal persistent conditions (not transient errors). It pairs with SNMP traps in production systems.

The alarm handler **is** a `gen_event` manager. We already have `otp-gen-event`. So `AlarmHandler` is an application of `GenEventManager` with a built-in set/clear/list API.

### `otp-sasl` design

```kotlin
/**
 * Operational alarm registry — OTP's alarm_handler in Kotlin.
 *
 * An alarm is a persistent condition, not a one-shot event.
 * set_alarm sets it (idempotent); clear_alarm clears it.
 * Subscribers registered via [addHandler] receive AlarmSet / AlarmCleared events.
 *
 * OTP source: lib/sasl/src/alarm_handler.erl
 */
object AlarmHandler {
    private val alarms = ConcurrentHashMap<String, Any?>()   // alarmId → description
    private val manager = GenEventManager<AlarmEvent>()

    fun setAlarm(alarmId: String, description: Any? = null) {
        val isNew = alarms.putIfAbsent(alarmId, description) == null
        if (isNew) manager.notify(AlarmEvent.Set(alarmId, description))
    }

    fun clearAlarm(alarmId: String) {
        if (alarms.remove(alarmId) != null) {
            manager.notify(AlarmEvent.Cleared(alarmId))
        }
    }

    fun getAlarms(): Map<String, Any?> = alarms.toMap()

    fun addHandler(handler: GenEventHandler<AlarmEvent>) = manager.addHandler(handler)
    fun removeHandler(handler: GenEventHandler<AlarmEvent>) = manager.removeHandler(handler)
}

sealed class AlarmEvent {
    data class Set(val alarmId: String, val description: Any?) : AlarmEvent()
    data class Cleared(val alarmId: String) : AlarmEvent()
}
```

### Pattern: supervisor that sets an alarm on max restart intensity

```kotlin
class AlarmingSupervisor : SupervisorApplication() {
    override fun childSpecs() = listOf(workerSpec)

    override fun onRestartIntensityExceeded() {
        AlarmHandler.setAlarm("supervisor.overloaded",
            mapOf("supervisor" to name, "time" to Instant.now()))
    }
}
```

---

## 9. Behavioral equivalence via `otp-jinterface` — the proof

### The real gap

Everything built so far is validated by tests that test kotlin-otp against itself. That proves **internal consistency**, not **behavioral equivalence with Erlang**. The two are different:

- Internal consistency: a crash restarts the right children.
- Behavioral equivalence: given `{:$gen_call, {Pid, Ref}, :increment}` to both a real Erlang `gen_server` and a kotlin-otp `GenServer`, both return the same reply with the same state transition.

We now have `otp-jinterface` and Erlang is installed at `/opt/homebrew/bin/erl`. This makes behavioral equivalence tests possible.

### Test architecture

```
┌─────────────────────────────────────┐
│  Kotlin test coroutine               │
│                                      │
│  ref_erlang = ErlangRemoteRef(...)   │  ← real Erlang gen_server via jinterface
│  ref_kotlin = GenServers.startLink() │  ← kotlin-otp GenServer
│                                      │
│  Same messages → both refs           │
│  Assert: replies match               │
│  Assert: crash reasons match         │
│  Assert: supervisor restart counts match │
└─────────────────────────────────────┘
```

### Erlang reference implementation via `ErlangContainer`

Use Erlang's `-noshell` mode to start a gen_server in a subprocess:

```kotlin
/**
 * Launches a real Erlang gen_server in a subprocess for behavioral comparison.
 *
 * The Erlang node runs the provided .erl source (compiled in a temp dir).
 * Accessible via jinterface as a standard gen_server.
 *
 * Requires 'erl' on PATH and EPMD running.
 */
class ErlangNode(
    val nodeName: String = "testnode@localhost",
    val cookie: String = "kotlin_otp_test",
) : AutoCloseable {
    private lateinit var process: Process
    private lateinit var bridge: OtpErlangBridge

    fun start(erlSource: String, scope: CoroutineScope): OtpErlangBridge {
        val tmpDir = Files.createTempDirectory("otp_test")
        val srcFile = tmpDir.resolve("server.erl").also { it.writeText(erlSource) }
        // Compile
        Runtime.getRuntime().exec(arrayOf("erlc", "-o", tmpDir.toString(), srcFile.toString())).waitFor()
        // Start node
        process = ProcessBuilder(
            "erl", "-noshell", "-sname", nodeName.substringBefore("@"),
            "-setcookie", cookie,
            "-pa", tmpDir.toString(),
            "-eval", "server:start_link(), receive stop -> ok end.",
        ).start()
        Thread.sleep(500) // let EPMD register
        bridge = OtpErlangBridge("test_client@localhost", cookie, scope)
        return bridge
    }

    override fun close() {
        process.destroy()
        bridge.shutdown()
    }
}
```

### Behavioral equivalence test: counter server

```erlang
%% reference_counter.erl
-module(reference_counter).
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2]).

start_link() -> gen_server:start_link({local, counter}, ?MODULE, 0, []).
init(State) -> {ok, State}.
handle_call(get, _From, State) -> {reply, State, State};
handle_call({add, N}, _From, State) -> {reply, ok, State + N}.
handle_cast(reset, _State) -> {noreply, 0}.
```

```kotlin
class BehavioralEquivalenceTest {
    @Test
    fun `counter GenServer behaves identically to Erlang reference`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val erlNode = ErlangNode()
        val bridge = erlNode.start(REFERENCE_COUNTER_ERL, scope)

        // Kotlin actor
        val kotlinRef = GenServers.startLink(scope, KotlinCounterServer())
        // Erlang remote ref
        val erlangRef = bridge.remoteRef("counter", "testnode@localhost", CounterCodec)

        val scenarios = listOf(
            CounterOp.Add(5), CounterOp.Add(3), CounterOp.Get,
            CounterOp.Add(-2), CounterOp.Get,
        )

        for (op in scenarios) {
            val kotlinReply = kotlinRef.call<Any>(op)
            val erlangReply = erlangRef.call(op)
            assertEquals(erlangReply, kotlinReply,
                "behavioral divergence on op $op: Erlang=$erlangReply Kotlin=$kotlinReply")
        }

        erlNode.close()
        scope.cancel()
    }
}
```

### Property-based behavioral testing

With `kotest-property`, generate random message sequences and assert equivalence:

```kotlin
class GenServerBehaviorProperty : StringSpec({
    "kotlin-otp and Erlang gen_server produce identical replies for random message sequences" {
        checkAll(Arb.list(Arb.counterOp(), 1..50)) { ops ->
            // Run ops against both; compare reply sequences
            val kotlinReplies = runBlocking { ops.map { kotlinRef.call<Any>(it) } }
            val erlangReplies = runBlocking { ops.map { erlangRef.call(it) } }
            kotlinReplies shouldBe erlangReplies
        }
    }
})
```

**What this unlocks:** not just testing that kotlin-otp works, but that it is *correct* with respect to OTP. Any behavioral divergence (wrong crash reason, wrong restart count, wrong reply on error recovery) becomes a test failure.

---

## 10. `recon`-style diagnostics (`otp-recon` module)

### The real gap

Fred Hébert's `recon` library is the standard production introspection tool for Erlang systems. Its core API:

```erlang
recon:proc_count(memory, 5)           %% top 5 processes by memory
recon:proc_window(reductions, 5, 1000)%% highest reduction rate over 1s
recon:node_stats(1, 5)                %% scheduler/IO stats, sampled 5 times
```

Built on `erlang:processes()` + `erlang:process_info/2` — both of which we have analogues for (`ProcessTable.all()` + `ProcessTable.info(id)`).

### `otp-recon` design

```kotlin
/**
 * Production introspection for kotlin-otp — Erlang's recon library in Kotlin.
 *
 * Built on ProcessTable and OtpObserver. All queries are non-blocking,
 * read-only, and safe to run in production.
 *
 * OTP source: https://github.com/ferd/recon — recon.erl
 */
object Recon {
    /**
     * Return the top [n] processes by [attribute], sorted descending.
     * Analogous to recon:proc_count/2.
     */
    fun procCount(attribute: ProcessAttribute, n: Int): List<ProcessInfoEntry> =
        ProcessTable.all()
            .sortedByDescending { it.attributeValue(attribute) }
            .take(n)
            .map { ProcessInfoEntry(it, it.attributeValue(attribute)) }

    /**
     * Sample [attribute] twice over [windowMs] ms; return top [n] by delta.
     * Analogous to recon:proc_window/3.
     */
    suspend fun procWindow(attribute: ProcessAttribute, n: Int, windowMs: Long): List<ProcessWindowEntry> {
        val before = ProcessTable.all().associateBy { it.id }
        delay(windowMs)
        val after = ProcessTable.all().associateBy { it.id }
        return after.values
            .mapNotNull { p ->
                val b = before[p.id] ?: return@mapNotNull null
                val delta = p.attributeValue(attribute) - b.attributeValue(attribute)
                ProcessWindowEntry(p, delta)
            }
            .sortedByDescending { it.delta }
            .take(n)
    }

    enum class ProcessAttribute { Reductions, MessageQueueLen, Memory }

    data class ProcessInfoEntry(val info: ProcessInfo, val value: Long)
    data class ProcessWindowEntry(val info: ProcessInfo, val delta: Long)

    private fun ProcessInfo.attributeValue(attr: ProcessAttribute): Long = when (attr) {
        ProcessAttribute.Reductions     -> reductions
        ProcessAttribute.MessageQueueLen -> messageQueueLen.toLong()
        ProcessAttribute.Memory         -> memoryBytes
    }
}
```

### Usage in a health-check actor

```kotlin
class SystemHealthServer : GenServer<Unit> {
    override suspend fun init() = InitResult.Ok(Unit)
    override suspend fun handleCall(request: Any, state: Unit): ReplyResult<Unit> {
        val topByReductions = Recon.procCount(Recon.ProcessAttribute.Reductions, 10)
        val busy = topByReductions.filter { it.value > 100_000 }
        return ReplyResult.Reply(SystemHealth(busyActors = busy.map { it.info.name }), Unit)
    }
    override suspend fun handleCast(request: Any, state: Unit) = NoreplyResult.Noreply(Unit)
}
```

---

## Suggested order

```
§1  (ActorArena)    → Panama + Java 21 required. Do this first if actors do I/O.
§3  (pg)            → Self-contained, immediately useful for any fan-out pattern.
§9  (equivalence)   → Requires Erlang node. Most valuable for proving correctness.
§5  (Mnesia/STM)    → Requires OtpTable versioning. One sprint of work.
§6  (logger)        → Drop-in replacement for OtpLogging. Backwards-compatible.
§4  (global)        → Builds on otp-distribution. Good exercise in distributed naming.
§7  (proc_lib)      → Adds startLinkSync and InitialCall to ProcessInfo.
§8  (sasl)          → One file + gen_event. Very fast to implement.
§10 (recon)         → One file on top of ProcessTable. Zero new dependencies.
§2  (compiler plugin) → Largest scope. Leave last; requires plugin build infrastructure.
```

---

## What the library becomes at the end of this roadmap

At the completion of BEYOND_THE_DEEP_END, kotlin-otp is no longer modelling OTP. It **is** OTP's application layer in Kotlin:

- **Pub/sub via `pg`** — fan-out broadcast identical to Phoenix LiveView's PubSub.
- **Cluster-wide naming via `global`** — the same conflict resolution semantics.
- **Multi-table atomic transactions via `otp-mnesia`** — STM with OTP commit semantics.
- **Structured logging via `otp-logger`** — handler/filter/formatter pipeline.
- **Synchronous startup via `proc_lib` semantics** — init failures propagate to the parent.
- **Operational alarms via `sasl`** — set/clear/query persistent conditions.
- **Behaviorally verified** — property tests against real Erlang nodes confirm equivalence.
- **Production-observable via `recon`** — top-N by reductions/memory/queue length.
- **Off-heap isolated** — arena-allocated state freed deterministically on actor death.
- **Automatically preempted** — compiler plugin injects reductions without source annotation.

The semantic gap remaining at that point is narrow and fundamental: **shared JVM GC** and **no BEAM-level preemption for bytecode not compiled by the plugin**. Everything else is library semantics rather than JVM limitations.

Update [TRACEABILITY.md](../../TRACEABILITY.md) and [LIMITATIONS.md](../../LIMITATIONS.md) as each section lands.
