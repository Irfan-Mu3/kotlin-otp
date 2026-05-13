# Deeper still: the next layer

> **Status: COMPLETE (2026-05-02).** All 13 sections are implemented. See [TRACEABILITY.md](../../TRACEABILITY.md) for the full mapping. The next roadmap is [THE_DEEP_END.md](THE_DEEP_END.md).

This document is the **third roadmap**, following [DEEPER.md](DEEPER.md) and [EVEN_DEEPER.md](EVEN_DEEPER.md).

**Baseline:** everything in [TRACEABILITY.md](../../TRACEABILITY.md) is in place—all three supervisor strategies with supervisor-wide intensity, `gen_statem` with OTP-style timeouts and callback modes, `gen_event`, phased application startup, `ApplicationEnv`, structured `OtpLogContext`, `OtpTracer` no-op interface, `RegistryLifecycle`, and stress/race tests. The library is feature-complete for the "first actors" case.

The gap that remains is not more API surface—it is **depth inside** the existing behaviours and three OTP concepts that have no module yet: bounded mailboxes, the selective-receive model, and distribution. The sections below are ordered by teaching value and implementation cost.

---

## 1. Bounded mailboxes and back-pressure

### Why it matters

All current mailboxes are `Channel.UNLIMITED`. Production OTP systems crash a process or apply back-pressure when it falls behind. Erlang's default mailbox is also unbounded, but good OTP code uses `gen_server` calls (which inherently block the caller) or explicit bounded queues for flow control. Teaching unbounded mailboxes forever hides a whole class of operational problems.

### Design sketch

Add `MailboxBound` and `OverflowPolicy` to `ProcessMailbox` and wire them into `GenServers.startLink` and `GenStateMs.startLink`:

```kotlin
sealed class OverflowPolicy {
    data object Block : OverflowPolicy()          // caller suspends (default for calls)
    data object DropOldest : OverflowPolicy()      // discard oldest pending message
    data object DropNew : OverflowPolicy()         // discard the arriving message
    data object CrashSender : OverflowPolicy()     // throw from the send site
    data class  DeadLetterTo(                      // route overflow to a sink ref
        val sink: GenServerRef<*>,
    ) : OverflowPolicy()
}

data class MailboxBound(
    val capacity: Int,
    val policy: OverflowPolicy = OverflowPolicy.Block,
)

// In GenServers:
fun <S> startLink(
    parent: CoroutineScope,
    server: GenServer<S>,
    context: CoroutineContext = Dispatchers.Default,
    name: String? = null,
    mailboxBound: MailboxBound? = null,   // null = unlimited (current default)
): GenServerRef<S>
```

**`DeadLetterTo`** is useful for observability: a supervisor-owned dead-letter server can count and log dropped messages, giving a signal that the worker is overloaded before the system degrades.

### Tests

- Slow consumer + fast producer → verifies `DropOldest` / `Block` behave correctly.
- Bounded-`call` path: when the mailbox is full, the caller must respect the policy (suspend on `Block`, see an error on `CrashSender`).
- Dead-letter integration: verify overflow messages land in the dead-letter server's mailbox.

### JVM / OTP gap

OTP mailboxes are not bounded natively; `gen_server` back-pressure comes from synchronous `call` semantics. The `Block` policy is the closest analog to OTP's implicit call-based back-pressure, but it adds contention not present in BEAM's per-process GC model. Document the throughput trade-off.

---

## 2. Async reply (`from` / `reply` pattern)

### Why it matters

OTP's `handle_call` can return `{noreply, State}` and defer the reply until later with `gen_server:reply(From, Reply)`. This is fundamental for:

- Long-running requests that should not block the mailbox loop.
- Batching replies after accumulating context.
- Delegating work to a spawned child, replying only on completion.

The current `ReplyResult.Reply` always replies inline. The `Noreply` path today discards the caller's deferred. This should be a first-class pattern.

### Design sketch

```kotlin
// Opaque handle handed to the caller via handleCall's return value
class ReplyHandle<S>(
    private val pending: CompletableDeferred<Any?>,
    val state: S,
)

sealed class ReplyResult<out S> {
    data class Reply<S>(val response: Any?, val newState: S) : ReplyResult<S>()
    data class Noreply<S>(val newState: S) : ReplyResult<S>()  // mailbox continues
    data class DeferReply<S>(                                   // new: keep the from
        val handle: ReplyHandle<S>,
    ) : ReplyResult<S>()
    data class Stop<S>(...)
}

// Extension on the handle (callable from any coroutine, not just the server loop):
suspend fun <S> ReplyHandle<S>.reply(response: Any?)
```

`DeferReply` stores the `CompletableDeferred` from the original `Call` message in a `ReplyHandle` that is given back to the server. The server may pass it to a child coroutine. Completing the `CompletableDeferred` from any context unblocks the original caller.

**Timeout contract:** if the original `call` times out (the `withTimeout` in `GenServerRef.call` fires), the deferred is already abandoned. The server's eventual `handle.reply(...)` writes to a completed `CompletableDeferred` which is a no-op in Kotlin. Document that: the reply is silently dropped, unlike OTP where `gen_server:reply` to a timed-out caller crashes nothing (the reply message arrives in the caller's mailbox as a `late_reply` stray).

---

## 3. `sys` module: introspection and debugging

### Why it matters

OTP's `sys` module is one of the best-designed parts of OTP: it lets you inspect, suspend, resume, and replace the state of any `gen_server`, `gen_statem`, or `gen_event` without touching their public API. It is also the foundation of `dbg` and `observer`. Teaching OTP without `sys` leaves out half the operational story.

The interface from OTP `sys.erl`:

```erlang
sys:get_state(Name)      % → current state
sys:get_status(Name)     % → full diagnostic {status, Pid, {module, M}, [D]}
sys:replace_state(Name, Fun)  % → transform and hot-swap the state
sys:suspend(Name)        % → pause mailbox processing
sys:resume(Name)         % → resume
sys:statistics(Name, Flag)    % → enable/disable per-process timing stats
```

### Design sketch

Add a **system channel** (second `Channel<SysMsg>`) to every `GenServerRef`, distinct from the user mailbox and the control channel:

```kotlin
sealed class SysMsg {
    data class GetState(val reply: CompletableDeferred<Any?>) : SysMsg()
    data class GetStatus(val reply: CompletableDeferred<SysStatus>) : SysMsg()
    data class ReplaceState(
        val transform: (Any?) -> Any?,
        val reply: CompletableDeferred<Any?>,
    ) : SysMsg()
    data class Suspend(val reply: CompletableDeferred<Unit>) : SysMsg()
    data class Resume(val reply: CompletableDeferred<Unit>) : SysMsg()
}

data class SysStatus(
    val id: OtpProcessId,
    val name: String?,
    val module: String,
    val state: Any?,
    val messageQueueLength: Int,
)
```

The run loop drains the sys channel **at the very top of each iteration** (before the control channel drain), implementing OTP's system message priority. `Suspend` sets a flag that makes the loop block on a `Mutex` or `CompletableDeferred<Unit>` instead of reading user messages; `Resume` releases it.

**Why a separate channel and not a control message?** Because `sys` semantics say system messages must be handled even when the server's `handle_call` / `handle_cast` is stuck or slow. Putting them on a third channel, drained first, approximates OTP's out-of-band delivery.

### `GenStateMRef.sys` parity

`gen_statem` responds to `sys:get_state` with `{CurrentState, Data}`. Mirror this by adding the same sys channel to `GenStateMRef`, returning a `StateMSysStatus(state: S, data: D, ...)`.

### Tests

- `get_state` while the server is processing a slow `handleCall`.
- `replace_state` swaps out the state, subsequent calls observe the new one.
- `suspend` / `resume`: cast messages queue up during suspension, drain after resume.
- `get_status.messageQueueLength` reflects mailbox depth before the server drains it.

---

## 4. Standalone timer module (`OtpTimers`)

### Why it matters

OTP has two timer mechanisms that live outside `gen_statem`: `erlang:send_after/3` and the `timer` module. Every OTP tutorial uses one of them. Today, timers in this library exist only inside `gen_statem`. Any `gen_server` that needs a periodic tick, a deadline, or a retry delay has no idiomatic mechanism beyond `launch { delay(...); ref.cast(...) }` in the application code—which leaks the job if the server stops.

### Design sketch

```kotlin
/** Opaque timer reference, cancellable. */
class TimerRef internal constructor(private val job: Job) {
    fun cancel() { job.cancel() }
    val isActive: Boolean get() = job.isActive
}

object OtpTimers {
    /**
     * After [delay], sends [msg] to [target] as an [InfoMsg.TimerTick].
     * The timer is automatically cancelled if [scope]'s Job is cancelled.
     */
    fun sendAfter(
        scope: CoroutineScope,
        delay: Duration,
        target: GenServerRef<*>,
        msg: InfoMsg = TimerTick(ref = Unit),
    ): TimerRef

    /**
     * Sends [msg] to [target] every [interval]. First fire after [initialDelay]
     * (defaults to [interval]).
     */
    fun sendInterval(
        scope: CoroutineScope,
        interval: Duration,
        target: GenServerRef<*>,
        msg: InfoMsg = TimerTick(ref = Unit),
        initialDelay: Duration = interval,
    ): TimerRef
}
```

The returned `TimerRef` wraps the launched `Job`. Cancellation is cooperative via `job.cancel()`.

**Supervision link:** the timer is launched under [scope], so if the owning server's scope is cancelled (supervisor terminates the child), all pending timers are automatically cancelled—mirroring OTP's "all timers owned by a process are cancelled on process death."

**Uniquely named ticks:** `TimerTick.ref` is an `Any` correlation token so the server can distinguish concurrent timers (`ref = "retry-timer"` vs `ref = "heartbeat"`).

### Tests

- `sendAfter` fires exactly once; `cancel` before fire prevents delivery.
- `sendInterval` fires multiple times; after `cancel`, no more deliveries.
- Server termination cancels all pending `sendAfter` timers (scope cancellation).
- Virtual-time test with `kotlinx-coroutines-test` for deterministic delivery order.

---

## 5. `gen_statem` postpone and state-enter callbacks

### Why it matters

These are the two `gen_statem` features with the highest OTP-application-code frequency that the library does not yet implement. Without them, a direct port of any real OTP state machine falls back to awkward workarounds.

### Postpone

In OTP, `{postpone, true}` re-queues the current event at the **front** of the next state's event queue. Classic use case: an FSM in `connecting` state receives a `send(data)` event too early—postpone lets it come back when the state becomes `connected`.

Implementation:

```kotlin
sealed class StateMTransition<out S : Any, out D, out E> {
    data class Stay<...>(
        val newData: D,
        val timeout: StateMTimeoutSpec? = null,
        val postpone: Boolean = false,   // re-queue current event
    ) : StateMTransition<...>()

    data class Next<...>(
        val newState: S,
        val newData: D,
        val timeout: StateMTimeoutSpec? = null,
        val postpone: Boolean = false,
    ) : StateMTransition<...>()
    ...
}
```

In the run loop, when `postpone = true`, the current `GenStateMMsg.Event` is stored in a `postponedEvents: ArrayDeque<GenStateMMsg.Event<E>>`. On the **next state change** (a `Next` transition with a different state), the postponed events are prepended back to the channel, in order, before new events are processed.

**JVM implementation note:** channels are not reentrant, so "prepend to channel" cannot be done directly. The correct approach is to maintain an in-memory `postponedQueue: ArrayDeque<GenStateMMsg.Event<E>>` and drain it at the top of each iteration *before* `mailbox.receive()`. When the state changes, postponed events become available again for draining.

**JVM / OTP gap:** OTP's postpone mechanism is semantically a per-queue prepend managed by the BEAM; our ArrayDeque approach is equivalent in single-event-per-iteration semantics but is not reentrant across concurrent coroutines. Document that postponed events are replayed only on the *next state change*, not mid-iteration.

### State-enter callbacks

OTP's `{call, enter}` callback mode fires a special `handle_event(enter, OldState, State, Data)` call whenever the state machine enters a new state (including the initial state from `init`). Use cases: setup, logging, arming timeouts that apply to an entire state.

```kotlin
interface GenStateM<S : Any, D, E> {
    // existing
    suspend fun init(): InitStateMResult<S, D>
    suspend fun handleEvent(state: S, data: D, event: E): StateMTransition<S, D, E>
    suspend fun handleTimeout(state: S, data: D, kind: StateMTimeoutKind): StateMTransition<S, D, E>

    // new: called when entering [newState]; [prevState] is null on init
    suspend fun onEnterState(
        newState: S,
        prevState: S?,
        data: D,
    ): StateMTransition<S, D, E> = StateMTransition.Stay(data)
}
```

The run loop calls `onEnterState` after every `Next` transition (and once after `init` with `prevState = null`). The return value of `onEnterState` is applied like any other transition—allowing the state-enter callback to arm a timeout, produce a reply, or even trigger another transition.

**Risk:** infinite loops via state-enter → immediate `Next` → state-enter. Mirror OTP's protection: detect same-state transitions from `onEnterState` and treat them as `Stay`.

---

## 6. Crash reporters (SASL-style)

### Why it matters

OTP's SASL application formats crash reports that are invaluable in production: process name, registered name, supervisor path, crash reason, last state, recent messages, stacktrace. Without this, operator experience degrades to raw exception logs. This is not a novel feature—it is operational necessity disguised as infrastructure.

### Design sketch

Add a `CrashReport` data class and a `CrashReporter` hook in `OtpLogging`:

```kotlin
data class CrashReport(
    val id: OtpProcessId,
    val name: String?,
    val supervisorId: String?,
    val reason: TerminateReason,
    val lastState: Any?,            // for gen_server / gen_statem
    val messageQueueLength: Int,
    val stackTrace: List<StackTraceElement>,
    val linkedProcesses: List<OtpProcessId>,
    val timestamp: Instant,
)

fun interface CrashReporter {
    fun report(crash: CrashReport)
}

// In OtpLogging:
fun setCrashReporter(reporter: CrashReporter)
fun reportCrash(crash: CrashReport)
```

Wire the crash reporter into:

- `GenServer` run loop `catch` path (Failure reason → emit `CrashReport` with last state captured before `terminate`).
- `Supervisor` coordinator on `ExitKind.Failure` (child id, restart count, and cause from the slot).
- `GenStateM` run loop (state + data at time of crash).

**SASL report format (text sink example):**

```
=CRASH REPORT====
   crasher:
     initial call: org.otpstudy.demo.CounterServer.init/0
     pid:          otp-pid:42
     registered:   counter
     exception:    java.lang.ArithmeticException: / by zero
     at:           org.otpstudy.demo.CounterServer.handleCall(CounterServer.kt:31)
     state:        CounterState(value=0)
     message_queue: 3
   neighbours: []
```

The text format is an `OtpLogger` implementation, not the core—keep `CrashReport` a data class so each team can format it for their own sink (JSON, structured logging, OTel event).

---

## 7. Selective receive (save queue)

### Why it matters

This is the single most distinctive Erlang / OTP primitive not yet addressed. It is the foundation of `gen_server`'s internal `call` / `cast` / `info` discrimination, `receive ... after` timeouts, and many protocol patterns (handshake, partial acknowledgement). Not having it is the biggest semantic gap between this library and real OTP.

The honest constraint is **Kotlin channels are FIFO with no scan**. Full selective receive requires either:

1. A copy of every message that the process has "seen but not yet matched" (the BEAM's per-process queue with a `save` pointer), or
2. A message type that routes itself to the right handler (what `GenServerMsg` already does implicitly).

The goal is not to solve the general case—it is to teach the pattern and make structured selective receive available for the cases where it matters.

### Proposal: `SaveQueue` and `ReceivePattern`

```kotlin
/**
 * A FIFO channel with a "save" list for deferred messages.
 *
 * Messages that do not match the current pattern are held in [saved] and
 * re-injected at the front of the channel on the next [receive] call.
 *
 * JVM / OTP difference: the BEAM keeps one per-process queue with a scan
 * pointer; this implementation materialises the "saved" list explicitly and
 * replays it. The O(n) replay cost is visible; do not use for hot paths with
 * large message volumes.
 */
class SelectiveMailbox<T>(private val channel: Channel<T>) {
    private val saved = ArrayDeque<T>()

    suspend fun receive(matches: (T) -> Boolean): T {
        // Drain saved list first (respecting FIFO order within saved)
        val iter = saved.iterator()
        while (iter.hasNext()) {
            val msg = iter.next()
            if (matches(msg)) {
                iter.remove()
                return msg
            }
        }
        // Read from channel, saving non-matching messages
        while (true) {
            val msg = channel.receive()
            if (matches(msg)) return msg
            saved.addLast(msg)
        }
    }

    /** Flush all saved messages back to the channel (e.g., on state change). */
    suspend fun flushSaved() {
        for (msg in saved.toList()) {
            channel.send(msg)
        }
        saved.clear()
    }
}
```

Use cases to document and test:

- **Handshake pattern:** wait for a specific `{correlationId, Reply}` message while caching unrelated messages that arrived concurrently.
- **Priority drain:** process all `Priority.High` messages before any `Priority.Low`, even if low messages arrived first.
- **`receive ... after` analog:** combine `SelectiveMailbox.receive` with a `withTimeoutOrNull` block.

**OTP source reference:** `erts/emulator/beam/erl_message.c` (`erts_msgq_peek_msg`, `erts_msgq_unlink_msg`) for how the BEAM scan pointer works; contrast with the explicit-copy approach here.

---

## 8. Links with `trap_exit`

### Why it matters

The current `linkJobs` cascade cancels B when A is cancelled. Real OTP links are **bidirectional** and carry an exit **reason**; with `process_flag(trap_exit, true)`, an actor can receive the exit signal as a message and decide what to do—restart, clean up, or re-raise. This is how supervisors intercept their children's exits without dying themselves.

### Design sketch

```kotlin
sealed class ExitSignal {
    data class Exit(val from: OtpProcessId, val reason: TerminateReason) : ExitSignal()
}

/** A bidirectional link between two actors. */
class OtpLink(
    val a: OtpProcessId,
    val b: OtpProcessId,
) {
    fun unlink() { ... }
}

// process_flag analog: per-actor flag, stored in GenServer state or a side channel
interface TrapExitHandler {
    suspend fun handleExit(signal: ExitSignal): NoreplyResult<*>
}
```

When two actors are linked via `OtpLink.link(refA, refB)`:

- If A's job completes with a non-normal reason and B does **not** have `trap_exit`:  B's job is cancelled with the same cause (cascading exit, same as `linkJobs` today).
- If B **does** have `trap_exit` (implements `TrapExitHandler`): the exit is delivered as `ExitSignal.Exit` to B's info mailbox instead of cancelling B's job.

The `trap_exit` flag must be modelled per-GenServer, not globally. One clean implementation: add an optional `TrapExitHandler` parameter to `GenServers.startLink`; if present, the run loop delivers exit signals as `GenServerMsg.Info(Exit(...))` rather than propagating cancellation.

**OTP source:** `lib/kernel/src/proc_lib.erl` `translate_initial_call` and `erts/emulator/beam/erl_process.c` `erts_send_exit_signal` for the signal routing logic.

---

## 9. `supervisor_bridge`

### Why it matters

Real systems have components that are not `gen_server`s: legacy thread pools, reactive streams, third-party library event loops, or plain coroutine jobs. OTP's `supervisor_bridge` wraps a non-standard process under a supervisor, appearing as a standard child. Without it, every non-GenServer actor either bypasses the supervision tree or needs an artificial GenServer shell.

### Design sketch

```kotlin
/**
 * Wraps an arbitrary suspend block as a supervised child.
 * The block must terminate (normally or with an exception) for the supervisor
 * to make a restart decision; it must honour cancellation to support [Shutdown].
 *
 * Analogous to OTP [`supervisor_bridge`](https://www.erlang.org/doc/man/supervisor_bridge.html).
 */
object SupervisorBridge {
    fun childSpec(
        id: String,
        restart: Restart = Restart.Permanent,
        shutdown: Shutdown = Shutdown.Timeout(5.seconds),
        block: suspend CoroutineScope.() -> Unit,
    ): ChildSpec = ChildSpec(id, restart, shutdown, block)
}
```

This is mostly a documentation / naming win—`SupervisorBridge.childSpec` is a thin wrapper over `ChildSpec` that makes the intent explicit. The real value is a **companion pattern**: a bridge that gives the wrapped task an exit channel back into the supervisor coordinator, so it can signal the supervisor with a structured reason rather than just throwing.

```kotlin
class BridgeRef internal constructor(
    private val exitChannel: Channel<ExitSignal>,
) {
    /** Signal the bridge (and thus the supervisor) with a reason. */
    fun exit(reason: TerminateReason) {
        exitChannel.trySend(ExitSignal.Exit(OtpProcessId.allocate(), reason))
    }
}
```

The bridge-flavoured `startLink` passes a `BridgeRef` to the user block so it can call `exit(Normal)` on clean shutdown, matching OTP's `proc_lib:init_ack` / `exit(normal)` bridge protocol.

---

## 10. Distribution concepts (without wire protocol)

### Why it matters

OTP's distributed nature—`node()`, `Node@Host`, `{Name, Node}` registered process addresses, `rpc:call`, and distributed supervision trees—is central to why Erlang is used in the first place. A library that stops at single-node actors misses the reason OTP supervision trees are useful at scale: processes may live on different nodes, supervisors span machine boundaries, and `net_kernel` keeps the mesh alive.

The Erlang wire protocol (`dist_util.erl`) is immense. The goal is not wire compatibility—it is to make the **concepts** tangible and testable.

### `OtpNode` abstraction

```kotlin
/**
 * Represents an actor runtime — local or remote. Analogous to an Erlang node.
 * All [GenServerRef]s carry a [NodeId] so the address space is explicit.
 */
data class NodeId(val name: String, val host: String = "local") {
    override fun toString() = "$name@$host"
}

interface OtpNode {
    val id: NodeId

    /** Resolve a registered name to a ref, or null if unknown / unreachable. */
    fun <S> whereis(name: String): GenServerRef<S>?

    /** Send a cast to a named process on this node. No delivery guarantee on remote nodes. */
    fun cast(name: String, message: Any)

    /** RPC-style call to a named process. Timeout applies to the round trip. */
    suspend fun <R> call(name: String, request: Any, timeout: Duration = 5.seconds): R
}

class LocalNode(override val id: NodeId) : OtpNode { ... }

class RemoteNodeStub(
    override val id: NodeId,
    private val transport: NodeTransport,   // gRPC, in-memory pipe, etc.
) : OtpNode { ... }
```

**`NodeTransport` interface** decouples the node abstraction from the transport. For tests, use an `InMemoryTransport` that routes between two `LocalNode`s in the same JVM. For real deployments, implement with gRPC or Aeron.

### Distributed supervision

The real payoff is a `DistributedSupervisor` that holds children on different nodes:

```kotlin
data class DistributedChildSpec(
    val spec: ChildSpec,
    val preferredNode: NodeId,
    val fallbackNodes: List<NodeId> = emptyList(),
)
```

When a child's `preferredNode` becomes unreachable, the supervisor attempts to start the child on the next fallback. This is the Kotlin analogue of OTP's global process groups and `pg` / distributed supervisor patterns.

### Tests to write

- Two `LocalNode`s in the same test process communicate via `InMemoryTransport`.
- A child started on node A can be called from a client connected to node B.
- When node A is "partitioned" (transport returns error), the distributed supervisor migrates the child to node B (fallback).

---

## 11. ETS-style in-process tables (`OtpTable`)

### Why it matters

ETS is the backbone of production Erlang systems: fast concurrent lookup, shared across processes without copying, owner-linked (table dies with its owner). The JVM equivalent is `ConcurrentHashMap` with a wrapper, but without ownership semantics and OTP's named table registry. Building `OtpTable` teaches why Erlang's process/data model differs from shared-memory OO languages.

### Design sketch

```kotlin
enum class TableType { Set, OrderedSet, Bag, DuplicateBag }
enum class TableAccess { Public, Protected, Private }

class OtpTable<K, V>(
    val name: String,
    val type: TableType = TableType.Set,
    val access: TableAccess = TableAccess.Protected,
) {
    // Core ETS API surface
    fun insert(key: K, value: V)
    fun lookup(key: K): List<V>           // list because Bag allows duplicates
    fun delete(key: K)
    fun match(pattern: (K, V) -> Boolean): List<Pair<K, V>>
    fun foldl(acc: A, f: (K, V, A) -> A): A
    fun toList(): List<Pair<K, V>>
}

object OtpTableRegistry {
    fun <K, V> new(name: String, owner: GenServerRef<*>, ...): OtpTable<K, V>
    fun lookup(name: String): OtpTable<*, *>?
    // Auto-destroy when owner's job completes
}
```

`OrderedSet` uses `ConcurrentSkipListMap`; `Set` uses `ConcurrentHashMap`; `Bag` uses `ConcurrentHashMap<K, CopyOnWriteArrayList<V>>`.

**Ownership:** register an `invokeOnCompletion` on the owner `GenServerRef.job` that removes the table from `OtpTableRegistry` and clears it. This matches OTP's "ETS table is deleted when the owning process exits."

**`Protected` access:** only the owner can write; any process can read. In Kotlin this is a `ReadWriteLock` per table—readers never block each other; only the owner thread can write. This is weaker than BEAM ETS (no per-row locking) but teaches the concept.

---

## 12. `persistent_term` analog (`OtpPersistentTerms`)

### Why it matters

OTP 21 added `persistent_term` for configuration data that is read far more often than it is written—system-wide flags, compiled regex patterns, certificates. Unlike ETS, writes are expensive (global GC pause) but reads are truly zero-cost (term is embedded in the process's heap on first read). This forces a different design: write once at startup, read millions of times.

### Design sketch

```kotlin
object OtpPersistentTerms {
    // Write: expensive — notifies all registered watchers
    fun <V> put(key: String, value: V)
    fun delete(key: String)

    // Read: O(1), no lock (AtomicReference snapshot)
    fun <V> get(key: String): V?
    fun <V> require(key: String): V

    // Watcher for the rare case of updates (analogous to 'persistent_term' events)
    fun watch(key: String, callback: (String, Any?) -> Unit): AutoCloseable
}
```

Implementation: a single `ConcurrentHashMap` backed by an `AtomicReference<Map<String, Any?>>` snapshot. Writes take the write lock, update the map, publish a new snapshot, and notify watchers. Reads are a single `AtomicReference.get()` + map lookup—no lock, very fast.

The teaching value is the design constraint: make `put` visibly expensive (add a simulated "global GC pause" log warning for writes after the first) so students understand why you should not use `persistent_term` for frequently-written data.

---

## 13. Operational tooling: `observer` surface

### Why it matters

OTP's `observer` GUI is what separates a system you can run from one you can operate. Its data comes from existing `sys` APIs, ETS, `erlang:process_info/2`, and the statistics in supervisors. Once `sys` introspection (§3), `OtpTable` (§11), and structured logging (already done) are in place, an `observer`-style data layer becomes feasible.

### Design sketch: `OtpObserver` data layer

Not a GUI—a machine-readable API that a UI or metric sink can consume:

```kotlin
data class ProcessSnapshot(
    val id: OtpProcessId,
    val name: String?,
    val supervisorPath: List<String>,
    val state: Any?,
    val messageQueueLength: Int,
    val restartCount: Int,
    val uptimeMs: Long,
)

data class SupervisorSnapshot(
    val id: String,
    val strategy: SupervisorStrategy,
    val children: List<ProcessSnapshot>,
    val restartsSinceStart: Int,
)

object OtpObserver {
    fun listProcesses(): List<ProcessSnapshot>
    fun inspectProcess(id: OtpProcessId): ProcessSnapshot?
    fun supervisorTree(): SupervisorSnapshot?   // root supervisor only
    fun tableStats(): List<TableStats>          // from OtpTableRegistry
}
```

This is read-only; `sys.replace_state` (§3) handles mutation. Expose `OtpObserver` as an HTTP endpoint (Ktor / embedded Javalin) for a minimal browser-based view.

---

## Suggested order of attack

The sections above are deliberately more independent than previous roadmaps. Pick by which gap is most painful in your application code:

| Priority | Why first |
|----------|-----------|
| **§2 — Async reply** | Unblocks any gen_server doing I/O inside `handleCall`. Low risk, high payoff. |
| **§4 — OtpTimers** | Needed by nearly every real server. Clean module, testable with virtual time. |
| **§1 — Bounded mailboxes** | Reveals back-pressure story; needed before stress-testing at scale. |
| **§5 — Postpone + state-enter** | Completes gen_statem to OTP parity. Self-contained module change. |
| **§3 — sys introspection** | Enables all operational tooling. Medium complexity. |
| **§6 — Crash reporter** | Operational necessity; wire into existing logging hooks. |
| **§8 — Links + trap_exit** | Foundation for §10 (distribution); do before node work. |
| **§9 — supervisor_bridge** | Small naming win now; bigger if wrapping reactive or thread-pool code. |
| **§7 — Selective receive** | High teaching value; do after bounded mailboxes to show the contrast. |
| **§11 — OtpTable** | Needed for stateful fast-path reads; large surface area, worth a dedicated sprint. |
| **§12 — persistent_term** | Quick win after OtpTable; reinforces the write-once read-many design constraint. |
| **§10 — Distribution** | Highest complexity; wait until §8 (links) and §3 (sys) are stable. |
| **§13 — Observer surface** | Synthesis step: all previous pieces feed it. Last milestone before the library is "production-shaped". |

---

## What still requires leaving Kotlin

These items remain out of scope for a pure-library approach and are listed here to set honest expectations, not to discourage—they belong in a **separate sidecar or process**, not in this library:

| Goal | Why it was deferred here | Current path (see THE_DEEP_END.md) |
|------|--------------------------|--------------------------------------|
| Hard per-actor GC | JVM heap is shared; no API exposes per-`Job` GC roots | THE_DEEP_END §1: `Isolated<T>` ownership enforcement + optional off-heap `ActorArena` via Panama |
| Preemptive reduction counting | No JVM hook for cooperative preemption between arbitrary bytecode | THE_DEEP_END §2: cooperative `ReductionBudget` (Level 1); compiler plugin (Level 2); Java agent (Level 3) |
| Wire-compatible `erl_dist` | `dist_util.erl` + `epmd` handshake is 10 000+ lines of protocol | THE_DEEP_END §3: `otp-jinterface` module wrapping Erlang's official Java client; distribution primitives from §12 provide the in-JVM half |
| Hot code upgrade | JVM class loading cannot swap running instance state atomically without restart | THE_DEEP_END §4: `codeChange` callback + `CodeChangeRegistry` + `URLClassLoader` reload; `sys.replace_state` from §2 handles state migration — **unblocked by this roadmap** |
| `ets:match_spec` compiled patterns | ETS match specs are compiled by the BEAM's pattern-matching engine | THE_DEEP_END §5: typed Kotlin DSL on top of `OtpTable` from §11 — **unblocked by this roadmap** |

Update [TRACEABILITY.md](../../TRACEABILITY.md) and [LIMITATIONS.md](../../LIMITATIONS.md) as each section lands.
