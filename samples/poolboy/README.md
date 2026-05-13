# poolboy port (Phase 1 alpha test)

A Kotlin port of [`devinus/poolboy`](https://github.com/devinus/poolboy) — the canonical Erlang worker-pool library — built on top of the in-tree [kotlin-otp](../../) primitives. Doubles as the first realistic Phase 1 / alpha test of the public API surface; every gap or papercut shows up as a Kotlin compile/test failure here first.

Erlang sources mirrored, line-for-line where possible:

- [`poolboy.erl`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl) — the pool `gen_server`.
- [`poolboy_sup.erl`](https://github.com/devinus/poolboy/blob/master/src/poolboy_sup.erl) — the `simple_one_for_one` worker supervisor.
- [`example.erl` / `example_worker.erl` / `example.app`](https://github.com/devinus/poolboy/blob/master/README.md) from the upstream README — the optional pgSQL demo. Ported here with **H2** (in-memory, pure-Java JDBC) instead of `epgsql` so the sample stays self-contained.

## Run it

```
./gradlew :samples:poolboy:test           # 18-test matrix (~0.6s)
./gradlew :samples:poolboy:run            # ExampleApp main: starts pool1+pool2, runs SQL, stops cleanly
```

## File layout

| File | Mirror of | Purpose |
|------|-----------|---------|
| [`Poolboy.kt`](src/main/kotlin/org/otpstudy/poolboy/Poolboy.kt) | `poolboy:start_link/1,2`, `WorkerFactory` | Public entry point + `PoolRef` handle |
| [`PoolGenServer.kt`](src/main/kotlin/org/otpstudy/poolboy/PoolGenServer.kt) | `poolboy.erl` `init` / `handle_call` / `handle_cast` / `handle_info` / `terminate` | The pool actor |
| [`PoolMessages.kt`](src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt) | `#state{}` record, request tuples | Typed request / cast / info messages, `PoolStatus` |
| [`example/ExampleWorker.kt`](src/main/kotlin/org/otpstudy/poolboy/example/ExampleWorker.kt) | `example_worker.erl` | H2 JDBC worker `GenServer` |
| [`example/ExampleApp.kt`](src/main/kotlin/org/otpstudy/poolboy/example/ExampleApp.kt) | `example.erl`, `example.app` | Two-pool `SupervisorApplication` + `main` |
| [`PoolboyTest.kt`](src/test/kotlin/org/otpstudy/poolboy/PoolboyTest.kt) | poolboy `test/poolboy_tests.erl` (subset) | 12-test matrix |

## Erlang → Kotlin mapping

| Erlang / OTP | Kotlin / kotlin-otp | Notes |
|---|---|---|
| `gen_server:start_link({local, Name}, ...)` | [`GenServers.startLinkSync(parent, server, name = ...)`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt) + [`GlobalProcessRegistry.register`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ProcessRegistry.kt) | `Poolboy.startLink` is `suspend` and awaits the gen-server's `init` via `startLinkSync` — when it returns, all workers are spawned and the pool is ready |
| `process_flag(trap_exit, true)` | `override val trapExit get() = true` on `GenServer<S>` | Set defensively; the actual worker-death channel goes through `WorkerDown` info messages |
| `supervisor:start_link(?MODULE, {Mod, Args})` with `simple_one_for_one` | [`DynamicSupervisor.startLink`](../../otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/DynamicSupervisor.kt) + `SimpleOneForOneTemplate` | Workers use `Restart.Temporary`; the pool itself decides when to replace |
| `supervisor:start_child(Sup, [])` returning `{ok, Pid}` | `dynSup.startChildSync<SpawnedWorker<W>>().second` (template calls `ready(SpawnedWorker(…))`) | `startChild()` still returns only `childId` if you do not need the typed handle synchronously |
| `supervisor:terminate_child(Sup, Pid)` | `dynSup.terminateChild(childId)` | Used to dismiss overflow workers |
| `ets:new(monitors, [private])` + `ets:insert/lookup/delete` | In-actor `MutableMap<GenServerRef<W>, MonitorEntry<W>>` | Pool actor is single-threaded — no need for `OtpTable` |
| `queue:in/out` (FIFO checkout) | `ArrayDeque.addLast` / `removeFirst` | |
| `queue:in/out_r` (LIFO checkout) | `ArrayDeque.addLast` / `removeLast` | |
| `gen_server:reply(From, Reply)` for blocked checkout | [`ReplyResult.DeferReply(handle, state)`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt) + later `handle.reply(workerRef)` | Already implemented in kotlin-otp; this port uses it heavily |
| `erlang:demonitor(MRef, [flush])` | `DisposableHandle.dispose()` returned by `invokeOnCompletion` | |
| `'EXIT'` from a linked worker | `worker.job.invokeOnCompletion { deliver…(WorkerDown(worker)) }` (installed in `Poolboy.startLink`) | Workers are not `link`-ed in the OTP sense; pool learns exits via `WorkerDown` |
| `erlang:monitor(process, FromPid)` (borrower) | `borrower.invokeOnCompletion { deliver…(BorrowerDown(cref)) }` + dispose hook on checkin | Each checkout has a unique `cref`; `ReplyHandle.callerJob` supplies the default borrower `Job` from `GenServerRef.call` |
| `'DOWN'` message from `monitor/2` | `BorrowerDown(cref: CheckoutRef)` `InfoMsg` | We do not reuse [`org.otpstudy.genserver.Down`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/InfoMsg.kt) for borrowers because that type is for **monitors this actor installed**, not arbitrary borrower jobs |
| `gen_server:cast(Pool, {cancel_waiting, CRef})` on call timeout | `ref.cast(PoolCast.CancelWaiting(cref))` from the `try/catch` in `PoolRef.checkout` | |
| `application:get_env(example, pools)` | [`ApplicationEnv.require<List<PoolDef>>("pools")`](../../otp-application/src/main/kotlin/org/otpstudy/application/ApplicationEnv.kt) | Typed facade — no global process dictionary |
| `epgsql:connect/4` + `epgsql:squery/2` + `epgsql:equery/3` | `DriverManager.getConnection` + `Statement.execute` + `PreparedStatement.execute` | H2 in-memory; pure Java |

## Test matrix

18 tests (all green, ~0.6s, see [PoolboyTest.kt](src/test/kotlin/org/otpstudy/poolboy/PoolboyTest.kt)):

Core poolboy behaviour:

| # | Test | Asserts |
|---|------|---------|
| 1 | `pool_startup_size` | `size = 5` produces `PoolStatus(Ready, 5, 0, 0)` |
| 2 | `checkout_returns_distinct_workers` | 3 checkouts return 3 different refs; the 4th blocks then succeeds after a checkin |
| 3 | `lifo_strategy` | After three checkin's the next checkout returns the most-recent worker |
| 4 | `fifo_strategy` | After three checkin's the next checkout returns the oldest worker |
| 5 | `max_overflow` | `size=2, maxOverflow=1`: 3 checkouts succeed (`Full, 0, 1, 3`); 4th with `block=false` returns `null`; checkin of overflow dismisses it (`Overflow, 0, 0, 2`) |
| 6 | `borrower_death_returns_worker` | Borrower coroutine throws → pool sees `BorrowerDown` → worker re-queued |
| 7 | `worker_death_replaced` | `worker.job.cancelAndJoin()` → pool sees `WorkerDown` → spawns a replacement |
| 8 | `transaction_releases_on_exception` | `transaction { throw ... }` re-checks-in the worker; primary exception is re-thrown |
| 9 | `stop_terminates_supervisor` | `pool.stop()` ends the gen_server job |
| 10 | `cancel_waiting_on_timeout` | A `checkout` that times out while blocked must not later "win" a worker; next checkout works |
| 11 | `example_app_smoke` | `ExampleApp` (two H2 pools via `ApplicationEnv`) starts and stops cleanly |
| 12 | `example_app_runs_real_sql` | Through `pool.transaction` execute `CREATE TABLE` + 3× `INSERT` + `SELECT COUNT(*)`; assert count == 3 |

Robustness / API contract (added round 2):

| # | Test | Asserts |
|---|------|---------|
| 13 | `startlink_blocks_until_init_done` | `Poolboy.startLink` is `suspend` and returns only after all workers are spawned (no observable "warming up" window) |
| 14 | `init_failure_does_not_leak_supervisor` | Worker factory that throws → `startLink` propagates the failure synchronously and rolls back the dynamic supervisor (no orphaned children under `parent`) |
| 15 | `foreign_checkin_is_ignored` | `poolA.checkin(workerFromPoolB)` does not corrupt either pool's accounting; both continue serving |
| 16 | `double_checkin_is_ignored` | Calling `checkin(w)` twice on the same worker is a no-op on the second call |
| 17 | `stop_is_idempotent` | `pool.stop()` is safe to call repeatedly — second and third calls return immediately, no exception |
| 18a | `unknown_call_does_not_crash_pool` | An unknown `call` payload returns `null` (logged, not raised) instead of crashing the gen_server |
| 18b | `unknown_cast_does_not_crash_pool` | An unknown `cast` payload is logged and ignored — pool keeps serving |

## kotlin-otp integration notes (poolboy alpha)

These items were tightened while porting poolboy; they are **implemented** in-tree now (see [`CHANGELOG.md`](../../CHANGELOG.md) for breaking-change notes).

1. **Typed dynamic child start** — `SimpleOneForOneTemplate.start` takes an explicit **`ready(T)`** callback; **`startChildSync`** awaits the first `ready` (with timeout) and returns `Pair<childId, T>` while the child keeps running (`join()` in the template tail).
2. **`GenServer.init(self)`** — pool wiring uses **`AtomicReference<((InfoMsg) -> Unit)?>`** set to **`self::sendInfo`** on the first line of `init`; worker `invokeOnCompletion` hooks call that sink so **`WorkerDown`** reaches the mailbox without a channel forwarder.
3. **Caller `Job` on calls** — `GenServerRef.call` passes the suspending caller’s **`Job`** into **`GenServerMsg.Call`** / **`ReplyHandle.callerJob`** (captured **outside** `withTimeout` so borrower monitors stay tied to the real caller). **`PoolRequest.Checkout.borrower`** overrides when you are not in a normal `call` stack.
4. **JUnit 5 `@Test` must return `Unit` on the JVM** — expression-bodied tests whose last expression is not `Unit` are **silently ignored** by Jupiter. Prefer `: Unit = runBlocking { …; Unit }` or a block body; see [`LIMITATIONS.md`](../../LIMITATIONS.md) and [`.cursor/rules/kotlin-coroutines-boundaries.mdc`](../../.cursor/rules/kotlin-coroutines-boundaries.mdc).

## Poolboy hardening (round 2, all in `samples/poolboy/`)

Round 2 was poolboy-only — none of these required touching `otp-*`. They turn latent bugs into explicit, tested behaviour:

1. **`Poolboy.startLink` is `suspend`** — uses `GenServers.startLinkSync` so the gen_server's `init` (which prepopulates workers) is awaited synchronously. After `startLink` returns, the pool is fully ready (no observable "warming up" window). Test: `startlink_blocks_until_init_done`.
2. **Init-failure rollback** — when worker pre-population throws, the dynamic supervisor and any partially-spawned workers are torn down via `dynSup.shutdown()` (under `NonCancellable`) before the failure propagates to the caller. No orphaned children under `parent`. Test: `init_failure_does_not_leak_supervisor`.
3. **Foreign / duplicate `checkin` is logged + ignored** — handing a worker from another pool (or the same one, twice) into `PoolRef.checkin` no longer silently corrupts state; the pool logs via `OtpLogging` and skips the routing step. Tests: `foreign_checkin_is_ignored`, `double_checkin_is_ignored`.
4. **`PoolRef.stop()` is idempotent** — `AtomicBoolean` guard + `withContext(NonCancellable)` so the second call returns immediately and a `cancel()` mid-stop still completes the dyn-supervisor shutdown. Test: `stop_is_idempotent`.
5. **Unknown `call` / `cast` payloads no longer crash the pool** — `handleCallFrom` replies with `null` (matching OTP `{reply, {error, invalid_message}, State}` discipline), `handleCast` is a no-op; both log via `OtpLogging`. Tests: `unknown_call_does_not_crash_pool`, `unknown_cast_does_not_crash_pool`.
6. **O(1) cref → worker lookup** — `PoolState.crefIndex: MutableMap<CheckoutRef, GenServerRef<W>>` kept in sync with `monitors` so `BorrowerDown` and `cancel_waiting` paths no longer scan the full `monitors` table. No behaviour change at small `size`; matters for pools of thousands.

## kotlin-otp papercuts surfaced by round 2 (deferred)

These were noticed while writing the round-2 hardening tests. They are real and reproducible, but fixing them belongs in `otp-*` (separate round, not in scope for this poolboy delta):

1. **`DynamicSupervisor.startChildSync` serializes at the coordinator.** The supervisor coordinator is a single coroutine that suspends on `withTimeout { readyDeferred.await() }` *inside its main `for (event in events)` loop* while a `StartChildSync` event is in flight. Consequence: `coroutineScope { repeat(50) { launch { dynSup.startChildSync() } } }` runs 50× **sequentially**, defeating any attempt to parallelise pool warmup. Suggested fix: when handling `StartChildSync`, register the deferred against the slot and continue the coordinator loop; complete it later from a `ChildReady` event posted by the worker. This unlocks parallel `init` for poolboy (and a real cold-start win for size-50+ pools).

2. **Exceptions thrown by `SimpleOneForOneTemplate.start` propagate to the global coroutine exception handler.** When the worker factory inside `start` throws, the supervisor *correctly* propagates the failure to `startChildSync`'s caller via `reply.completeExceptionally(t)` — but the same exception also escapes the child `launch` (which has a `SupervisorJob` parent that doesn't trap it) and ends up printed to `stderr` by the default Kotlin handler. Visible in `init_failure_does_not_leak_supervisor`'s `system-err` output. Suggested fix: wrap the child launch's body in `try { ... } catch (t: Throwable) { /* the deferred carries this; do not rethrow */ }` once the deferred has been completed exceptionally.

## Documented follow-ups (deferred)

The original `poolboy:pool()` type allows three more registration variants we did not port — these test kotlin-otp's distribution layer rather than poolboy itself. Add when the distribution layer exits alpha:

- **`{Name, node()}`** — call a locally-named pool on a different JVM node. Two `LocalNode`s wired through [`InMemoryTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/InMemoryTransport.kt) is the minimal in-process test; full fidelity uses [`KotlinNodeTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/KotlinNodeTransport.kt) over loopback TCP.
- **`{global, GlobalName}`** — same as above but registered in `GlobalProcessRegistry` rather than addressed by name + node.
- **`{via, Module, Name}`** — a pluggable-registry escape hatch (gproc / syn equivalent). Needs a kotlin-otp change first: there's no `interface ViaRegistry { register/whereis/unregister }` today.

Suggested test sketch for `{global, ...}`:

```kotlin
@Test
fun pool_callable_via_global_registry(): Unit = runBlocking {
    val nodeA = LocalNode("a@localhost")
    val nodeB = LocalNode("b@localhost")
    val transport = InMemoryTransport().also { it.connect(nodeA, nodeB) }

    val pool = Poolboy.startLink(this, PoolConfig(size = 1, name = "global-pool"), testFactory())
    // assert nodeB.lookup("global-pool") returns a GenServerRef proxying to nodeA
    pool.stop()
}
```

## Conceptual gap deliberately preserved

A *connection pool* is a JVM-local resource (open `java.sql.Connection`s, sockets, file handles…) — checking one out on node A and using it on node B is meaningless on the JVM, just as it is on BEAM. So even when the distribution variants above land, the practical use case for "distributed poolboy" stays narrow (mostly: control-plane processes addressed via the pool, not the pooled resources themselves).
