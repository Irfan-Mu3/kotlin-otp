# poolboy port (Phase 1 alpha test)

A Kotlin port of [`devinus/poolboy`](https://github.com/devinus/poolboy) — the canonical Erlang worker-pool library — built on top of the in-tree [kotlin-otp](../../) primitives. Doubles as the first realistic Phase 1 / alpha test of the public API surface; every gap or papercut shows up as a Kotlin compile/test failure here first.

Erlang sources mirrored, line-for-line where possible:

- [`poolboy.erl`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl) — the pool `gen_server`.
- [`poolboy_sup.erl`](https://github.com/devinus/poolboy/blob/master/src/poolboy_sup.erl) — the `simple_one_for_one` worker supervisor.
- [`example.erl` / `example_worker.erl` / `example.app`](https://github.com/devinus/poolboy/blob/master/README.md) from the upstream README — the optional pgSQL demo. Ported here with **H2** (in-memory, pure-Java JDBC) instead of `epgsql` so the sample stays self-contained.

## Run it

```
./gradlew :samples:poolboy:test           # 34-test matrix (~1s)
./gradlew :samples:poolboy:run            # ExampleApp main: starts pool1+pool2, runs SQL, stops cleanly
```

## File layout

| File | Mirror of | Purpose |
|------|-----------|---------|
| [`Poolboy.kt`](src/main/kotlin/org/otpstudy/poolboy/Poolboy.kt) | `poolboy:start_link/1,2`, `checkout/1–3`, `WorkerFactory` | Public entry point, `PoolRef`, `resolve`, top-level `checkout`/`transaction` |
| [`PoolHandle.kt`](src/main/kotlin/org/otpstudy/poolboy/PoolHandle.kt) | `poolboy:checkout`, `transaction` | Shared local + remote pool API |
| [`PoolAddress.kt`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) | `poolboy:pool()` type | `{local, Name}`, `{Name, Node}`, `{global, Name}`, `{via, …}` |
| [`RemotePoolHandle.kt`](src/main/kotlin/org/otpstudy/poolboy/RemotePoolHandle.kt) | remote `gen_server:call` to pool | Pool RPCs via [`NodeTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/NodeTransport.kt) |
| [`PoolGenServer.kt`](src/main/kotlin/org/otpstudy/poolboy/PoolGenServer.kt) | `poolboy.erl` `init` / `handle_call` / `handle_cast` / `handle_info` / `terminate` | The pool actor |
| [`PoolMessages.kt`](src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt) | `#state{}` record, request tuples | Typed request / cast / info messages, `PoolStatus`, `PoolStoppedException` |
| [`example/ExampleWorker.kt`](src/main/kotlin/org/otpstudy/poolboy/example/ExampleWorker.kt) | `example_worker.erl` | H2 JDBC worker `GenServer` |
| [`example/ExampleApp.kt`](src/main/kotlin/org/otpstudy/poolboy/example/ExampleApp.kt) | `example.erl`, `example.app` | Two-pool `SupervisorApplication` + `main` |
| [`PoolWire.kt`](src/main/kotlin/org/otpstudy/poolboy/PoolWire.kt) | — | `@Serializable` pool RPC payloads for TCP |
| [`PooledWorker.kt`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt) | remote pid + `gen_server:call` | `LocalPooledWorker` / `RemotePooledWorker` handles |
| [`PoolboyTest.kt`](src/test/kotlin/org/otpstudy/poolboy/PoolboyTest.kt) | poolboy `test/poolboy_tests.erl` (subset) | 34-test matrix |

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
| `pool()` — local name | [`PoolAddress.Local`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) → [`GlobalProcessRegistry`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ProcessRegistry.kt) | Same JVM |
| `pool()` — `{Name, Node}` | [`PoolAddress.OnNode`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) + [`LocalNode.register`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/LocalNode.kt) on `homeNode` | [`RemotePoolHandle`](src/main/kotlin/org/otpstudy/poolboy/RemotePoolHandle.kt) + [`KotlinNodeTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/KotlinNodeTransport.kt) (TCP loopback tests) or [`InMemoryTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/InMemoryTransport.kt) |
| `pool()` — `{global, Name}` | [`PoolAddress.Global`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) → [`GlobalRegistry`](../../otp-global/src/main/kotlin/org/otpstudy/global/GlobalRegistry.kt) | Wire-replicated via [`DistMsg.Global`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/DistMsg.kt); remote hits use [`RemoteGenServerRef`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/RemoteGenServerRef.kt) |
| BEAM remote pid after checkout | [`PooledWorker`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt) + [`PoolRequest.ForwardCall`](src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt) | Cross-JVM: [`WorkerToken`](src/main/kotlin/org/otpstudy/poolboy/PoolWire.kt) + forward through home pool — not a distributable [`GenServerRef`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt) |
| `pool()` — `{via, Mod, Name}` | [`PoolAddress.Via`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) + [`ViaRegistry`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ViaRegistry.kt) | [`ProcessResolver`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ProcessResolver.kt) instead of an atom module name |

## Test matrix

34 tests (all green, ~1s, see [PoolboyTest.kt](src/test/kotlin/org/otpstudy/poolboy/PoolboyTest.kt)):

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

Performance + ergonomics (added round 3, after upstream coordinator fix):

| # | Test | Asserts |
|---|------|---------|
| 19 | `parallel_init_spawns_workers_concurrently` | 5 workers × 200ms `init` each → `Poolboy.startLink` completes in < 600ms (would be > 1s if pool init were serial). Uses a synthetic `startLinkSync` factory so the spawn round-trip blocks on the worker's `init` |
| 20 | `parallel_init_propagates_first_failure_and_rolls_back` | Factory throws on the 3rd of 5 parallel spawns; `startLink` rethrows; structured-concurrency cancels in-flight siblings; `Poolboy.startLink`'s catch shuts down the dyn supervisor — no leaked children under `parent`; subsequent `startLink` on the same scope works |
| 21 | `checkout_on_stopped_pool_throws_pool_stopped_exception` | After `pool.stop()`, `pool.checkout()` and `pool.status()` both throw `PoolStoppedException` with the pool name, wrapping the underlying `ServerDownException` as `cause` |
| 22 | `transaction_on_stopped_pool_throws_pool_stopped_exception` | `pool.transaction { … }` on a stopped pool throws `PoolStoppedException`, not the previous "pool returned null worker" / raw `ServerDownException` leak |

Distribution addressing (round 4):

| # | Test | Asserts |
|---|------|---------|
| 23 | `pool_resolvable_via_local_node` | Pool on node A registered on [`LocalNode`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/LocalNode.kt); node B resolves via [`PoolAddress.OnNode`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) + [`InMemoryTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/InMemoryTransport.kt) → `status()` |
| 24 | `pool_checkout_via_remote_node_inmemory` | Remote checkout returns a worker; `worker.call` works (same JVM) |
| 25 | `pool_global_registry_resolution` | [`PoolAddress.Global`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) + [`GlobalRegistry.registerName`](../../otp-global/src/main/kotlin/org/otpstudy/global/GlobalRegistry.kt) |
| 26 | `pool_via_registry_resolution` | [`PoolAddress.Via`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) + [`MapViaRegistry`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ViaRegistry.kt) |
| 27 | `remote_checkout_surfaces_pool_stopped` | After `pool.stop()` on home node, remote `checkout` throws [`PoolStoppedException`](src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt) |

TCP + global replication (round 5, kotlin-otp-only — no BEAM/jinterface):

| # | Test | Asserts |
|---|------|---------|
| — | `pool_wire_roundtrip` | [`DistributionWire.encodeSerializable`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/DistributionWire.kt) round-trips [`WirePoolRequest`](src/main/kotlin/org/otpstudy/poolboy/PoolWire.kt) / [`WireCheckoutResult`](src/main/kotlin/org/otpstudy/poolboy/PoolWire.kt) |
| — | `wire_checkout_retains_lease_until_checkin` | Wire checkout leaves `monitors=1`; [`ForwardCall`](src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt) succeeds ([`BorrowerLease.Remote`](src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt)) |
| 28 | `pool_checkout_via_tcp_loopback` | Two [`KotlinNodeTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/KotlinNodeTransport.kt)s; remote checkout → [`RemotePooledWorker.call`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt) |
| 29 | `pool_status_via_tcp_loopback` | Remote `status()` over TCP |
| 30 | `tcp_checkin_returns_worker_to_pool` | Checkout → call → token checkin → second checkout |
| 31 | `pool_global_via_tcp_loopback` | [`GlobalRegistry.install`](../../otp-global/src/main/kotlin/org/otpstudy/global/GlobalRegistry.kt) + [`PoolAddress.Global`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt) over TCP |

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

## Round-3 hardening (after upstream coordinator fix)

The two kotlin-otp papercuts surfaced by round 2 (`startChildSync` coordinator serialization + template-exception stderr leak) have been **fixed upstream** in `otp-supervisor`. Round 3 builds on that fix to take two more wins in the poolboy sample, again entirely inside `samples/poolboy/`:

7. **Parallel worker pre-population in `PoolGenServer.init`.** `init` now uses `coroutineScope { (0 until size).map { async { workerHandler.spawn() } }.awaitAll() }` instead of a serial `repeat(size) { spawn() }`. Erlang poolboy's `prepopulate/3` is **serial**; this is a deliberate Kotlin win once `startChildSync` no longer blocks the coordinator. The win is observable when the user's `WorkerFactory` blocks on the worker's `init` — which is the right default for resource-holding workers like DB connections, and which the included [`ExampleWorker.factory`](src/main/kotlin/org/otpstudy/poolboy/example/ExampleWorker.kt) now does (it uses `GenServers.startLinkSync`, so JDBC connection failures surface at `Poolboy.startLink` time and a 50-worker pool warms up in roughly the time of a single connection). On failure, structured concurrency cancels in-flight siblings and `Poolboy.startLink`'s existing catch shuts down the dyn supervisor — no behaviour change for the failure path. Tests: `parallel_init_spawns_workers_concurrently`, `parallel_init_propagates_first_failure_and_rolls_back`.
8. **`PoolStoppedException`.** A new public exception type wraps `ServerDownException` from `PoolRef.checkout` / `PoolRef.status` / `PoolRef.transaction` after the pool has stopped. Carries the pool name and preserves the underlying cause. Lets callers catch "pool is gone" without depending on kotlin-otp's internal exception types. Tests: `checkout_on_stopped_pool_throws_pool_stopped_exception`, `transaction_on_stopped_pool_throws_pool_stopped_exception`.

## kotlin-otp papercuts surfaced by round 2 (now resolved upstream)

Both items below were originally "out of scope, fix in `otp-*`". They were addressed in a follow-up round to `otp-supervisor`; documented here as the historical record of what this port surfaced:

1. **`DynamicSupervisor.startChildSync` serialized at the coordinator.** The supervisor coordinator suspended on `withTimeout { readyDeferred.await() }` *inside its main event loop* while a `StartChildSync` event was in flight, defeating concurrent sync starts. **Fix landed:** the ready handshake is offloaded to a `supervisorScope.launch` that posts a `StartChildSyncAwaitResult` event back to the coordinator. The coordinator stays responsive; concurrent `startChildSync` calls genuinely parallelize. New observable failure mode: `startChildSync` in flight during `dynSup.shutdown()` now completes with `IllegalStateException("shutting down")` instead of hanging.
2. **Exceptions thrown by `SimpleOneForOneTemplate.start` leaked to stderr.** Failure was correctly delivered to `startChildSync`'s caller via `reply.completeExceptionally(t)`, but the same exception also escaped the child `launch` (parented by a `SupervisorJob`) and ended up printed to `stderr` by the default Kotlin handler. **Fix landed:** when the sync deferred wins the `completeExceptionally` race, the child returns from its `launch` body without rethrowing; a slot-flag guard (`lastSyncStartFailedEpoch`) prevents the resulting `ChildExited(Normal)` from triggering a `Restart.Permanent` loop, regardless of whether the await-result event or the exit event reaches the coordinator first.

## Round 4 — distribution addressing

Mirrors the remaining `poolboy:pool()` registration variants. Implemented in `samples/poolboy/` plus a small [`otp-registry`](../../otp-registry) addition:

1. **[`PoolAddress`](src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt)** — `Local`, `OnNode`, `Global`, `Via(ProcessResolver, name)`.
2. **[`PoolHandle`](src/main/kotlin/org/otpstudy/poolboy/PoolHandle.kt)** — shared `checkout` / `checkin` / `status` / `stop` / `transaction` for [`PoolRef`](src/main/kotlin/org/otpstudy/poolboy/Poolboy.kt) (local) and [`RemotePoolHandle`](src/main/kotlin/org/otpstudy/poolboy/RemotePoolHandle.kt).
3. **[`Poolboy.resolve`](src/main/kotlin/org/otpstudy/poolboy/Poolboy.kt)** + top-level `Poolboy.checkout` / `checkin` / `status` / `transaction` — OTP-style entry points without holding a `PoolRef`.
4. **`homeNode: LocalNode?` on `startLink`** — when set with a `name`, registers the pool gen_server on that node's registry so `{Name, Node}` works.
5. **[`ViaRegistry`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ViaRegistry.kt)** + [`MapViaRegistry`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ViaRegistry.kt) — `{via, …}` escape hatch (no gproc/syn port).

## Round 5 — TCP remote poolboy + distributed global

**kotlin-otp-only distribution:** TCP links between JVMs via [`KotlinNodeTransport`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/KotlinNodeTransport.kt) + kotlinx.serialization — not Erlang EPMD, ETF, or mixed clusters ([`LIMITATIONS.md`](../../LIMITATIONS.md)).

1. **[`PoolWire`](src/main/kotlin/org/otpstudy/poolboy/PoolWire.kt)** — `WirePoolRequest` / `WirePoolCast` / `WireCheckoutResult` / `WorkerToken`; [`RemotePoolHandle`](src/main/kotlin/org/otpstudy/poolboy/RemotePoolHandle.kt) always encodes on the wire.
2. **[`PooledWorker<W>`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt)** — `checkout()` returns `PooledWorker?` instead of `GenServerRef`; [`LocalPooledWorker`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt) vs [`RemotePooledWorker`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt) (`ForwardCall` + token checkin).
3. **[`DistributionWire.encodeSerializable`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/DistributionWire.kt)** — registered `@Serializable` types round-trip as `JsonElement` (not `toString()`).
4. **[`GlobalRegistry.install`](../../otp-global/src/main/kotlin/org/otpstudy/global/GlobalRegistry.kt)** — broadcast [`GlobalDistMsg`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/GlobalDistMsg.kt) on [`DistMsg.Global`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/DistMsg.kt); `syncPeers` on connect; remote `whereis` → [`RemoteGenServerRef`](../../otp-distribution/src/main/kotlin/org/otpstudy/distribution/RemoteGenServerRef.kt).

**Remote borrower limitation:** TCP checkout uses [`BorrowerLease.Remote`](src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt) — no home-node `Job` monitor. Return workers via [`PooledWorker.checkin`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt), [`useLease { }`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt), `transaction` `finally`, or transport `call` timeout — not automatic return on client coroutine cancel. Follow-up: `WireBorrowerDown` cast from client on cancel.

**OTP divergence (documented):** global registration uses async broadcast + `syncPeers`, not synchronous OTP `multi_call` on every `register_name`; netsplit locker protocol is out of scope.

## Conceptual gap deliberately preserved

A *connection pool* is a JVM-local resource (open `java.sql.Connection`s, sockets, file handles…) — checking one out on node A and using it on node B is meaningless on the JVM, just as it is on BEAM. Cross-JVM use is **reach the pool on the home node** and forward worker RPCs through [`PooledWorker`](src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt) — not shipping JDBC connections or `GenServerRef` mailboxes across the wire.
