# Changelog

All notable API changes for kotlin-otp are recorded here (library is pre–1.0; breaking changes are expected).

## [Unreleased]

### Breaking

- **`samples/poolboy`**: `PoolHandle.checkout()` returns `PooledWorker<W>?` (not `GenServerRef<W>?`). Use `worker.call` / `worker.checkin()` or `transaction { … }`. Local workers expose `LocalPooledWorker.ref` when needed.
- **`GenServer`**: `init` is now `suspend fun init(self: GenServerRef<S>): InitResult<S>`. Use `self` for `sendInfo` / `call` / `cast` during startup; messages enqueue until the mailbox loop runs after `init` returns.
- **`SimpleOneForOneTemplate`**: the `start` lambda is  
  `suspend (scope: CoroutineScope, childId: String, ready: (T) -> Unit) -> Unit`.  
  Invoke **`ready(value)` exactly once** when the supervised resource is ready. Use **`DynamicSupervisorRef.startChildSync`** (default timeout 60s) to await that value as `Pair<childId, T>` while the child coroutine continues (for example `ref.job.join()` after `ready`).
- **`GenServerMsg.Call` / `ReplyHandle`**: carry an optional **`callerJob: Job?`** for protocols that monitor the caller (worker pools, leases). Populated from **`GenServerRef.call`** when the call is not synthetic.

### Fixed

- **`GenServerRef.call`**: `callerJob` was previously read **inside** `withTimeout { … }`, so it captured the timeout scope’s **Job**, which **completes when the call returns**. That broke any `Job.invokeOnCompletion` “borrower monitor” installed during the call (workers were returned to pools immediately after checkout). **`callerJob` is now captured before entering `withTimeout`.**
- **`GenServer`**: `ReplyResult.Reply` / `Stop` now assign **`state = newState` before `reply.complete()`** so concurrent callers observe updated state.
- **`DynamicSupervisor` / `DynamicSupervisorRef.startChildSync`**: The coordinator no longer blocks on the ready handshake (it is offloaded to a separate coroutine), so concurrent sync starts and other supervisor queries proceed in parallel. **`shutdown()`** while a sync start is still waiting completes that call with **`IllegalStateException("shutting down")`** instead of leaving the caller suspended forever. Failures in **`SimpleOneForOneTemplate.start`** before the first **`ready`** no longer rethrow as an uncaught coroutine failure on the default handler (stderr); handshake cleanup avoids a **`Restart.Permanent`** infinite restart loop when **`ready`** was never reached.

### Samples

- **`samples/poolboy`**: round-2 hardening — `Poolboy.startLink` is now **`suspend`** and uses `GenServers.startLinkSync` so init failures are surfaced synchronously and the dynamic supervisor is rolled back; `PoolRef.stop()` is idempotent under `NonCancellable`; foreign / duplicate `checkin` is logged + ignored; unknown `call` / `cast` payloads no longer crash the pool; new `crefIndex` for O(1) `BorrowerDown` / `cancel_waiting` lookup.
- **`samples/poolboy`**: round-3 hardening (taking advantage of the upstream `DynamicSupervisor` coordinator fix) — `PoolGenServer.init` pre-populates workers in parallel via `coroutineScope { … async { spawn() }.awaitAll() }` (observable speed-up when the `WorkerFactory` blocks on the worker's `init`, e.g. uses `GenServers.startLinkSync`); new public `PoolStoppedException` is thrown by `PoolRef.checkout` / `status` / `transaction` after `stop()` so callers no longer have to catch the internal `ServerDownException`. See [`samples/poolboy/README.md`](samples/poolboy/README.md).
- **`otp-registry`**: [`ProcessResolver`](otp-registry/src/main/kotlin/org/otpstudy/registry/ProcessResolver.kt), [`ViaRegistry`](otp-registry/src/main/kotlin/org/otpstudy/registry/ViaRegistry.kt), [`MapViaRegistry`](otp-registry/src/main/kotlin/org/otpstudy/registry/ViaRegistry.kt) — pluggable `{via, Mod, Name}`-style name resolution.
- **`samples/poolboy`**: round-4 distribution addressing — [`PoolAddress`](samples/poolboy/src/main/kotlin/org/otpstudy/poolboy/PoolAddress.kt), [`PoolHandle`](samples/poolboy/src/main/kotlin/org/otpstudy/poolboy/PoolHandle.kt), [`RemotePoolHandle`](samples/poolboy/src/main/kotlin/org/otpstudy/poolboy/RemotePoolHandle.kt), `Poolboy.resolve` / top-level `checkout` helpers; optional `homeNode` on `startLink` for `{Name, Node}`; depends on `otp-distribution` and `otp-global`.
- **`samples/poolboy`**: round-5 TCP remote poolboy — [`PoolWire`](samples/poolboy/src/main/kotlin/org/otpstudy/poolboy/PoolWire.kt), [`PooledWorker`](samples/poolboy/src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt), [`BorrowerLease`](samples/poolboy/src/main/kotlin/org/otpstudy/poolboy/PoolMessages.kt) (`Local` / `Remote`), `ForwardCall` / token checkin, [`useLease`](samples/poolboy/src/main/kotlin/org/otpstudy/poolboy/PooledWorker.kt), [`KotlinNodeTransport`](otp-distribution/src/main/kotlin/org/otpstudy/distribution/KotlinNodeTransport.kt) loopback tests. 34-test matrix.
- **`otp-distribution`**: [`DistributionWire.encodeSerializable`](otp-distribution/src/main/kotlin/org/otpstudy/distribution/DistributionWire.kt) / `decodeSerializable`; [`DistMsg.Global`](otp-distribution/src/main/kotlin/org/otpstudy/distribution/DistMsg.kt) + [`GlobalDistMsg`](otp-distribution/src/main/kotlin/org/otpstudy/distribution/GlobalDistMsg.kt); [`RemoteGenServerRef`](otp-distribution/src/main/kotlin/org/otpstudy/distribution/RemoteGenServerRef.kt); `KotlinNodeTransport` awaits gen-server replies before sending RPC response.
- **`otp-global`**: [`GlobalRegistry.install`](otp-global/src/main/kotlin/org/otpstudy/global/GlobalRegistry.kt) — peer replication, `syncPeers` on connect, `RemoteGenServerRef` for remote names; replication tests with [`InMemoryTransport`](otp-distribution/src/main/kotlin/org/otpstudy/distribution/InMemoryTransport.kt).

### Migration snippets

**Dynamic supervisor template**

```kotlin
val template = SimpleOneForOneTemplate<MyHandle>(
    restart = Restart.Temporary,
    shutdown = Shutdown.Timeout(5.seconds),
    start = { scope, childId, ready ->
        val ref = factory.start(scope)
        ready(MyHandle(childId, ref)) // exactly once; startChildSync awaits this
        ref.job.join()
    },
)
val sup = DynamicSupervisor.startLink(parent, flags, template)
val (_, handle) = sup.startChildSync<MyHandle>()
```

**GenServer init**

```kotlin
override suspend fun init(self: GenServerRef<MyState>): InitResult<MyState> {
    self.sendInfo(MyStartupInfo)
    return InitResult.Ok(MyState())
}
```
