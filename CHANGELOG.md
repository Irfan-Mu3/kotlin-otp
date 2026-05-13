# Changelog

All notable API changes for kotlin-otp are recorded here (library is pre–1.0; breaking changes are expected).

## [Unreleased]

### Breaking

- **`GenServer`**: `init` is now `suspend fun init(self: GenServerRef<S>): InitResult<S>`. Use `self` for `sendInfo` / `call` / `cast` during startup; messages enqueue until the mailbox loop runs after `init` returns.
- **`SimpleOneForOneTemplate`**: the `start` lambda is  
  `suspend (scope: CoroutineScope, childId: String, ready: (T) -> Unit) -> Unit`.  
  Invoke **`ready(value)` exactly once** when the supervised resource is ready. Use **`DynamicSupervisorRef.startChildSync`** (default timeout 60s) to await that value as `Pair<childId, T>` while the child coroutine continues (for example `ref.job.join()` after `ready`).
- **`GenServerMsg.Call` / `ReplyHandle`**: carry an optional **`callerJob: Job?`** for protocols that monitor the caller (worker pools, leases). Populated from **`GenServerRef.call`** when the call is not synthetic.

### Fixed

- **`GenServerRef.call`**: `callerJob` was previously read **inside** `withTimeout { … }`, so it captured the timeout scope’s **Job**, which **completes when the call returns**. That broke any `Job.invokeOnCompletion` “borrower monitor” installed during the call (workers were returned to pools immediately after checkout). **`callerJob` is now captured before entering `withTimeout`.**

### Samples

- **`samples/poolboy`**: round-2 hardening — `Poolboy.startLink` is now **`suspend`** and uses `GenServers.startLinkSync` so init failures are surfaced synchronously and the dynamic supervisor is rolled back; `PoolRef.stop()` is idempotent under `NonCancellable`; foreign / duplicate `checkin` is logged + ignored; unknown `call` / `cast` payloads no longer crash the pool; new `crefIndex` for O(1) `BorrowerDown` / `cancel_waiting` lookup. 18-test matrix (was 12). See [`samples/poolboy/README.md`](samples/poolboy/README.md) — also flags two new kotlin-otp papercuts (`DynamicSupervisor.startChildSync` serializes at the coordinator; `SimpleOneForOneTemplate.start` exceptions leak to stderr) for a future round.

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
