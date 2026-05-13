# OTP documentation → Kotlin modules

| OTP concept / doc | Kotlin module | Notes |
|-------------------|---------------|--------|
| [Design principles — supervision trees](https://www.erlang.org/doc/design_principles/design_principles.html) | `otp-supervisor`, `otp-application` | Tree shape and lifecycle are approximated with coroutine scopes and jobs. |
| [Supervisor behaviour / child specs](https://www.erlang.org/doc/design_principles/sup_princ.html) | `otp-supervisor` | `SupervisorFlags` (`one_for_one` / `one_for_all` / `rest_for_one`), `ChildSpec`, `RestartPolicy`, `RestartBackoff`, ordered shutdown. `RestartIntensityScope` extends OTP's per-supervisor window to a per-child option. |
| [gen_server concepts](https://www.erlang.org/doc/design_principles/gen_server_concepts.html) | `otp-gen-server` | Callback shape and mailbox loop; `handleInfo` now takes typed `InfoMsg`; high-priority control channel via `GenServerRef.sendControl`; `TerminateReason` mapped on external cancellation. See [LIMITATIONS.md](LIMITATIONS.md). |
| Process mailboxes / message ordering | `otp-mailbox` | `Channel`-backed mailbox; not selective receive. |
| Logical process identity (for debugging) | `otp-core` | `OtpProcessId`, `Restart`, `Shutdown`, `ChildType`. |
| OTP `application` resource and start phases | `otp-application` | `OtpApplication` + `SupervisorApplication` (flat); `PhasedSupervisorApplication` adds ordered `StartPhase` list and `ApplicationEnv` typed config facade. |
| [`gen_statem`](https://www.erlang.org/doc/design_principles/statem.html) | `otp-gen-statem` | `GenStateM` + `StateMTransition`; OTP-style timeouts (`StateTimeout` / `EventTimeout` / `GenericTimeout`) with epoch-based cancellation; `GenStateMHandleEvent` adapter for `handle_event_function` style (M2). |
| `gen_statem` callback modes (`handle_event_function`) | `otp-gen-statem` | `GenStateMHandleEvent<S,D,E>` interface + `GenStateMs.startLinkHandleEvent`; events and timeouts unified under `StateMInput`. |
| [`gen_event`](https://www.erlang.org/doc/design_principles/events.html) | `otp-gen-event` | `GenEventManagers`, `GenEventHandler`; ordered synchronous notification; `BackPressurePolicy` (`Block` / `DropOldest` / `DropNew`). |
| Dynamic supervisors (`simple_one_for_one`) | `otp-supervisor` | `DynamicSupervisor` extended to support all three strategies (`one_for_one`, `one_for_all`, `rest_for_one`) on dynamic children; KDoc documents semantics and JVM/OTP gap. |
| Process naming / registration (library-only) | `otp-registry` | `ProcessRegistry`, `GlobalProcessRegistry`, `RegistryLifecycle` helper for auto-register/unregister on GenServer start/stop. |
| Links / monitors (library-only) | `otp-core` | `ProcessMonitor`, `DownMessage`, `linkJobs`. |
| Structured logging hooks | `otp-core` | `OtpLogging`, `OtpLogger`, `OtpLogContext` (extended with `supervisorId`, `childId`, `restartCount`, `stateLabel`). |
| Observability / OpenTelemetry | `otp-core` | `OtpTracer` / `OtpSpan` no-op interfaces; swap in an OTel-backed implementation via `OtpLogging.setTracer`. |
| Isolation: Loom / virtual threads | — | No separate module; dispatcher parameter on all `startLink` calls. See [LIMITATIONS.md](LIMITATIONS.md) for the isolation ladder and a Loom example. |

## DEEPER_STILL.md — implemented

| DEEPER_STILL section | Kotlin module / file | Notes |
|----------------------|---------------------|-------|
| §1 Bounded mailboxes and back-pressure | `otp-gen-server` · `MailboxBound.kt` | `OverflowPolicy` sealed class (Block / DropOldest / DropNew / CrashSender / DeadLetterTo); `MailboxBound` wired into `GenServers.startLink` and `GenStateMs.startLink`; `DeadLetterMsg` added to `InfoMsg`. |
| §2 sys module (inspect / suspend / resume) | `otp-gen-server` · `SysMsg.kt`, `GenServer.kt` | `SysMsg` sealed class; third `sysMailbox` channel; `select {}` over sys + user mailboxes; `suspended` flag + suspend loop; `GenServerRef.sysGetState/Status/ReplaceState/Suspend/Resume`. |
| §3 Deferred replies (`reply/2`) | `otp-gen-server` · `GenServer.kt` | `ReplyHandle` + `DeferReply` in `ReplyResult`; `handleCallFrom` default-delegates to `handleCall` for backward compat. |
| §4 OTP timers (`send_after` / `start_timer`) | `otp-gen-server` · `OtpTimers.kt` | `TimerRef` wrapping `Job`; `OtpTimers.sendAfter` + `sendInterval`; `TimerTick` info message. |
| §5 Selective receive | `otp-mailbox` · `SelectiveMailbox.kt` | `SelectiveMailbox<T>` with `saved: ArrayDeque<T>`; `receive(matches)` drains saved first; `flushSaved()` returns deferred messages to channel. |
| §6 `gen_statem` postpone + `state_enter` | `otp-gen-statem` · `GenStateM.kt` | `postpone: Boolean` on `Stay`/`Next`; `postponedQueue` drained on each loop iteration after a state change; `onEnterState` callback; infinite-loop guard for same-state re-enter. |
| §7 Links and exit signals (`link` / `trap_exit`) | `otp-gen-server` · `ExitSignal.kt`, `GenServer.kt` | `ExitSignal.Exit` info message; `OtpLink` with `AtomicBoolean`; bidirectional `invokeOnCompletion` hooks; `trapExit` flag on `GenServerRef` routes exit signal instead of cancel. |
| §8 Crash reports (`error_logger` / `logger`) | `otp-gen-server` · `CrashReport.kt` | `CrashReport` data class; `CrashReporter` fun interface; `CrashReporting` singleton with `AtomicReference`; wired into GenServer run-loop failure catch. |
| §9 Supervisor bridge (non-GenServer children) | `otp-supervisor` · `SupervisorBridge.kt` | `BridgeRef` with `exitChannel`; `BridgeExit` sealed class; `SupervisorBridge.childSpec` wraps arbitrary `suspend` blocks as supervised `ChildSpec`. |
| §10 `persistent_term` | `otp-core` · `OtpPersistentTerms.kt` | `AtomicReference<Snapshot>` for O(1) reads; `synchronized` writes; `watch()` returning `AutoCloseable`; write-count Warn log. |
| §11 ETS tables | `otp-ets` · `OtpTable.kt`, `OtpTableRegistry.kt` | `OtpTable<K,V>` with `TableType` (Set/OrderedSet/Bag/DuplicateBag) and `TableAccess`; `ReentrantReadWriteLock`; `OtpTableRegistry` global ConcurrentHashMap with owner-job lifecycle; `TableStats`. |
| §12 Distribution primitives | `otp-distribution` · `LocalNode.kt`, `RemoteNodeStub.kt`, `InMemoryTransport.kt` | `NodeId`, `OtpNode` interface, `NodeTransport` interface; `LocalNode` (in-JVM node with ConcurrentHashMap registry); `RemoteNodeStub`; `InMemoryTransport` (in-process transport for tests); `DistributedChildSpec`. |
| §13 Observer data layer | `otp-observer` · `OtpObserver.kt`, `ProcessTable.kt` | `ProcessSnapshot`; `OtpObserver.inspectProcess` via sys channel; `ProcessTable` with lazy probe lambdas; `OtpObserver.tableStats` from `OtpTableRegistry`. |

**Historical roadmap narrative** (DEEPER → EVEN_DEEPER → DEEPER_STILL → THE_DEEP_END → BEYOND): [docs/archive/roadmaps/README.md](docs/archive/roadmaps/README.md).

## THE_HADAL_ZONE (next roadmap)

[THE_HADAL_ZONE.md](THE_HADAL_ZONE.md) is the following layer after [THE_ABYSS.md](THE_ABYSS.md): Kotlin-native TCP transport, scatter-gather, leader election, ports, extended `sys`, JVM post-mortem snapshots, supervision-tree views, and profiling — all **without** Erlang/BEAM interop. Prefer **module-scoped** `./gradlew :<module>:test` and the **Suggested order** section in that doc when implementing; see [scripts/README.md](scripts/README.md).

| THE_HADAL_ZONE theme | Kotlin module / file | Notes |
|----------------------|---------------------|--------|
| §1 Out of scope (ETF / BEAM wire) | — | Documented in [THE_HADAL_ZONE.md](THE_HADAL_ZONE.md) only; no `otp-etf`. |
| §2 Kotlin TCP + JSON frames | `otp-distribution` · `KotlinNodeTransport.kt`, `DistMsg.kt`, `DistributionWire.kt` | `wire` / `startAccepting` / `connectOut`; `decodeGenServerPayload` maps JSON null → `Unit` for `GenServerRef`. |
| §3 `abcast` / `multi_call` | `otp-distribution` · `DistributedGenServers.kt` | Over [NodeTransport]; works with [InMemoryTransport](otp-distribution/src/main/kotlin/org/otpstudy/distribution/InMemoryTransport.kt). |
| §4 Leader election (bully) | `otp-distribution` · `GenLeader.kt` | `GenLeaders.startLink`, `LeaderCallbacks`, `LeaderMsg.Elect(exclude)`. |
| §5 Global `timer` | `otp-gen-server` · `OtpTimer.kt` | Distinct from per-actor [OtpTimers](otp-gen-server/src/main/kotlin/org/otpstudy/genserver/OtpTimers.kt). |
| §6 Ports | `otp-gen-server` · `OtpPort.kt` | `PortData` / `PortExit` as `InfoMsg`. |
| §7 `sys:get_state` / `replace_state` names | `otp-gen-server` · `GenServer.kt` | `getState` / `replaceState` aliases on `GenServerRef` (sys channel). |
| §8 JVM post-mortem snapshot | `otp-observer` · `Postmortem.kt` | `Postmortem.capture()` + `ProcessTable.all()`. |
| §9 Supervision tree ASCII/DOT | `otp-observer` · `SupervisorTree.kt` | `SupervisorRef.whichChildren()` from `otp-supervisor`. |
| §10 Profiling wrapper | `otp-gen-server` · `ProfiledGenServer.kt` | `GenServer.profiled()`. |
| §11 Kotlin vs TS (meta) | — | Roadmap prose only in [THE_HADAL_ZONE.md](THE_HADAL_ZONE.md). |
| Static supervisor introspection | `otp-supervisor` · `Supervisor.kt` | `SupervisorChildInfo`, `whichChildren()` via snapshot lambda (no `ChildSlot` leak). |
