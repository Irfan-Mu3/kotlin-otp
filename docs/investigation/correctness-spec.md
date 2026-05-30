# Correctness Spec

## GenLeader

Target implementation:

- `otp-distribution/src/main/kotlin/org/otpstudy/distribution/GenLeader.kt`

Algorithm summary:

- Bully-style election where winner is the lexicographically greatest `name@host` among `peers + localNode`, minus optional `exclude`.
- Election can be triggered at startup and after `NodeEvent.NodeDown(leader)`.

### Safety Invariants

1. `S1` - Election determinism  
   Given the same survivor set, all election invocations choose the same winner.

2. `S2` - Winner validity  
   `leader` is always either local node or one of configured peers, and never outside candidate set.

3. `S3` - Exclusion correctness  
   If an election is invoked with `exclude=X`, then `leader != X`.

4. `S4` - Callback branch safety  
   `elected` callback executes iff `winner == localNode`; otherwise `surrendered` executes.

5. `S5` - Split-view boundedness  
   Under divergent survivor views, selected leader must still be a valid member of the local survivor set.

### Liveness Invariants

1. `L1` - Startup convergence  
   After start, the process eventually computes and stores a non-null `leader`.

2. `L2` - Leader-down convergence  
   If current leader goes down and at least one survivor exists, process eventually converges to a new leader.

3. `L3` - Call path liveness  
   `handleCall` always completes with a reply path (leader or follower callback), no self-wait cycles.

### Concurrency Boundaries / Assumptions

- `GenLeaderServer` state transitions occur on single GenServer mailbox loop.
- Node down notifications arrive via `NodeMonitor` into `handleInfo`.
- System models local view election only; no distributed consensus or term/epoch synchronization.
- Correctness is evaluated under JVM coroutine scheduling (not BEAM process scheduling).

## DynamicSupervisorSyncHandshake

Target implementation:

- `otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/DynamicSupervisor.kt`

Algorithm summary:

- `startChildSync` creates child slot and a ready-deferred handshake, launches worker, and resolves caller via `StartChildSyncAwaitResult`.
- Epoch fencing plus `lastSyncStartFailedEpoch` prevents stale completion events from restarting failed sync-start children.

### Safety Invariants

1. `S1` - No stale-exit restart  
   `ChildExited` must affect only current slot epoch.

2. `S2` - Sync failure cleanup  
   If sync start fails before becoming stable, child slot must be removed and sync flags cleared.

3. `S3` - Shutdown propagation  
   Pending sync replies complete exceptionally when shutdown starts.

4. `S4` - Intensity enforcement  
   Supervisor cancels when restart intensity policy is exceeded (including supervisor-wide mode).

### Liveness Invariants

1. `L1` - Sync completion  
   `startChildSync` eventually returns success or failure (timeout/shutdown/exception), not infinite wait under bounded timeout.

2. `L2` - Coordinator progress  
   Coordinator still serves queries and start/terminate operations while sync waiters are in flight.

3. `L3` - Post-failure quiescence for temporary children  
   Failed temporary sync-start children eventually disappear from child map.

### Concurrency Boundaries / Assumptions

- Coordinator is single writer for `children` map and sync state flags.
- Sync waiters execute in separate coroutines and communicate back only through event channel.
- Child completion callbacks can race with sync waiter result delivery; epoch/failure markers are relied on for correctness.

## GenServerFairnessAndStarvation

Target implementation:

- `otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt`

Algorithm summary:

- GenServer loop prioritizes channels as: `sysMailbox` (highest), `controlMailbox` (drained before user message), then user mailbox.
- Hibernate path blocks on next user-mailbox message and intentionally does not drain sys/control during hibernation wake wait.

### Safety Invariants

1. `S1` - Serialized callback execution  
   Only one user message callback mutates state at a time.

2. `S2` - Sys operational safety  
   Sys operations can suspend/resume/inspect state without corrupting mailbox ordering.

3. `S3` - Queue accounting safety  
   User-mailbox queue length reflects user-message enqueue/dequeue path (with known exclusion of sys/control queues).

### Liveness Invariants

1. `L1` - Finite-control-flood progress  
   Under finite control pressure, user mailbox messages eventually execute.

2. `L2` - Call completion progress  
   Calls complete or fail (timeout/server-down), with no internal deadlock cycles.

3. `L3` - Hibernate wake progress  
   Hibernating server resumes once a user-mailbox message arrives; sys/control handling may be delayed until wake.

### Concurrency Boundaries / Assumptions

- Fairness is cooperative and depends on finite high-priority traffic.
- Sys/control starvation guarantees are not strict under infinite high-priority streams.
- Hibernate path intentionally trades responsiveness for reduced active work until wake.

## GlobalReplicationOrderingAndConflict

Target implementation:

- `otp-global/src/main/kotlin/org/otpstudy/global/GlobalRegistry.kt`
- `otp-distribution/src/main/kotlin/org/otpstudy/distribution/GlobalReplicationBus.kt`

Algorithm summary:

- Local registrations broadcast `Register`; unregistrations broadcast `Unregister`; peer connect triggers `SyncSnapshot`.
- Remote entries are maintained in per-node `NodeView.remote` maps.
- Per-name version tracking (`localVersions`/`observedVersions`) rejects stale register/snapshot replay after newer updates.
- Conflict resolution policies (`KeepFirst`, `KeepLast`, `Custom`) are applied when local and incoming names collide.

### Safety Invariants

1. `S1` - Local precedence consistency  
   For a given node view, policy determines deterministic local-vs-remote conflict outcome.

2. `S2` - Unregister safety  
   Once unregister is observed, name should not reappear from stale ordering artifacts.

3. `S3` - Snapshot monotonicity  
   Sync snapshots should not regress newer state (no stale reintroduction).

### Liveness Invariants

1. `L1` - Replication propagation  
   A registered name eventually becomes resolvable as remote on connected peers.

2. `L2` - Unregister propagation  
   Removed names eventually disappear from peers.

### Concurrency Boundaries / Assumptions

- Version checks are per-name and local to each node view; cluster-wide causal metadata/vector clocks are still absent.
- Strong convergence under partition/heal still depends on transport behavior and future stress validation.
