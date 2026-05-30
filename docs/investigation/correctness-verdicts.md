# Correctness Verdicts

## GenLeader

- **Algorithm:** `dist.genleader`
- **Target:** `otp-distribution/src/main/kotlin/org/otpstudy/distribution/GenLeader.kt`
- **Verdict:** `Correct but fragile`
- **Confidence:** `Medium`

### Why

- Core safety invariants around deterministic winner selection, exclusion handling, and callback branch behavior hold under direct tests.
- Leader-down reelection converges in local model and avoids blocking cycles.
- New split-view tests show divergence is bounded: local leaders remain valid within each node's observed survivor set.
- Fragility remains because correctness is local-view only and does not provide consensus semantics across divergent views.

### Evidence

- Invariants and boundaries: `docs/investigation/correctness-spec.md#genleader`
- Hazards and coverage: `docs/investigation/hazard-matrix.md#genleader`
- Tests: `otp-distribution/src/test/kotlin/org/otpstudy/distribution/GenLeaderTest.kt`

### Assumptions

- Single-process mailbox serialization from GenServer runtime.
- Node monitor events represent local runtime view and may not reflect globally consistent membership ordering.

### Remediation / Next Actions

1. Add noisy-event starvation tests to raise liveness confidence. ✅ (`GenLeaderTest`, `GenLeaderRobustnessContractTest`)
2. Consider adding election epoch/term semantics if stronger cluster agreement is required.

## DynamicSupervisorSyncHandshake

- **Algorithm:** `sup.dynamic_handshake`
- **Target:** `otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/DynamicSupervisor.kt`
- **Verdict:** `Correct but fragile`
- **Confidence:** `Medium`

### Why

- Sync-start completion, timeout, shutdown propagation, and coordinator responsiveness are exercised with direct tests.
- New evidence confirms supervisor-wide intensity cancellation and no child leak on duplicate `ready` callback misuse.
- Additional adversarial tests now cover immediate-normal-exit races during active sync handshake and show no stale-child accumulation.
- New long-run mixed-outcome churn tests show bounded completion and no child leaks, improving starvation confidence.
- Remaining fragility is mostly around observability and branch-level determinism under extreme scheduler variance.

### Evidence

- Invariants and boundaries: `docs/investigation/correctness-spec.md#dynamicsupervisorsynchandshake`
- Hazards and coverage: `docs/investigation/hazard-matrix.md#dynamicsupervisorsynchandshake`
- Tests: `otp-supervisor/src/test/kotlin/org/otpstudy/supervisor/DynamicSupervisorTest.kt`

### Assumptions

- Coordinator is single-threaded event owner for child map/state.
- Coroutine scheduling remains fair enough for bounded reconciliation loops and waiter completion.

### Remediation / Next Actions

1. Add explicit instrumentation counters for pending sync waiters and reconciliation retries/high-water marks. ✅ (`DynamicSupervisor.metricsSnapshot`)
2. Add deterministic branch harnessing around reconciliation retries to reduce scheduler-dependent uncertainty.

## GenServerFairnessAndStarvation

- **Algorithm:** `runtime.genserver_fairness`
- **Target:** `otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt`
- **Verdict:** `Correct under assumptions`
- **Confidence:** `Medium`

### Why

- Message handling remains serialized and call paths provide bounded completion semantics under normal assumptions.
- New tests confirm user-mailbox progress under finite control flood and expected sys-delay behavior during hibernation with eventual recovery on wake.
- Remaining weakness is explicit: strict starvation resistance is not guaranteed under unbounded high-priority traffic.

### Evidence

- Invariants and boundaries: `docs/investigation/correctness-spec.md#genserverfairnessandstarvation`
- Hazards and coverage: `docs/investigation/hazard-matrix.md#genserverfairnessandstarvation`
- Tests: `otp-gen-server/src/test/kotlin/org/otpstudy/genserver/GenServerTest.kt`

### Assumptions

- High-priority traffic (sys/control) is finite or operationally bounded.
- Coroutine scheduler provides practical progress for pending user tasks over finite windows.

### Remediation / Next Actions

1. Add quantitative stress tests for worst-case user-delay envelopes.
2. Evaluate optional fairness budgeting across control/user drains for stricter starvation bounds.

## GlobalReplicationOrderingAndConflict

- **Algorithm:** `global.replication_ordering`
- **Target:** `otp-global/src/main/kotlin/org/otpstudy/global/GlobalRegistry.kt`
- **Verdict:** `Correct but fragile`
- **Confidence:** `High`

### Why

- Versioned register/unregister propagation now rejects stale replay and stale snapshot reintroduction paths.
- Adversarial ordering tests that previously demonstrated resurrection now pass with version guards.
- Mixed replay churn tests confirm final-state convergence to highest-version event for tested reorder/replay patterns.
- Remaining fragility is primarily in true partition-heal behavior and large-scale churn latency, not stale replay correctness.

### Evidence

- Invariants and boundaries: `docs/investigation/correctness-spec.md#globalreplicationorderingandconflict`
- Hazards and coverage: `docs/investigation/hazard-matrix.md#globalreplicationorderingandconflict`
- Tests: `otp-global/src/test/kotlin/org/otpstudy/global/GlobalRegistryTest.kt`

### Assumptions

- Transport may reorder, replay, or delay control messages relative to current registry state.
- No causal/version metadata exists to reject stale updates.

### Remediation / Next Actions

1. Add partition/rejoin simulations to validate convergence under reorder/replay at scale.
2. Add latency and churn stress metrics for eventual convergence confidence. ✅ (`GlobalRegistry.metricsSnapshot`)
