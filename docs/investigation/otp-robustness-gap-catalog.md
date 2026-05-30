# OTP Robustness Gap Catalog

Primary benchmark: Erlang/OTP safety, liveness, failure containment, and convergence semantics.

Legend:
- modernization classification: `adopt` | `adapt` | `reject`
- severity: `low` | `medium` | `high` | `critical`
- confidence: `low` | `medium` | `high`

| OTP guarantee | Kotlin current behavior | gap severity | modernization classification | evidence confidence |
|---|---|---|---|---|
| Deterministic local leader choice for same survivor set | `GenLeader` picks lexicographically greatest candidate and preserves local validity under split views | medium | adopt | high |
| Cluster-wide leader agreement under diverging membership views | `GenLeader` is intentionally local-view only (no epoch/term consensus) | high | adapt | medium |
| Supervisor sync-start either returns ready value or fails without stale child leakage | `DynamicSupervisor` fences epochs and clears failed sync starts | medium | adopt | high |
| Supervisor fairness under sync churn (bounded waiter pressure) | completion is robust but high-water/pressure metrics were missing; metrics now exposed | medium | adapt | medium |
| Finite high-priority traffic should not permanently starve user mailbox | `GenServer` drains sys/control first but user work progresses under finite floods | medium | adopt | medium |
| Strict starvation resistance under unbounded sys/control flood | no strict scheduler budget across channels; user starvation remains possible by design | high | adapt | medium |
| Unregister should dominate stale register/snapshot replay | `GlobalRegistry` version guards prevent stale resurrection | medium | adopt | high |
| Partition/rejoin convergence should be observable and bounded | convergence semantics improved; runtime metrics for stale drops/sync latency now exposed | high | adapt | medium |
| BEAM preemptive reductions and process GC semantics | coroutine runtime differs fundamentally from BEAM VM execution model | low | reject | high |

## Evidence Sources

- `docs/investigation/correctness-spec.md`
- `docs/investigation/hazard-matrix.md`
- `docs/investigation/correctness-verdicts.md`
- `docs/investigation/state-ledger.md`
- Contract suites:
  - `otp-distribution/src/test/kotlin/org/otpstudy/distribution/GenLeaderRobustnessContractTest.kt`
  - `otp-global/src/test/kotlin/org/otpstudy/global/GlobalRobustnessContractTest.kt`
  - `otp-supervisor/src/test/kotlin/org/otpstudy/supervisor/DynamicSupervisorRobustnessContractTest.kt`
  - `otp-gen-server/src/test/kotlin/org/otpstudy/genserver/GenServerRobustnessContractTest.kt`
