# Hazard Matrix

## GenLeader

| hazard_id | type | trigger condition | expected safe behavior | current coverage | confidence |
|---|---|---|---|---|---|
| `GL-R1` | Race | `NodeDown` arrives while election message is already queued | Eventual convergence to deterministic winner; no invalid leader values | `Covered` (new tests) | `Medium` |
| `GL-R2` | Race | Duplicate peers or repeated election triggers | Winner remains deterministic and stable for same survivor set | `Covered` (new tests) | `Medium` |
| `GL-D1` | Deadlock | `handleInfo` recasts election to self | No blocking self-call cycle; mailbox continues processing | `Covered` (structural + new test path) | `Medium` |
| `GL-S1` | Starvation | Frequent non-leader node events | Core calls still complete and leader state remains stable | `Partial` | `Low` |
| `GL-R3` | Race / split-brain model gap | Different nodes observe different survivor sets | Local process remains internally consistent; divergent leaders are bounded to local candidate sets | `Covered` (new split-view tests) | `Medium` |

## Follow-up Gaps

- Add sustained noisy-node event tests for stronger `GL-S1` confidence. ✅ (`GenLeaderTest`, `GenLeaderRobustnessContractTest`)
- If stronger cluster agreement is required, evaluate election epoch/term or consensus-style coordination.

## DynamicSupervisorSyncHandshake

| hazard_id | type | trigger condition | expected safe behavior | current coverage | confidence |
|---|---|---|---|---|---|
| `DS-R1` | Race | Child exits near sync waiter completion | No stale epoch restart; slot state remains consistent | `Covered` (existing + stress tests) | `Medium` |
| `DS-R2` | Race | Sync failure before ready with restart=Permanent | No infinite restart loop for failed sync start | `Covered` (existing tests) | `Medium` |
| `DS-R3` | Race | Ready callback invoked more than once | Sync call completes and child does not leak | `Covered` (new tests) | `Medium` |
| `DS-R5` | Race | Ready then immediate normal child exit during active sync handshake | No stale child accumulation; sync returns and temporary children drain to zero | `Covered` (new tests) | `High` |
| `DS-D1` | Deadlock | Multiple concurrent `startChildSync` operations | Coordinator remains responsive to queries/start requests | `Covered` (existing tests) | `Medium` |
| `DS-S1` | Starvation | Frequent sync starts + failures | Requests complete without unbounded pending sync queue | `Covered` (new long-run churn tests) | `Medium` |
| `DS-R4` | Race/Policy | Supervisor-wide intensity under dynamic restarts | Supervisor cancels when shared threshold exceeded | `Covered` (new tests) | `Medium` |

## Follow-up Gaps (DynamicSupervisor)

- Add deterministic race harness around the `yield(64)` reconciliation branch.
- Add metrics instrumentation for pending sync queue high-water marks under stress. ✅ (`DynamicSupervisor.metricsSnapshot`)

## GenServerFairnessAndStarvation

| hazard_id | type | trigger condition | expected safe behavior | current coverage | confidence |
|---|---|---|---|---|---|
| `GS-R1` | Race | Concurrent sys/control/user message arrivals | State remains serialized; no data race corruption | `Covered` (existing architecture + tests) | `Medium` |
| `GS-D1` | Deadlock | In-flight call while server exits or deferred reply paths | Call fails with timeout/server-down instead of hanging indefinitely | `Covered` (existing tests) | `Medium` |
| `GS-S1` | Starvation | Finite but heavy control flood before user work | User messages eventually process after high-priority drain | `Covered` (new test) | `Medium` |
| `GS-S2` | Starvation/latency | Hibernation while sys message arrives | Sys may be delayed until wake; no permanent stall once wake message arrives | `Covered` (new test) | `Medium` |
| `GS-S3` | Starvation | Infinite sys/control stream | User mailbox fairness not guaranteed by strict scheduler policy | `Missing (by design limitation)` | `Low` |

## Follow-up Gaps (GenServer)

- Add stress harness quantifying user-mailbox delay under sustained high-priority traffic.
- Consider fairness guardrails (e.g., bounded control-drain bursts) if strict starvation resistance is required.

## GlobalReplicationOrderingAndConflict

| hazard_id | type | trigger condition | expected safe behavior | current coverage | confidence |
|---|---|---|---|---|---|
| `GR-R1` | Race/ordering | `Register -> Unregister -> stale Register replay` | Name remains removed after unregister | `Covered (fixed + tests)` | `High` |
| `GR-R2` | Race/ordering | `Register -> Unregister -> stale SyncSnapshot` | Snapshot must not reintroduce removed name | `Covered (fixed + tests)` | `High` |
| `GR-R4` | Race/ordering | Mixed replay churn (`register/unregister/snapshot` interleavings) | Final state converges to highest-version event for each name | `Covered` (new tests) | `High` |
| `GR-R3` | Conflict race | Local/remote collisions under resolver policy | Policy should produce deterministic outcome | `Covered` (existing tests) | `Medium` |
| `GR-S1` | Starvation | Frequent sync/register churn | Eventual convergence without long-lived stale entries | `Partial` | `Low` |

## Follow-up Gaps (GlobalReplication)

- Version/tombstone safeguards are in place; add regression tests that include mixed transport reorder and replay sequences.
- Add true partition-heal stress suite (with transport-level delivery suppression) to evaluate convergence latency and stale-entry persistence.
- Add runtime counters for stale drops, snapshot processing, and sync broadcast latency. ✅ (`GlobalRegistry.metricsSnapshot`)
