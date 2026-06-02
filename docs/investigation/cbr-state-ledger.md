# CBR State Ledger

This ledger is the persistent state model for the Confidence-Based Review (CBR) loop.
It replaces the former `state-ledger.md` (A*-style correctness loop).

The per-algorithm rows below are retained as historical evidence records. They feed into
claim `evidence_summary` entries in `cbr-claim-registry.md`.

---

## Portfolio Confidence (CBR)

```
portfolio_confidence  ≈ 0.85   (recomputed after iteration 17 adversarial tests)
hard_gate_satisfied   = false  (2 of 6 pairs Exempted; all 4 Unverified claims have adversarial test evidence; none yet at 0.95)
active_mode           = Gather
remediation_claim     = none
last_updated          = iteration 17 (PartitionFailureSemanticsContractTest)
```

Recompute using the formula in `cbr-confidence-model.md` after each iteration.
Do not update while `active_mode = Remediation`.

---

## CBR Iteration Log

| iteration | claim_id | evidence_type | outcome | belief_before | belief_after | notes |
|---|---|---|---|---|---|---|
| 1 | `genserver+supervisor+distribution.crash_containment_under_partition` | StructuralInspection/High (×2) | Exempted | 0.10 | 0.10 (Exempted — counts at 50%) | Supervisor has no distribution coupling; DistributedSupervisor not yet implemented. Property holds by isolation. Re-open when DistributedSupervisor is built. |
| 2 | `mailbox+supervisor.selective_receive_during_restart` | StructuralInspection/High (×2) | Exempted | 0.20 | 0.20 (Exempted — counts at 50%) | SelectiveMailbox is a local Channel wrapper with no supervisor coupling. On restart, old instance GC'd with child scope; new child gets fresh instance. Holds by structural isolation. |
| 3 | `supervisor+distribution.child_restart_on_remote_node_failure` | StructuralInspection/High (×3) + NarrativeRationale/High | Exempted | 0.20 | 0.20 (Exempted — counts at 50%) | Supervisor has no remote-node awareness; NodeEvent.NodeDown never reaches supervisor. Remote failure appears as ExitKind.Failure; normal restart policy applies. DistributedSupervisor not yet implemented. |
| 4 | `global+distribution.registry_convergence_after_leader_change` | StructuralInspection/High (×2) + AdversarialTest/Medium (carry-over) | Inconclusive | 0.20 | 0.27 | GlobalRegistry and GenLeader are decoupled at library level. Version guards work; convergence after leader change requires application-level wiring. Gap: no adversarial test isolates leader-change convergence scenario. |
| 5 | `genserver+distribution.call_semantics_across_node_boundary` | StructuralInspection/High (×2) + StructuralInspection/Low (carry-over) | Confirmed | 0.20 | 0.37 | Failure modes are structurally distinct; no conflation path found. Gap: no adversarial test for concurrent call load under timeout/transport-error mix. |
| 6 | `genserver+supervisor.restart_deferred_reply` | StructuralInspection/High (×2) | Confirmed | 0.30 | 0.59 | Death-watch + idempotent CompletableDeferred correctly handles deferred reply under restart. Supervisor creates fresh Job/Ref on restart; old ReplyHandle already completed. Gap: no concurrent stress test. |
| 7 | `genserver+mailbox.control_priority_under_shutdown` | StructuralInspection/High (×2) | Confirmed | 0.25 | 0.57 | drainControl() is non-blocking (tryReceive); cannot block CancellationException delivery. Loop exits on every iteration via isActive check or select cancellation. Gap: no rapid-flood concurrent stress test. |
| 8 | `genserver+supervisor.restart_deferred_reply` | AdversarialTest/High (×3) | Confirmed | 0.59 | 0.83 | Tests: single-crash deferred caller, 20 concurrent deferred callers, supervisor-restarted child Ping. All pass. Death-watch race-free under load confirmed. [DeferredReplyUnderRestartContractTest.kt] |
| 9 | `genserver+mailbox.control_priority_under_shutdown` | AdversarialTest/High (×4) | Confirmed | 0.57 | 0.86 | Tests: 10k flood stop() < 2s, concurrent flood scope cancel < 3s, sys+control flood stop() < 2s, call completes despite 5k flood. All pass. drainControl() non-blocking + cancellation delivery confirmed under stress. [ControlPriorityUnderShutdownContractTest.kt] |
| 10 | `global+distribution.registry_convergence_after_leader_change` | AdversarialTest/High (×4) | Confirmed | 0.27 | 0.65 | Tests: names survive leader change, stale replays during election dropped (version guard), leaderless-window names replicate via syncPeers on reconnect, 10 rapid term-change cycles + stale-replay storm produce correct final state. All 4 pass. [RegistryConvergenceAfterLeaderChangeContractTest.kt] |
| 11 | `genserver+distribution.call_semantics_across_node_boundary` | AdversarialTest/High (×6) | Confirmed | 0.37 | 0.64 | Tests: happy-path reply, unknown-node → IllegalStateException, timeout → CancellationException (not plain IllegalStateException), 50+50 concurrent no cross-contamination, ghost cast silently swallowed, 20+20 mixed load correctly separated. All 6 pass in 1s. Key fix: GenServer.startLink(scope) not runBlocking-this to avoid hang; JVM CancellationException→IllegalStateException hierarchy documented. [CrossNodeCallSemanticsContractTest.kt] |
| 12 | `mailbox+genserver.selective_receive_under_high_load` | AdversarialTest/High (×4) | Confirmed | 0.25 | 0.63 | Tests: (1) 5k noise in saved list → receive matches within 5s; (2) concurrent 5k noise flood before match → waiting receive unblocked; (3) GenServer handler blocking on SelectiveMailbox.receive under 1k cast flood → handler resumes and call replies; (4) mark/receiveFrom with 10k pre-mark entries → median within 20× of zero-noise baseline (O(1) skip confirmed). Key structural finding: SelectiveMailbox suspends the handler coroutine, blocking the run loop; liveness depends on the matching message eventually arriving; n in O(n) is bounded by non-matching accumulation, not overall mailbox throughput. All 4 pass. [SelectiveReceiveUnderHighLoadContractTest.kt] |
| 13 | `global+distribution.registry_convergence_after_leader_change` | AdversarialTest/High (×4) | Confirmed | 0.65 | 0.84 | Tests: (5) KeepLast conflict resolver — higher-versioned remote entry wins on all views; (6) three-epoch leader chain A→B→C — names from all three epochs visible from nodeC; (7) unregister-before-reconnect — version-2 Unregister beats stale version-1 SyncSnapshot, stale drop counter incremented; (8) dead GenServer auto-unregister — invokeOnCompletion hook fires unregisterName, Unregister propagates to peer, name absent from remote view. All 4 pass. [RegistryConvergenceAfterLeaderChangeContractTest.kt] Key: applyRemoteRegister skips home node (self-loop silent); epoch3 local registration uses local path, not handler. |
| 14 | `runtime.genserver_fairness.liveness` | AdversarialTest/High (×4) | Confirmed | 0.65 | 0.84 | Tests: (L1-10k) user progress after 10k control flood confirmed; (L1-quant) user-message latency < 2s under 5k control flood; (L2-50c) 50 concurrent calls all complete without deadlock; (L2-mix) slow call times out as CancellationException, concurrent fast call succeeds simultaneously — no cross-deadlock. All 4 pass. [GenServerFairnessLivenessContractTest.kt] Key: drainControl() is non-blocking so even 10k control ticks do not permanently starve the user mailbox select. |
| 15 | `genserver+distribution.call_semantics_across_node_boundary` | AdversarialTest/High (×4) | Confirmed | 0.64 | 0.78 | Tests: (T6) abrupt job.cancel() mid-call → ServerDownException (not timeout/transport error); (T7) node removed during in-flight call → call terminates (not hang); (T8) re-added node after disconnect → subsequent calls succeed; (T9) 100 mixed concurrent ops (25 happy/unknown/timeout/cast) → all outcome classes correctly separated. All 4 pass. [CrossNodeCallSemanticsContractTest.kt] Key: death-watch fires immediately on job.cancel(); stop() is sequential in the mailbox and reaches the server after the slow reply. |
| 16 | `mailbox+genserver.selective_receive_under_high_load` | AdversarialTest/High (×3) | Confirmed | 0.63 | 0.78 | Tests: (T5) saved-list size bounded exactly by noise count per session (10 sessions × 200 noise, no inflation); (T6) flushSaved under load → all 500 noise messages returned to channel in FIFO order, broad-match receive drains cleanly; (T7) two independent mailboxes concurrently live under separate 3k-noise floods — no cross-contamination, savedSizes exact. All 3 pass. [SelectiveReceiveUnderHighLoadContractTest.kt] |
| 17 | `distribution.partition_failure_semantics` | AdversarialTest/High (×4) | Confirmed | 0.00 | 0.63 | Tests: (1) multiCall under partition classifies reachable reply vs noReply node correctly; (2) call fails with transport IllegalStateException during partition and succeeds after rejoin; (3) cast during partition is swallowed and not replayed after rejoin; (4) abcast is best-effort (partitioned node dropped) and resumes delivery after rejoin. All 4 pass. [PartitionFailureSemanticsContractTest.kt] |

---

## Prior Correctness Loop — Scoring Model (retained for reference)

The prior loop used `f = g + h` as a priority heuristic. These scores are not CBR belief
values; they are investigation cost estimates from the correctness campaign. They serve as
`NarrativeRationale` or `ExpertReview` evidence for the corresponding CBR claims.

- `g` = observed investigation cost units
- `h` = estimated uncertainty remaining
- `h = 3*UnprovenSafety + 3*UnprovenLiveness + 2*ConcurrencyComplexity + 2*Nondeterminism + 2*CoverageGap - 2*HarnessMaturity`

### Cost Units (`g`)

- `+1` invariant/spec update
- `+2` hazard matrix update
- `+3` adversarial test added
- `+1` deterministic trace/instrumentation addition
- `+2` flaky stabilization pass

---

## Prior Correctness Loop — Ledger Table

| algorithm_id | status | g_cost | h_score | f_score | evidence_strength | next_best_action | cbr_claim_ids |
|---|---:|---:|---:|---:|---|---|---|
| `dist.genleader` | `done` | 18 | 4 | 22 | `high` | Add noisy-event starvation stress and rejoin stability checks | `dist.genleader.safety`, `dist.genleader.liveness` |
| `sup.dynamic_handshake` | `done` | 26 | 3 | 29 | `high` | Add instrumentation for pending sync-reply queue high-water marks and reconciliation retry counters | `sup.dynamic_handshake.safety` |
| `runtime.genserver_fairness` | `done` | 13 | 6 | 19 | `medium` | Add stress quantification for user-delay under sustained control/sys pressure | `runtime.genserver_fairness.liveness` |
| `global.replication_ordering` | `done` | 28 | 2 | 30 | `high` | Add true partition-heal stress (transport-level suppression) and convergence-latency metrics | `global.replication_ordering.safety` |

Evidence links: `correctness-spec.md`, `hazard-matrix.md`, `correctness-verdicts.md`.

---

## Notes

- `dist.genleader` was selected for the first correctness loop because of high correctness impact and low existing direct test coverage.
- Prior scores are coarse-grained; they are superseded by CBR belief scores in `cbr-claim-registry.md`.

---

## Prior Correctness Loop — Queue History (archived)

Retained as audit trail only. CBR scheduling is driven by belief scores in `cbr-claim-registry.md`.

### Post first loop
1. `sup.dynamic_handshake` (`f=20`)
2. `runtime.genserver_fairness` (`f=22`)
3. `global.replication_ordering` (`f=24`)

### Post second loop
1. `runtime.genserver_fairness` (`f=20`)
2. `global.replication_ordering` (`f=22`)

### Post third loop
1. `global.replication_ordering` (`f=20`)

### Post fourth loop (seed queue exhausted)
1. `global.replication_ordering` remediation (versioning/tombstones)
2. `sup.dynamic_handshake` reconciliation-race determinism
3. `dist.genleader` split-view behavior bounds

### Post global remediation
1. `dist.genleader` split-view behavior bounds
2. `global.replication_ordering` partition/churn convergence stress
3. `sup.dynamic_handshake` long-run sync-churn stress

### Post dynamic reconciliation loop
1. `dist.genleader` split-view behavior bounds
2. `global.replication_ordering` partition/churn convergence stress
3. `sup.dynamic_handshake` long-run sync-churn stress

### Post GenLeader split-view loop
1. `global.replication_ordering` partition/churn convergence stress
2. `sup.dynamic_handshake` long-run sync-churn stress
3. `dist.genleader` noisy-event starvation stress

### Post global churn loop
1. `sup.dynamic_handshake` long-run sync-churn stress
2. `dist.genleader` noisy-event starvation stress
3. `global.replication_ordering` true partition-heal stress + latency metrics

### Post dynamic long-run churn loop
1. `dist.genleader` noisy-event starvation stress
2. `global.replication_ordering` true partition-heal stress + latency metrics
3. `sup.dynamic_handshake` instrumentation (queue high-water + retry counters)

### Post modernized robustness sprint
Completed:
1. `dist.genleader` noisy-event starvation stress ✅
2. `global.replication_ordering` sync/stale-drop instrumentation + contract coverage ✅
3. `sup.dynamic_handshake` pending-sync/reconciliation/restart metrics ✅

Remaining (now tracked as CBR Unknown backlog items):
1. `global.replication_ordering` transport-suppressed partition/heal stress harness
2. `runtime.genserver_fairness` optional control-drain fairness budgeting
3. `dist.genleader` epoch/term extension (if cluster-wide agreement required)
