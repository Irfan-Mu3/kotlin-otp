# CBR Claim Registry

Flat claim table for the Confidence-Based Review loop. Do not add sections — filter by
`components` or `depth` columns instead. Sort by `belief` ascending to find the next claim
to work on.

Refer to `cbr-confidence-model.md` for the evidence taxonomy, belief update rules, and
portfolio formula.

## Portfolio Confidence (current)

```
portfolio_confidence ≈ 0.70   (recomputed after iterations 8–9)
hard_gate_satisfied  = false  (all pairs have evidence; 2 depth-2 claims still below threshold after adversarial tests)
remediation_active   = none
active_mode          = Gather
```

## Claim Table

| id | components | depth | axis | sub_axis | status | belief | threshold | evidence_summary |
|---|---|---|---|---|---|---|---|---|
| `mailbox.fifo.semantics` | mailbox | 1 | Fidelity | Parity | Unverified | 0.80 | 0.95 | StructuralInspection/High: ProcessMailbox uses FIFO Channel semantics [ProcessMailbox.kt] → Confirmed; StructuralInspection/Medium: parity-matrix row "FIFO mailbox baseline: Parity" [cbr-parity-matrix.md] → Confirmed |
| `mailbox.selective_receive.saved_list` | mailbox | 1 | Fidelity | IntentionalDivergence | Unverified | 0.90 | 0.95 | StructuralInspection/High: explicit saved-list replay with O(n) scan documented [SelectiveMailbox.kt] → Confirmed; NarrativeRationale/High: parity-matrix "Intentional Divergence" with OTP baseline note [cbr-parity-matrix.md] → Confirmed; InvariantSpec/Medium: SelectiveMailboxTest non-matching messages saved and reconsidered in order [SelectiveMailboxTest.kt] → Confirmed |
| `mailbox.selective_receive.beam_optimisation_parity` | mailbox | 1 | Fidelity | Parity | Unverified | 0.00 | 0.95 | — |
| `genserver.sequential_callback` | genserver | 1 | Fidelity | Parity | Unverified | 0.80 | 0.95 | StructuralInspection/High: single mailbox loop processes one message at a time [GenServer.kt] → Confirmed; NarrativeRationale/Medium: parity-matrix "Sequential callback execution: Parity" [cbr-parity-matrix.md] → Confirmed |
| `genserver.call_on_death` | genserver | 1 | Fidelity | Parity | Unverified | 0.85 | 0.95 | StructuralInspection/High: caller receives ServerDownException via death watch in GenServerRef.call [GenServer.kt] → Confirmed; NarrativeRationale/Medium: OTP analog gen_server:do_call/4 DOWN handling cited [cbr-parity-matrix.md] → Confirmed; InvariantSpec/Medium: parity-matrix row "Synchronous call behavior on server death: Parity" → Confirmed |
| `genserver.async_deferred_reply` | genserver | 1 | Fidelity | Parity | Unverified | 0.80 | 0.95 | StructuralInspection/High: ReplyResult.DeferReply and handleCallFrom in GenServer.kt → Confirmed; NarrativeRationale/Medium: parity-matrix "Async/deferred reply behavior: Parity" [cbr-parity-matrix.md] → Confirmed |
| `genserver.control_priority` | genserver | 1 | Fidelity | IntentionalDivergence | Unverified | 0.85 | 0.95 | StructuralInspection/High: explicit high-priority sys/control channels before main mailbox via drainControl [GenServer.kt] → Confirmed; NarrativeRationale/Medium: parity-matrix "Control/sys message prioritization: Intentional Divergence" [cbr-parity-matrix.md] → Confirmed |
| `genserver.hibernate` | genserver | 1 | Fidelity | IntentionalDivergence | Unverified | 0.75 | 0.95 | StructuralInspection/Medium: hibernate-like state release but blocks sys/control drain while hibernating [GenServer.kt] → Confirmed; NarrativeRationale/Medium: parity-matrix "Hibernation semantics: Intentional Divergence" [cbr-parity-matrix.md] → Confirmed |
| `genserver.priority_otp28_alignment` | genserver | 1 | Fidelity | Parity | Unverified | 0.00 | 0.95 | — |
| `genserver.fairness_quantitative` | genserver | 1 | Fidelity | Parity | Unverified | 0.00 | 0.95 | — |
| `supervisor.restart_policies` | supervisor | 1 | Fidelity | Parity | Unverified | 0.85 | 0.95 | AdversarialTest/High: Permanent/Transient/Temporary parity tests pass [SupervisorOtpParityTest.kt] → Confirmed; NarrativeRationale/Medium: parity-matrix "Restart policy parity: Parity" [cbr-parity-matrix.md] → Confirmed |
| `supervisor.restart_intensity` | supervisor | 1 | Fidelity | Parity | Unverified | 0.80 | 0.95 | StructuralInspection/High: supervisor cancels when intensity exceeded [Supervisor.kt] → Confirmed; AdversarialTest/Medium: SupervisorOtpParityTest intensity cutoff coverage [SupervisorOtpParityTest.kt] → Confirmed |
| `supervisor.ordering` | supervisor | 1 | Fidelity | Parity | Unverified | 0.75 | 0.95 | StructuralInspection/High: reverse-order stop and suffix restarts implemented [Supervisor.kt] → Confirmed; NarrativeRationale/Medium: parity-matrix "OneForAll/RestForOne ordering: Parity" [cbr-parity-matrix.md] → Confirmed |
| `supervisor.dynamic_strategies` | supervisor | 1 | Fidelity | IntentionalDivergence | Unverified | 0.80 | 0.95 | StructuralInspection/High: OneForAll and RestForOne for dynamic children beyond OTP simple_one_for_one [DynamicSupervisor.kt] → Confirmed; NarrativeRationale/High: parity-matrix "Dynamic supervisor strategy extensions: Intentional Divergence" [cbr-parity-matrix.md] → Confirmed |
| `distribution.local_node` | distribution | 1 | Fidelity | Parity | Unverified | 0.70 | 0.95 | StructuralInspection/Medium: name registry lookup and local call/cast consistent with local-node abstractions [LocalNode.kt] → Confirmed; NarrativeRationale/Medium: parity-matrix "Local node semantics: Parity" [cbr-parity-matrix.md] → Confirmed |
| `distribution.cast_swallow` | distribution | 1 | Fidelity | IntentionalDivergence | Unverified | 0.85 | 0.95 | StructuralInspection/High: RemoteNodeStub.cast swallows transport errors by design; call propagates [RemoteNodeStub.kt] → Confirmed; NarrativeRationale/High: parity-matrix "Remote call/cast transport abstraction: Intentional Divergence" [cbr-parity-matrix.md] → Confirmed |
| `distribution.in_memory_transport` | distribution | 1 | Fidelity | IntentionalDivergence | Unverified | 0.90 | 0.95 | StructuralInspection/High: deterministic in-process transport for tests/education, not wire-protocol equivalence [InMemoryTransport.kt] → Confirmed; NarrativeRationale/High: parity-matrix "In-memory transport: Intentional Divergence" [cbr-parity-matrix.md] → Confirmed |
| `distribution.partition_failure_semantics` | distribution | 1 | Fidelity | Parity | Unverified | 0.00 | 0.95 | — |
| `dist.genleader.safety` | distribution | 1 | Correctness | Safety | Unverified | 0.85 | 0.95 | InvariantSpec/High: S1-S5 safety invariants specified [correctness-spec.md#genleader] → Confirmed; AdversarialTest/High: GenLeaderRobustnessContractTest split-view, exclusion, determinism stress [GenLeaderRobustnessContractTest.kt] → Confirmed; NarrativeRationale/High: verdict "Correct under assumptions" with high evidence strength [correctness-verdicts.md#genleader] → Confirmed |
| `dist.genleader.liveness` | distribution | 1 | Correctness | Liveness | Unverified | 0.80 | 0.95 | InvariantSpec/High: L1-L3 liveness invariants specified [correctness-spec.md#genleader] → Confirmed; AdversarialTest/High: startup convergence and leader-down convergence tested [GenLeaderRobustnessContractTest.kt] → Confirmed; NarrativeRationale/Medium: noisy-event starvation stress gap noted [cbr-state-ledger.md] → Inconclusive |
| `sup.dynamic_handshake.safety` | supervisor | 1 | Correctness | Safety | Unverified | 0.85 | 0.95 | InvariantSpec/High: SH-S1 to SH-S4 safety invariants specified [correctness-spec.md#dynamicsupervisorsynchandshake] → Confirmed; AdversarialTest/High: epoch fencing, stale completion, cancellation stress [DynamicSupervisorRobustnessContractTest.kt] → Confirmed; NarrativeRationale/High: verdict "Correct under assumptions" with high evidence strength [correctness-verdicts.md] → Confirmed |
| `runtime.genserver_fairness.liveness` | genserver | 1 | Correctness | Liveness | Unverified | 0.65 | 0.95 | InvariantSpec/Medium: fairness invariants specified [correctness-spec.md#genserverfairnessandstarvation] → Confirmed; AdversarialTest/Medium: finite-flood user progress stress tests [GenServerRobustnessContractTest.kt] → Confirmed; NarrativeRationale/Medium: medium evidence strength verdict; stress quantification gap noted [cbr-state-ledger.md] → Inconclusive |
| `global.replication_ordering.safety` | global | 1 | Correctness | Safety | Unverified | 0.85 | 0.95 | InvariantSpec/High: ordering and conflict resolution invariants specified [correctness-spec.md#globalreplicationorderingandconflict] → Confirmed; AdversarialTest/High: version guards, stale-drop, partition/heal stress [GlobalRobustnessContractTest.kt] → Confirmed; NarrativeRationale/High: verdict "Correct under assumptions" with high evidence strength [correctness-verdicts.md] → Confirmed |
| `genserver+supervisor.restart_deferred_reply` | genserver, supervisor | 2 | Fidelity | Parity | Unverified | 0.83 | 0.95 | NarrativeRationale/Low: ReplyHandle and ServerDownException both exist; interaction under restart not directly tested → Inconclusive; StructuralInspection/High: job.invokeOnCompletion death-watch in GenServerRef.call calls reply.completeExceptionally(ServerDownException) on actor death — CompletableDeferred.completeExceptionally is idempotent; if handle.reply() fired first, death-watch is a no-op; if actor dies first, caller receives ServerDownException [GenServer.kt L182-186] → Confirmed; StructuralInspection/High: supervisor restart creates fresh Job and fresh GenServerRef via startWorker(); old ReplyHandle's deferred is already completed (either via reply or death-watch) before restart; no shared state between old and new child instances [Supervisor.kt L291-318] → Confirmed; AdversarialTest/High (×3): single-crash caller receives ServerDownException, 20 concurrent deferred callers all ServerDownException, supervisor-restarted child responds to Ping [DeferredReplyUnderRestartContractTest.kt] → Confirmed (all pass). |
| `genserver+mailbox.control_priority_under_shutdown` | genserver, mailbox | 2 | Fidelity | Parity | Unverified | 0.86 | 0.95 | NarrativeRationale/Low: sys/control drain and cooperative shutdown both implemented; interaction under active drain not directly tested → Inconclusive; StructuralInspection/High: drainControl() uses controlMailbox.tryReceive().getOrNull() — non-blocking; exits as soon as channel is empty; never suspends; cannot block cancellation [GenServer.kt L462-481] → Confirmed; StructuralInspection/High: run loop checks currentCoroutineContext().isActive at top of outer@ while loop on every iteration; supervisor job.cancel() delivers CancellationException at the next suspension point (select block); loop exits via catch(ce: CancellationException) which calls terminate() and rethrows [GenServer.kt L536, L568-587, L645-650] → Confirmed; AdversarialTest/High (×4): 10k control-flood stop() < 2s, concurrent flood scope cancel < 3s, sys+control flood stop() < 2s, user call completes despite prior 5k control flood [ControlPriorityUnderShutdownContractTest.kt] → Confirmed (all pass). |
| `mailbox+supervisor.selective_receive_during_restart` | mailbox, supervisor | 2 | Fidelity | Parity | Exempted | 0.20 | 0.95 | StructuralInspection/High: SelectiveMailbox is a standalone Channel wrapper with a local saved ArrayDeque; the Supervisor never references or interacts with SelectiveMailbox instances [SelectiveMailbox.kt, Supervisor.kt] → Confirmed; StructuralInspection/High: on restart, the supervisor cancels the child Job and starts a new one via startWorker(); the old SelectiveMailbox and its saved deque are GC'd with the old child scope; the new child receives a fresh instance — no shared state between restart logic and mailbox state [Supervisor.kt L291-318] → Confirmed. Rationale: property holds by structural isolation; no coupling path exists between Supervisor and SelectiveMailbox. |
| `supervisor+distribution.child_restart_on_remote_node_failure` | distribution, supervisor | 2 | Fidelity | Parity | Exempted | 0.20 | 0.95 | StructuralInspection/High: Supervisor coordinator loop accepts only ChildExited and RequestShutdown events; NodeMonitor.notifyDown sends NodeEvent.NodeDown only to GenServerRefs registered as watchers, never to a SupervisorRef or supervisor event channel [NodeMonitor.kt, Supervisor.kt] → Confirmed; StructuralInspection/High: a child that crashes due to a remote failure throws locally (non-CancellationException), which maps to ExitKind.Failure — normal restart policy applies; the supervisor has no remote-node awareness [Supervisor.kt L309-317] → Confirmed; NarrativeRationale/High: DistributedChildSpec "not yet implemented" comment confirms no DistributedSupervisor coupling currently exists [DistributedChildSpec.kt] → Confirmed. Rationale: property holds by isolation; re-open when DistributedSupervisor is implemented. |
| `global+distribution.registry_convergence_after_leader_change` | distribution, global | 2 | Fidelity | Parity | Unverified | 0.27 | 0.95 | AdversarialTest/Medium: GlobalRobustnessContractTest partition/rejoin convergence [GlobalRobustnessContractTest.kt] → Inconclusive (convergence under leader change specifically not isolated); StructuralInspection/High: GlobalRegistry uses versioned replication (monotonic version counters per name) and stale-drop in onDistMessage; version guards prevent stale resurrection [GlobalRegistry.kt L228-270] → Confirmed; StructuralInspection/High: GenLeader leader changes do not directly notify GlobalRegistry — the two are decoupled at library level; convergence after leader change depends on application-level elected/surrendered callback wiring and GlobalReplicationBus.onPeerConnected→syncPeers() [GenLeader.kt, GlobalRegistry.kt L81-87] → Inconclusive (property holds for correct applications but library provides no automatic guarantee). Gap: no adversarial test specifically exercises registry state during a GenLeader term change. |
| `mailbox+genserver.selective_receive_under_high_load` | genserver, mailbox | 2 | Correctness | Liveness | Unverified | 0.25 | 0.95 | NarrativeRationale/Low: SelectiveMailbox O(n) scan depth sensitivity measured; interaction with sys/control drain under load not directly tested → Inconclusive |
| `genserver+distribution.call_semantics_across_node_boundary` | distribution, genserver | 2 | Fidelity | Parity | Unverified | 0.37 | 0.95 | StructuralInspection/Low: RemoteNodeStub.call propagates failures; GenServerRef.call failure semantics exist; cross-boundary composition not directly tested → Inconclusive; StructuralInspection/High: RemoteNodeStub.call delegates to transport.call(id, name, request, timeout) — transport errors propagate directly to caller [RemoteNodeStub.kt L28-29] → Confirmed; StructuralInspection/High: local GenServerRef.call uses withTimeout + CompletableDeferred + death-watch; remote path uses RemoteNodeStub.call + transport — failure modes are structurally distinct (TimeoutCancellationException vs transport error vs ServerDownException) with no conflation path [GenServer.kt L164-193, RemoteNodeStub.kt] → Confirmed. Gap: no adversarial test exercises timeout/transport-error conflation under concurrent call load. |
| `genserver+supervisor+distribution.crash_containment_under_partition` | distribution, genserver, supervisor | 3 | Correctness | Safety | Exempted | 0.10 | 0.95 | StructuralInspection/High: Supervisor coordinator processes only ChildExited and RequestShutdown events; NodeEvent.NodeDown is never routed to the supervisor — only to GenServers that explicitly call NodeMonitor.monitorNode [Supervisor.kt, NodeMonitor.kt] → Confirmed (isolation by design); StructuralInspection/High: DistributedChildSpec exists but DistributedSupervisor is not yet implemented ("not yet implemented" comment in DistributedChildSpec.kt); no distribution-supervisor coupling path exists → Exempted (property holds trivially by isolation, not by verified active containment). Rationale: claim assumes active distribution-supervisor coupling that does not yet exist in the codebase. Re-open when DistributedSupervisor is implemented. |

---

## Unknown Backlog (not yet opened as claims)

These items from `cbr-parity-matrix.md` and `otp-robustness-gap-catalog.md` are registered
as future claim candidates. Open them as claims when the scheduler selects them or when the
99% hard gate requires them:

- `genserver.priority_otp28_alignment` — OTP 28+ priority message semantics alignment
- `mailbox.selective_receive.beam_optimisation_parity` — BEAM reference-mark optimisation
- `distribution.partition_failure_semantics` — distribution ordering/failure under reconnection
- `genserver.fairness_quantitative` — quantitative reduction-budget fairness under load
- `global+distribution.partition_heal_latency` — convergence latency under transport-suppressed partition/heal
- `supervisor+distribution.long_run_sync_churn` — long-run sync-churn stability
- `dist.genleader.epoch_term_extension` — epoch/term extension for cluster-wide agreement

---

## Hard Gate Status

| Component pair | Depth-2+ claim | Status |
|---|---|---|
| genserver + mailbox | `genserver+mailbox.control_priority_under_shutdown`, `mailbox+genserver.selective_receive_under_high_load` | Unverified |
| genserver + supervisor | `genserver+supervisor.restart_deferred_reply` | Unverified |
| genserver + distribution | `genserver+distribution.call_semantics_across_node_boundary` | Unverified |
| supervisor + distribution | `supervisor+distribution.child_restart_on_remote_node_failure` | Unverified |
| global + distribution | `global+distribution.registry_convergence_after_leader_change` | Unverified |
| mailbox + supervisor | `mailbox+supervisor.selective_receive_during_restart` | Unverified |

Hard gate: **NOT satisfied** (2 of 6 pairs Exempted ✓; 4 pairs Unverified — 2 claims now at 0.83–0.86 after adversarial tests, 2 still below 0.50; all need belief ≥ 0.95 or Exempted).

Progress per pair:

| Component pair | Depth-2+ claim | Pair status |
|---|---|---|
| genserver + mailbox | `genserver+mailbox.control_priority_under_shutdown` (0.86), `mailbox+genserver.selective_receive_under_high_load` (0.25) | Open — control_priority_under_shutdown AdversarialTest/High (×4) confirmed; high_load claim still Inconclusive |
| genserver + supervisor | `genserver+supervisor.restart_deferred_reply` (0.83) | Open — AdversarialTest/High (×3) confirmed; approaching threshold |
| genserver + distribution | `genserver+distribution.call_semantics_across_node_boundary` (0.37) | Open — Confirmed inspections, below threshold |
| supervisor + distribution | `supervisor+distribution.child_restart_on_remote_node_failure` (0.20) | **Exempted** ✓ |
| global + distribution | `global+distribution.registry_convergence_after_leader_change` (0.27) | Open — Inconclusive; adversarial test needed |
| mailbox + supervisor | `mailbox+supervisor.selective_receive_during_restart` (0.20) | **Exempted** ✓ |
