# OTP Parity Matrix (Kotlin-OTP vs Erlang/OTP)

This matrix records observed behavior in Kotlin-OTP against OTP semantics and implementation
intent. Each row is linked to a claim in `cbr-claim-registry.md` via `claim_id`. The `belief`
column is the current CBR belief score for that claim; keep it in sync with the registry.

Legend: `Parity`, `Intentional Divergence`, `Mismatch`, `Unknown`.

## Mailbox and Selective Receive

- **Selective receive scan behavior**: `Intentional Divergence`
  - Kotlin-OTP implements explicit saved-list replay in `SelectiveMailbox`, with documented visible `O(n)` replay costs.
  - Evidence: `otp-mailbox/src/main/kotlin/org/otpstudy/mailbox/SelectiveMailbox.kt`.
  - OTP baseline: BEAM mailbox with scan pointer and selective receive optimizations in runtime internals.
  - `claim_id`: `mailbox.selective_receive.saved_list` | `belief`: 0.90
- **FIFO mailbox baseline**: `Parity`
  - Kotlin `ProcessMailbox` provides FIFO semantics over channel.
  - Evidence: `otp-mailbox/src/main/kotlin/org/otpstudy/mailbox/ProcessMailbox.kt`.
  - `claim_id`: `mailbox.fifo.semantics` | `belief`: 0.80
- **Selective receive behavior tests**: `Parity`
  - Non-matching messages are saved and reconsidered in order.
  - Evidence: `otp-mailbox/src/test/kotlin/org/otpstudy/mailbox/SelectiveMailboxTest.kt`.
  - `claim_id`: `mailbox.fifo.semantics` (test coverage of FIFO/saved-list contract) | `belief`: 0.80
- **Selective receive BEAM optimization parity**: `Unknown`
  - Whether the BEAM reference-mark scan optimization is matched or whether the gap is material.
  - `claim_id`: `mailbox.selective_receive.beam_optimisation_parity` | `belief`: 0.00

## GenServer Runtime

- **Sequential callback execution**: `Parity`
  - Single mailbox loop processes one message at a time.
  - Evidence: `otp-gen-server/src/main/kotlin/org/otpstudy/genserver/GenServer.kt`.
  - `claim_id`: `genserver.sequential_callback` | `belief`: 0.80
- **Synchronous call behavior on server death**: `Parity`
  - Caller receives `ServerDownException` while waiting for `call`.
  - Evidence: `GenServerRef.call` death watch in `GenServer.kt`.
  - OTP analog: `gen_server:do_call/4` DOWN handling.
  - `claim_id`: `genserver.call_on_death` | `belief`: 0.85
- **Async/deferred reply behavior**: `Parity`
  - Deferred call reply via `ReplyHandle`.
  - Evidence: `ReplyResult.DeferReply` and `handleCallFrom` in `GenServer.kt`.
  - `claim_id`: `genserver.async_deferred_reply` | `belief`: 0.80
- **Control/sys message prioritization**: `Intentional Divergence`
  - Kotlin introduces explicit high-priority sys/control channels before main mailbox.
  - Evidence: `drainControl`, `SysMsg` handling in `GenServer.kt`.
  - `claim_id`: `genserver.control_priority` | `belief`: 0.85
- **Hibernation semantics**: `Intentional Divergence`
  - Kotlin provides hibernate-like state release but blocks sys/control drain while hibernating.
  - Evidence: `NoreplyResult.Hibernate` application path in `GenServer.kt`.
  - `claim_id`: `genserver.hibernate` | `belief`: 0.75
- **Priority message semantics alignment with OTP 28+**: `Unknown`
  - Whether new OTP 28+ priority message semantics introduce a gap.
  - `claim_id`: `genserver.priority_otp28_alignment` | `belief`: 0.00
- **Quantitative fairness under reduction budgeting**: `Unknown`
  - Quantitative effects of reduction-budget fairness under sustained load not yet measured.
  - `claim_id`: `genserver.fairness_quantitative` | `belief`: 0.00

## Supervision and Restart Semantics

- **Restart policy parity (Permanent/Transient/Temporary)**: `Parity`
  - Covered in parity tests.
  - Evidence: `otp-supervisor/src/test/kotlin/org/otpstudy/supervisor/SupervisorOtpParityTest.kt`.
  - `claim_id`: `supervisor.restart_policies` | `belief`: 0.85
- **Restart intensity cutoff**: `Parity`
  - Supervisor cancels when intensity exceeded.
  - Evidence: `Supervisor.kt` coordinator logic and `SupervisorOtpParityTest.kt`.
  - `claim_id`: `supervisor.restart_intensity` | `belief`: 0.80
- **OneForAll/RestForOne ordering**: `Parity`
  - Reverse-order stop and suffix restarts are implemented.
  - Evidence: `otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/Supervisor.kt`.
  - `claim_id`: `supervisor.ordering` | `belief`: 0.75
- **Dynamic supervisor strategy extensions**: `Intentional Divergence`
  - Supports `OneForAll` and `RestForOne` for dynamic children beyond OTP `simple_one_for_one` baseline.
  - Evidence: `otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/DynamicSupervisor.kt`.
  - `claim_id`: `supervisor.dynamic_strategies` | `belief`: 0.80
- **Stress/race behavior under dynamic churn**: `Parity (for implemented model)`
  - Concurrency/epoch invalidation stress tests exist.
  - Evidence: `otp-supervisor/src/test/kotlin/org/otpstudy/supervisor/DynamicSupervisorStressTest.kt`.
  - `claim_id`: `sup.dynamic_handshake.safety` | `belief`: 0.85

## Distribution

- **Local node semantics**: `Parity`
  - Name registry lookup and local call/cast behavior are consistent with local-node abstractions.
  - Evidence: `otp-distribution/src/main/kotlin/org/otpstudy/distribution/LocalNode.kt`.
  - `claim_id`: `distribution.local_node` | `belief`: 0.70
- **Remote call/cast transport abstraction**: `Intentional Divergence`
  - `RemoteNodeStub.cast` swallows transport errors by design; `call` propagates failures.
  - Evidence: `otp-distribution/src/main/kotlin/org/otpstudy/distribution/RemoteNodeStub.kt`.
  - `claim_id`: `distribution.cast_swallow` | `belief`: 0.85
- **In-memory transport**: `Intentional Divergence`
  - Deterministic in-process transport for tests/education, not wire-protocol equivalence.
  - Evidence: `otp-distribution/src/main/kotlin/org/otpstudy/distribution/InMemoryTransport.kt`.
  - `claim_id`: `distribution.in_memory_transport` | `belief`: 0.90
- **Distribution ordering and failure semantics under partitions/reconnections**: `Unknown`
  - Behaviour under partition and reconnect scenarios not yet characterized.
  - `claim_id`: `distribution.partition_failure_semantics` | `belief`: 0.00

## Differential Checks vs Erlang Runtime

- **Counter behavior equivalence test**: `Partial Parity`
  - Basic call/cast state transition equivalence tested, including optional real Erlang node comparison.
  - Evidence: `otp-jinterface/src/test/kotlin/org/otpstudy/jinterface/BehavioralEquivalenceTest.kt`.
  - Gap: coverage limited to toy counter protocol.
  - `claim_id`: (contributes as DifferentialTest evidence to multiple genserver claims) | `belief`: n/a

## High-Priority Unknowns to Investigate Next

These rows have `belief = 0.00` in the registry and are priority targets for the next CBR iteration:

- **Selective receive optimization parity with BEAM reference-mark optimization**: `Unknown`.
  `claim_id`: `mailbox.selective_receive.beam_optimisation_parity`
- **Priority message semantics alignment with OTP 28+**: `Unknown`.
  `claim_id`: `genserver.priority_otp28_alignment`
- **Distribution ordering and failure semantics under partitions/reconnections**: `Unknown`.
  `claim_id`: `distribution.partition_failure_semantics`
- **Quantitative fairness effects of reduction budgeting under load**: `Unknown`.
  `claim_id`: `genserver.fairness_quantitative`
