# Kotlin-OTP Investigation Report

## What Was Evaluated

- Semantic fidelity versus Erlang/OTP design intent (mailbox, gen_server loop, supervision, distribution).
- Local baseline performance and efficiency under representative stress patterns.
- API ergonomics and operability for developer and runtime workflows.

Primary artifacts:

- `docs/investigation/parity-matrix.md`
- `docs/investigation/local-benchmark-results.md`
- `docs/investigation/ergonomics-audit.md`
- `samples/investigation/src/main/kotlin/org/otpstudy/investigation/InvestigationBenchmarks.kt`

## Executive Findings

1. **Architecture fidelity is strong for core OTP mental model**, especially gen_server lifecycle and supervisor policies.
2. **Deliberate JVM divergences are explicit and mostly well-documented**, notably selective receive implementation details and dynamic supervisor extensions.
3. **Performance profile is acceptable for educational/prototyping goals**, with clear pressure points:
   - selective receive depth sensitivity,
   - restart storm noise/overhead behavior,
   - limited memory instrumentation quality.
4. **Ergonomics are good but not yet hard-to-misuse**; the main risk is advanced behavior being easy to invoke without understanding costs.

## Parity Status Snapshot

- `Parity`: FIFO mailbox semantics, sequential GenServer loop, call failure semantics, static supervisor restart policies/intensity windows.
- `Intentional Divergence`: explicit saved-list selective receive, high-priority control/sys channels, extended dynamic supervisor strategies, in-memory distribution transport.
- `Partial Parity`: differential Erlang checks (currently narrow protocol coverage).
- `Unknown`: selective receive optimization parity to BEAM internals, OTP 28+ priority message alignment, distribution behavior under network fault models.

## Performance and Efficiency Summary (Local Baseline)

- `gen_server_call_roundtrip`: p50 ~13.96 us, p95 ~24.04 us.
- `distribution_in_memory_call`: p50 ~14.00 us, p95 ~22.21 us.
- `gen_server_cast_enqueue`: high enqueue throughput (~1.99M ops/s), but enqueue-only.
- `selective_receive_depth_1000 -> depth_10000`: marked degradation, consistent with O(n) saved-list scanning model.
- `supervisor_restart_storm_recovery`: stable child achieved after 40 crash cycles with low recovery time.
- `mailbox_cast_memory_delta`: coarse metric and inconclusive for allocator-level decisions.

## Ergonomics Assessment

- **Strengths**
  - Familiar OTP-style naming and callback structures reduce conceptual switching costs.
  - Rich source comments linking behavior to OTP internals improve traceability.
  - Recovery and lifecycle APIs are expressive and composable.
- **Weaknesses**
  - Some advanced behavior has high misuse potential (selective receive on hot paths, sync handshake complexity).
  - Runtime introspection is present but lacks standardized quantitative counters.
  - JVM coroutine semantics can surprise OTP-native expectations.

## Priority Backlog (Impact x Confidence)

1. **P1: Introduce runtime metrics APIs**
   - Add mailbox depth/scan stats, restart histograms, and call latency buckets.
   - Output via observer/recon-friendly structured snapshots.
2. **P1: Expand differential parity tests**
   - Add richer Erlang side-by-side suites beyond counter example: restart edge cases, delayed replies, mixed call/cast/info ordering.
3. **P1: Add adversarial benchmark variants**
   - High-contention callers, bounded mailbox overflow policies, cancellation races, supervisor-wide intensity storms.
4. **P2: Ergonomics hardening**
   - Add opinionated helper APIs and docs for common safe usage patterns.
   - Add warnings/checks for selective receive in high-throughput loops.
5. **P2: Memory profiling integration**
   - Replace coarse heap delta with profiler-grade allocation and retained-memory instrumentation.

## Confidence and Decision Guidance

- **High confidence**: qualitative parity mapping and architectural intent.
- **Medium confidence**: local performance baseline trends.
- **Low-to-medium confidence**: memory efficiency conclusions due to coarse measurement.

Recommendation:

- Proceed with Kotlin-OTP as an educational/experimentation vehicle for OTP concepts.
- Do not claim production-grade performance parity with BEAM until P1 investigations complete.
