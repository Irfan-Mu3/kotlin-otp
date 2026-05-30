# OTP Modernization Exclusions

This register tracks intentionally excluded BEAM-era constraints so robustness modernization remains explicit.

| OTP constraint | Why it exists in Erlang/BEAM | Why not directly applicable here | Compensating guardrail |
|---|---|---|---|
| BEAM reduction-count preemption fairness | VM-level scheduler enforces periodic process preemption | Kotlin coroutines rely on cooperative suspension points and dispatcher policy | Stress/contract tests for finite-flood progress; optional fairness budgeting backlog item |
| Per-process heap + generational GC isolation | BEAM process memory isolation is core to fault containment and latency predictability | JVM heap and GC are shared across coroutines | mailbox bounds, actor arena option, and crash reporting hooks for backpressure/diagnostics |
| Built-in distributed term ordering semantics tied to BEAM external term format | OTP distribution stack assumes BEAM binary term encoding and ordering semantics | kotlin-otp wire and node runtime are different transport abstractions | explicit versioned replication metadata and conflict resolver policies |
| Transparent global consensus guarantees from distributed runtime primitives | BEAM ecosystem often layers stronger guarantees with mature cluster tooling | current implementation intentionally models local-view leader election | split-view boundedness tests and documented adapt classification for consensus-strength requirements |
| Strict one-to-one OTP `simple_one_for_one` strategy limitations | historical OTP API shape constrained strategy set for dynamic children | kotlin-otp intentionally expands strategy options for educational/runtime flexibility | explicit strategy docs + contract tests for OneForOne/OneForAll/RestForOne behaviors |
