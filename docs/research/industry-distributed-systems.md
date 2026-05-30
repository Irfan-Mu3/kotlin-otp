# Industry Research: Distributed Systems Design in Practice

**Date:** May 2026
**Context:** This document informs the kotlin-otp project's positioning and next evolution. It answers three questions the project must resolve before claiming practical relevance: how engineers actually design distributed systems in industry, what patterns emerge from library usage and what invariants must be robust vs extensible, and which performance requirements are tied to which patterns.

Cross-references to kotlin-otp investigation findings are marked with `→ internal:` throughout.

---

## Table of Contents

1. [How Industry Actually Designs Distributed Applications](#1-how-industry-actually-designs-distributed-applications)
2. [Library Patterns: Robust vs Malleable](#2-library-patterns-robust-vs-malleable)
3. [Performance Requirements by Pattern](#3-performance-requirements-by-pattern)
4. [Implications for kotlin-otp](#4-implications-for-kotlin-otp)
5. [Sources](#5-sources)

---

## 1. How Industry Actually Designs Distributed Applications

### 1.1 The dominant model: orchestrator-first, stateless services

The CNCF Annual Survey (2024) places Kubernetes at approximately 80% production adoption across surveyed organizations. The mainstream approach to distribution in industry is not actor models, message-passing runtimes, or supervision trees — it is:

- **Stateless services** behind load balancers, scaled horizontally by the orchestrator
- **REST or gRPC** for inter-service communication
- **Managed databases** (RDS, Spanner, DynamoDB, Redis) for all persistence
- **Service meshes** (Istio, Linkerd, Envoy) for retries, circuit breaking, traffic shaping, and mTLS at the network layer
- **K8s itself** for health checks, restarts, rolling deployments, and placement

This model solves distribution by **eliminating stateful runtime entities entirely**. There are no actors, no mailboxes, no supervision trees. Any state that must survive a process restart lives in an external store. The operational tradeoffs are: latency to the store on every stateful operation, correctness complexity pushed into the database (transactions, locking), and significant infrastructure overhead (the mesh, the managed DB tier, the K8s control plane).

This is not a deficiency in how engineers think — it is a deliberate choice to pay operational complexity at the infrastructure layer rather than the application layer. For a large class of CRUD-style workloads, this is the correct tradeoff.

**Where this breaks down:** The stateless-plus-external-store model fails to be economical when:

- State access patterns are too hot or too fine-grained for a shared store (e.g., per-user session objects updated on every message, per-entity game state updated 60 times per second)
- Failure isolation between entities is required (one user's session crashing must not affect others)
- The cost of an external round-trip is unacceptable for the latency SLA (sub-millisecond state reads)
- The number of concurrent entities exceeds what relational transactions can isolate cheaply (millions of concurrent guild objects, millions of active sessions)

These are precisely the conditions under which actor models were invented, and they are exactly the conditions faced by the companies in Camp B below.

### 1.2 The high-scale niche: stateful actor and entity models

A smaller but high-signal group of companies has adopted actor-style concurrency because their domain demands stateful, concurrent entities with strong isolation guarantees. These are not outliers — they are the companies building the highest-reliability, highest-scale systems in the industry:

**Discord** (Elixir/OTP): Each Discord guild (server) is a GenServer holding its state in process memory. At peak, this means millions of concurrent Elixir processes. The key OTP property exploited is not throughput — it is **fault isolation**: one guild's GenServer crashing is caught by its supervisor and restarted in isolation, without affecting any other guild. Discord's 2020 migration away from Go for their read states service (to Rust) explicitly *kept* Elixir as the backbone for stateful guild management, because the BEAM process model was the right fit.

**WhatsApp** (Erlang/OTP): Serves approximately 100 million daily active users with a remarkably small engineering team (estimated ~50 engineers at the time of the Facebook acquisition). The consistent framing in their public talks is reliability-per-engineer and failure isolation, not raw throughput. Each conversation thread is managed as an OTP process.

**Klarna** (Erlang): Fintech infrastructure where reliability and fault isolation are regulatory requirements, not nice-to-haves. Erlang's per-process GC and supervisor restart semantics make it straightforward to reason about crash recovery.

**Bet365**: Reported to handle approximately 100 million concurrent users at sports event peaks with Erlang/OTP throughout their core platform.

**Riot Games / VALORANT** (C++ with actor-inspired patterns): Published a 16.67 ms per-player frame budget for game servers. While not using a traditional actor framework, their server architecture uses per-entity update loops that map structurally to the actor model's serial-execution-per-entity property.

**The Ericsson origin**: OTP was designed for telecom switches where nine-nines availability (99.9999999%) was a contractual requirement and field upgrades without downtime were non-negotiable. The distribution primitives, supervision trees, and hot code loading are direct responses to those constraints.

### 1.3 The virtual actor trend (Microsoft Orleans / Dapr)

The fastest-growing area in actor-model adoption as of 2025–2026 is **virtual actors** — a programming model where the framework decides placement, lifecycle management, and reactivation, and the programmer only writes entity logic.

Microsoft Orleans introduced this pattern and has seen renewed interest because:

- Users do not have to manually write supervision trees or manage process lifecycle
- The framework handles "where does this actor live?" transparently
- Actors can be persisted to storage and reactivated on demand (grain persistence)
- The model generalizes naturally to stateful AI agents: each LLM agent is a virtual actor with its own conversation history, tool state, and lifecycle

Orleans is now explicitly marketed by Microsoft for AI agent orchestration scenarios in .NET ecosystems. Apache Pekko (the open-source continuation of Akka after the BSL license change) fills a similar role in the JVM space but without the virtual/transparent lifecycle aspect.

**Why this matters for OTP-style systems:** The virtual actor model shifts the ergonomics debate. Classical OTP requires users to understand supervision trees, child specs, restart strategies, and naming. Virtual actors hide this. The question for any new actor library is whether it can offer the safety of OTP supervision while approaching the ergonomics simplicity of virtual actors.

### 1.4 The modular monolith rebound

Between 2023 and 2026, a significant reversal of the microservices trend has been documented by InfoQ and QCon survey data: approximately 42% of organizations that adopted microservices have begun merging services back into larger units. The reasons cited consistently are:

- Distributed tracing complexity and debugging overhead outweigh the benefits for most teams
- Network latency between services becomes a design constraint that forces awkward data denormalization
- Organizational overhead of owning many small services (deployment pipelines, on-call rotations, API versioning) exceeds the benefits for teams below a certain scale threshold

**The structural consequence:** Organizations moving toward modular monoliths need concurrency within the monolith boundary. OTP's GenServer and Supervisor are precisely the right tools for this scenario — they provide structured concurrency, fault isolation between components, and restartable subsystems inside a single deployment unit. This is arguably the strongest near-term positioning for a Kotlin/JVM OTP library: **a structured concurrency framework for modular monoliths that need internal fault isolation**.

### 1.5 Summary of Question 1

```
Most engineers rely on K8s + stateless services — distribution complexity is real
but pushed to the infrastructure layer. The actor model is a high-value niche for
systems where per-entity state, fault isolation, and concurrency density matter
more than operational simplicity. The modular monolith rebound creates a new
opportunity: structured concurrency inside a deployment unit.
```

| Segment | Approach | Where actor model fits |
|---|---|---|
| CRUD web apps / APIs | Stateless + K8s + managed DB | Does not fit — external store is cheaper |
| High-concurrency stateful entities | Actor model (OTP, Akka, Orleans) | Core use case — fault isolation per entity |
| Real-time systems (gaming, comms) | Actor model or actor-inspired | Core use case — serial execution per entity |
| Fintech / telecom | Erlang/OTP | Core use case — nine-nines availability |
| AI agent orchestration | Virtual actors (Orleans / Dapr) | Emerging use case — stateful LLM agents |
| Modular monoliths (rebound trend) | Structured concurrency needed | Strong opportunity for OTP-style libraries |

---

## 2. Library Patterns: Robust vs Malleable

### 2.1 The load-bearing invariant: the generic/callback split

The single most important design principle in Erlang/OTP, and the one that makes it trustworthy in production, is the **separation between the generic module and the callback module**. This is not a stylistic choice — it is an architectural safety guarantee.

In OTP:
- `gen_server` is the generic module. Users never touch it. It owns the mailbox receive loop, the call/reply state machine, the crash reporting path, and the supervisor integration handshake.
- The user implements `init/1`, `handle_call/3`, `handle_cast/2`, `handle_info/2`, and `terminate/2`. These are callbacks into the generic module's lifecycle, not replacements for it.

The invariants that make the supervisor tree trustworthy depend on this split being inviolable:

**Invariant 1 — Synchronous initialization guarantee**
`init/1` must complete (return `{ok, State}` or `{stop, Reason}`) before the parent supervisor considers the child started. If a child's `init/1` blocks or crashes, the supervisor knows immediately and applies restart policy. Any library that allows asynchronous init (child "registers itself later") breaks supervision correctness — the supervisor may declare success before the child is ready.

`→ internal: kotlin-otp implements this via SimpleOneForOneTemplate.startChildSync + ready(T), which blocks the caller until the child emits a ready signal. Parity confirmed — docs/investigation/parity-matrix.md.`

**Invariant 2 — Single-mailbox sequential execution**
One message is processed at a time. There are no concurrent callbacks for a single actor. This is the invariant that makes it safe to reason about actor state without locks. Any library that introduces concurrency into the callback loop (e.g., launching a coroutine inside `handle_call` that also writes to state) breaks this guarantee silently.

`→ internal: kotlin-otp's GenServer runs a single sequential mailbox loop. Parity confirmed — docs/investigation/parity-matrix.md.`

**Invariant 3 — Restart intensity limiter**
The supervisor tracks crash frequency over a time window and terminates itself (escalating to its own supervisor) if the rate exceeds the configured intensity threshold. This prevents infinite restart loops from consuming system resources. The threshold is parameterized but the enforcement is not — users cannot disable the escalation.

`→ internal: kotlin-otp implements intensity cutoff with Supervisor escalation. Parity confirmed — SupervisorOtpParityTest.kt.`

**Invariant 4 — `call` failure on server death**
A caller blocked on a synchronous `call` must receive an error (not hang indefinitely) if the target server dies before sending a reply. This is observable safety: callers can always write `try { server.call(...) } catch (ServerDownException) { ... }` and rely on it firing.

`→ internal: kotlin-otp delivers ServerDownException via a death watch on the server Job. Parity confirmed — docs/investigation/parity-matrix.md.`

**These four invariants are the parts of the library that must never be exposed as user-configurable.** Any library that treats restart policy implementation, mailbox loop concurrency, or call-failure semantics as "advanced options" is trading safety for flexibility in a domain where the tradeoff is wrong.

### 2.2 The extension surface: what must be malleable

Based on studying Erlang/OTP community patterns, Akka's plugin API, and Orleans extensibility points, the following are the legitimate customization axes — the parts where one-size-fits-all would be a liability:

| Extension Point | Rationale | kotlin-otp status |
|---|---|---|
| **Mailbox discipline** (FIFO / LIFO / priority) | Burst absorption (LIFO), low-latency priority (priority mailbox) — domain-specific | FIFO baseline; SelectiveMailbox for administrative messages |
| **Bounded mailbox + overflow policy** | Drop oldest, drop newest, park caller, error — depends on whether data loss is acceptable | Not yet first-class; backlog item |
| **Queue discipline for admission control** | FIFO for fairness; LIFO for responsiveness; passive for external control | Implemented in samples/jobs (queue type plugins) |
| **Backpressure policy** | Drop, park, or reject are all valid depending on use case | Exposed via jobs regulators |
| **Rate / concurrency regulator** | Rate (N/s), Counter (N concurrent), group_rate (across queues) | Rate and Counter in samples/jobs; group_rate not yet implemented |
| **Persistence backend** | ETS for hot data; DETS for durability; external DB for shared state | otp-ets / otp-dets / otp-mnesia layers exist |
| **Transport for distribution** | In-memory (tests), TCP/JSON (Kotlin nodes), ETF (Erlang interop) | All three implemented — otp-distribution |
| **Restart strategy per child** | Permanent / Transient / Temporary; extensions for DynamicSupervisor | Parity + intentional extensions |
| **Worker pool overflow policy** | Reject, queue, create overflow workers — poolboy's core extension point | Implemented in samples/poolboy |

The `samples/poolboy` and `samples/jobs` ports demonstrate this split working correctly in kotlin-otp: the admission policy (what to do when capacity is exhausted) is pluggable, while the coordination loop (the GenServer state machine managing checkout/checkin, the rate regulator applying token bucket logic) is sealed.

### 2.3 Production footguns: what the library must protect against

These are the most common production incident categories across actor frameworks, drawn from Akka production postmortems, Orleans cluster management docs, and OTP community discussions:

**Footgun 1 — Split-brain during rolling deployments**
When a singleton actor (ShardCoordinator, global name registry leader, rate limiter coordinator) is replaced during a rolling deployment, there is a window where two nodes believe they are the leader. Both accept writes. This is the most documented production incident type in Akka Cluster Sharding deployments.

Mitigation: explicit epoch/fencing tokens, leader election with mandatory lease acquisition before accepting work, or partition-aware coordination protocols. OTP's `global` module uses a two-phase locking protocol to prevent this during netsplits.

`→ internal: kotlin-otp implements GenLeader (leader election) and otp-global. Whether these survive rolling-restart scenarios is listed as Unknown in parity-matrix.md — this is a P1 gap.`

**Footgun 2 — God Actor anti-pattern**
A single GenServer becomes the serialization point for all requests in a subsystem. Under load, its mailbox grows unboundedly and its processing latency becomes the system's bottleneck. Common in systems where a "coordinator" grows beyond its intended scope.

Mitigation: partition actors by entity key (sharding), use `cast` for fire-and-forget operations that do not need a reply, apply backpressure at the API boundary before reaching the actor.

**Footgun 3 — Selective receive on hot paths**
Using selective receive (`receive` with a predicate) in a high-throughput path causes O(n) mailbox scans on every non-matching message. Under load, this degrades throughput non-linearly and can cause message starvation.

`→ internal: kotlin-otp's SelectiveMailbox shows this degradation empirically — p50 jumps from 206 µs at depth 1,000 to 560 µs at depth 10,000. The ergonomics audit (3/5 misuse resistance) flags this as the primary footgun. Docs warn about it but the API does not enforce the warning.`

**Footgun 4 — JVM GC-induced false cluster partitions**
JVM G1GC stop-the-world pauses in production Akka and Pekko clusters have been measured at 80–125 ms. The cluster heartbeat timeout (default 5 s in Akka) is usually much longer, but under memory pressure or large heap sizes, pauses can approach or exceed heartbeat frequency, causing a node to be declared dead while it is merely pausing. The result is unnecessary shard migration or split-brain resolution (data loss in the worst case).

BEAM avoids this structurally: garbage collection in Erlang is per-process, bounded per-reduction, and does not stop the entire scheduler. This is not a footgun in kotlin-otp specifically — it is a JVM runtime constraint that applies to any JVM actor framework. However, it must be documented explicitly because users migrating from Erlang will not expect it.

`→ internal: Not currently documented with concrete numbers in LIMITATIONS.md. Recommended addition: G1GC 80–125 ms pause range vs BEAM per-process GC.`

**Footgun 5 — Deferred reply lifetime management**
When a GenServer defers a reply (`DeferReply`), the `ReplyHandle` is valid only until the caller's timeout or the caller's own termination. Holding a `ReplyHandle` beyond its validity window (e.g., storing it and replying minutes later) will either panic or silently drop the reply. This is a subtle API contract that is easy to violate.

`→ internal: kotlin-otp's ReplyHandle uses callerJob monitoring to detect caller death. The ergonomics audit notes this as a complexity that requires discipline in long-lived workflows.`

---

## 3. Performance Requirements by Pattern

### 3.1 The latency hierarchy

Not all distributed patterns have the same latency requirements. A key insight from industry data is that latency requirements differ by **two to three orders of magnitude** across use cases, and choosing the wrong pattern for a latency tier is a correctness problem as much as a performance problem.

| Pattern | Typical latency target | Basis |
|---|---|---|
| Local actor call (same process/JVM) | p50 < 20 µs | Akka JMH: ~8 µs floor; kotlin-otp baseline: ~14 µs |
| Cast enqueue (fire-and-forget) | p50 < 1 µs | kotlin-otp baseline: ~0.29 µs enqueue |
| In-memory distribution call | p50 < 20 µs | kotlin-otp baseline: ~14 µs (in-process transport) |
| Worker pool checkout (pre-warmed) | p95 < 1 ms | Erlang poolboy community; industry expectation |
| Local rate limiter (token bucket / semaphore) | Added overhead < 1 µs | Bucket4j, Guava RateLimiter published benchmarks |
| Supervisor restart (single crash) | < 5 ms to stable child | Industry expectation for background recovery |
| Supervisor restart storm (40 crashes) | < 50 ms to stable child | kotlin-otp baseline: ~13 ms |
| Distributed rate limiter (Redis-backed) | 0.5–2 ms added per request | Netflix, Cloudflare engineering blogs |
| Cross-DC rate limiter | ~10 ms acceptable | HLD Handbook; industry consensus |
| Remote actor call (TCP, same data center) | 1–10 ms | Orleans cross-silo ~25× local; network RTT ~1 ms |
| Selective receive at depth 1,000 | p50 ~206 µs | kotlin-otp local-benchmark-results.md |
| Selective receive at depth 10,000 | p50 ~560 µs | kotlin-otp local-benchmark-results.md |
| JVM GC pause (G1GC under pressure) | 80–125 ms p99+ spike | Akka/Pekko production reports |

### 3.2 Pattern-to-latency-tier mapping

```
Sub-millisecond (µs range)
├── Local actor call            — game state per-entity update, session reads
├── Cast enqueue                — telemetry, event emission
└── Local token bucket check    — in-process rate limiting without external store

1–10 ms range
├── Worker pool checkout        — DB connection pools, HTTP client pools
├── Supervisor single restart   — component recovery (background, non-blocking)
└── Supervisor storm recovery   — acceptable if crash loop detection triggers quickly

10–100 ms range
├── Redis-backed rate limiter   — API gateway per-user rate limits
├── Remote actor call (TCP)     — cross-service entity coordination
└── JVM GC pause (structural)   — unavoidable latency spike on JVM under pressure

100 ms+ (best-effort / administrative)
├── Selective receive on deep queues  — acceptable only for sys/control messages
├── Cross-DC remote call             — global coordination, acceptable for config
└── Distributed consensus (Raft)     — leader election, acceptable for infrequent ops
```

### 3.3 Where kotlin-otp's current numbers stand

`→ internal: docs/investigation/local-benchmark-results.md`

| Scenario | p50 | p95 | Throughput | Tier |
|---|---|---|---|---|
| `gen_server_call_roundtrip` | 13.96 µs | 24.04 µs | 66,306 ops/s | Sub-ms ✓ |
| `gen_server_cast_enqueue` | 0.29 µs | 0.96 µs | 1,988,755 ops/s | Sub-ms ✓ |
| `distribution_in_memory_call` | 14.00 µs | 22.21 µs | 67,593 ops/s | Sub-ms ✓ |
| `supervisor_restart_storm_recovery` | ~13 ms | ~13 ms | — | 1–10 ms ✓ |
| `selective_receive_depth_1000` | 205.79 µs | 365.54 µs | 3,739 ops/s | 100 ms tier (warning) |
| `selective_receive_depth_10000` | 559.50 µs | 778.88 µs | 1,649 ops/s | 100 ms tier (warning) |

**Reading the table:** Call and cast benchmarks are in the right tier for their use cases. The selective receive degradation is the only result that crosses a safety line — 206 µs at depth 1,000 is in the same range as a Redis round-trip, which means using `SelectiveMailbox` on a hot path would make the actor slower than an external store access. This validates the existing documentation warning but suggests the API needs a stronger guard.

### 3.4 The throughput dimension

Throughput requirements are typically tied to admission control patterns:

- **API gateway rate limiting**: industry targets range from 10,000–500,000 RPS per node, with local token bucket implementations (Bucket4j, Guava) adding sub-microsecond overhead and Redis-backed implementations adding 0.5–2 ms per request.
- **Worker pools**: pool throughput is bounded by worker service time, not checkout speed. A 100-worker pool with 10 ms service time per worker gives a theoretical 10,000 RPS throughput ceiling. Checkout latency (< 1 ms p95) matters for ensuring the pool itself is not the bottleneck.
- **Gen_server cast throughput**: at ~2M ops/s enqueue rate in kotlin-otp, the bottleneck for cast-heavy systems is the consumer side (how fast the actor drains its mailbox), not the producer side.

### 3.5 The JVM structural ceiling: tail latency under pressure

This is the most important structural finding for kotlin-otp's production positioning, and it cannot be improved by library design:

**JVM G1GC produces stop-the-world pauses of 80–125 ms in production Akka and Pekko clusters.** These pauses cause cascading effects in actor systems:
- Actors stop processing for the duration of the pause
- Cluster heartbeat timeouts may fire, triggering unnecessary shard migration or node eviction
- Callers blocked on `call` experience unexpected latency spikes that cannot be attributed to application logic
- Mailboxes accumulate messages during the pause, causing a processing burst immediately after GC that creates a second latency spike

BEAM eliminates this problem structurally: each Erlang process has its own heap, collected independently at process termination or when the per-process heap limit is reached. GC pauses are bounded per-process and do not stop the scheduler. This is the primary reason that Erlang/OTP can make stronger tail-latency guarantees than any JVM actor framework.

**Mitigations available on JVM (none are library-level):**
- ZGC or Shenandoah collectors (pause targets < 1 ms, but not zero)
- Smaller heap sizes (reduces GC pause duration at the cost of GC frequency)
- G1GC tuning (`-XX:MaxGCPauseMillis`, region sizing)
- Virtual threads (Project Loom) reduce thread count but do not affect GC pause behavior

kotlin-otp should document this as a known structural constraint in `LIMITATIONS.md` with the concrete 80–125 ms figure, so that users making production deployment decisions have the information they need.

---

## 4. Implications for kotlin-otp

### 4.1 Where the project is already well-positioned

**Supervision semantics match OTP intent.** The four load-bearing invariants (synchronous init, sequential execution, intensity limiter, call-failure-on-death) all have parity. This is the trust surface of OTP — the part that lets engineers rely on the library in production — and kotlin-otp has it. This is harder to build than it sounds.

**Latency profile is competitive for the target tier.** GenServer call roundtrip at p50 ~14 µs is in the same range as Akka (~8 µs floor) and better than OTP community benchmarks (~50–100 µs for `gen_server:call` on OTP 24+). These are all local/in-process measurements, but they establish that the coroutine-based implementation is not paying an unacceptable structural penalty.

**The right patterns are being validated.** `samples/poolboy` (worker pools) and `samples/jobs` (rate/concurrency admission) are exactly the patterns that industry uses for the problems actor systems are good at. These are not toy examples — they are direct ports of production Erlang libraries with non-trivial semantics.

**The extension surface is appropriately factored.** Admission policy (queue discipline, overflow behavior, rate regulator type) is pluggable in both samples. The coordination loop (the GenServer state machine) is not exposed for extension. This matches the OTP design principle.

### 4.2 Where to focus next (research-informed backlog)

**P1: Document the JVM GC structural ceiling**
Add the 80–125 ms G1GC pause range and its cluster-level consequences to `LIMITATIONS.md`. Engineers evaluating kotlin-otp for production should have this information before they hit it in a production incident. Frame it correctly: this is a JVM constraint, not a kotlin-otp deficiency, but it affects production deployment decisions.

**P1: Validate worker pool checkout latency under load**
The investigation benchmarks measure `gen_server_call_roundtrip` in isolation. A pre-warmed worker pool checkout under concurrent load (10–100 concurrent callers, pool at 80% utilization) should demonstrate < 1 ms p95. This is the industry expectation for worker pool patterns and the benchmark that makes `samples/poolboy` credible as a production library.

**P1: Adversarial benchmark for singleton safety during rolling restarts**
The top production incident category in actor frameworks is split-brain when a singleton (global registry leader, coordinator) is replaced. kotlin-otp's `GenLeader` and `otp-global` need a test that simulates a rolling restart and verifies that exactly one leader is active at all times during the transition. This is currently listed as Unknown in the parity matrix.

**P2: Stronger guards for SelectiveMailbox on hot paths**
The ergonomics audit gives misuse resistance 3/5 specifically because `SelectiveMailbox` can be used on hot paths without any runtime warning. Options: a `@HotPath` annotation that triggers a compile-time warning when used with selective receive, a runtime depth counter that logs a structured warning when scans exceed a threshold (e.g., depth > 100 in a single receive), or an API design that makes it harder to instantiate on a high-throughput actor.

**P2: Investigate virtual actor ergonomics**
The Orleans/Dapr model (framework manages lifecycle, user writes entity logic) is gaining the most industry mindshare in 2025–2026, particularly for AI agent orchestration. The question for kotlin-otp is whether a `VirtualGenServer` abstraction is feasible — one where the user implements callbacks but does not write `ChildSpec`, does not manage supervisor topology, and the framework reactivates the actor from state if needed. This would reduce the learning curve for users coming from a K8s-first background.

**P3: Position explicitly for the modular monolith use case**
The documentation and samples currently position kotlin-otp as "Erlang/OTP concepts on the JVM." The modular monolith rebound (42% of microservices adopters consolidating) creates a more specific and immediately actionable pitch: "structured fault isolation between components inside a single deployment unit." This framing requires no knowledge of Erlang and addresses a pain point that engineers are actively experiencing.

**P3: AI agent as GenServer sample**
A sample that uses a `GenServer` to manage the state of a long-running LLM agent (conversation history, tool call results, retry state) would demonstrate the natural fit between OTP supervision and AI agent orchestration. Each agent restart from a known state is a direct application of OTP `init/1` semantics. This is the fastest-growing adoption vector for actor frameworks in 2025–2026 and could substantially expand kotlin-otp's audience.

### 4.3 What to explicitly not pursue

**BEAM ETF wire compatibility**: Implementing Erlang's external term format and EPMD protocol would allow kotlin-otp nodes to join real Erlang clusters. The engineering cost is substantial and the use case is narrow (hybrid Erlang/Kotlin deployments). The current Kotlin-native TCP/JSON transport in `otp-distribution` is the right call for the primary audience.

**Claiming production parity with BEAM on tail latency**: The JVM GC ceiling is real and structural. Claiming that kotlin-otp matches Erlang/OTP on tail latency under production conditions would be inaccurate. The correct claim is architectural fidelity and competitive median latency, with explicit documentation of the tail latency gap.

**Full `group_rate` distributed admission control**: The `jobs` sample notes this as a v1 non-goal. Distributed rate limiting across nodes is a hard distributed systems problem (consensus, clock skew, partition handling). The Redis-backed industry approach exists and is well-understood. kotlin-otp should not try to solve this in the library until the simpler local-rate and counter patterns are production-validated.

---

## 5. Sources

### Industry surveys and reports

- CNCF Annual Survey 2024 — Kubernetes production adoption (~80%)
- InfoQ and QCon microservices consolidation survey data (2023–2024) — ~42% merging services back
- Lightbend State of Reactive Survey (2022–2023) — Akka adoption patterns

### Engineering blogs and case studies

- Discord Engineering Blog: "How Discord Stores Trillions of Messages" (2023) and Elixir architecture series
- WhatsApp architecture talks at various conferences (Erlang Factory)
- Klarna Engineering Blog: Erlang in fintech (reliability over throughput)
- Riot Games / VALORANT server architecture (16.67 ms frame budget)
- Bet365 Erlang adoption case studies
- Netflix Rate Limiting Engineering Blog (Redis-backed rate limiting)
- Cloudflare Engineering Blog: rate limiting at scale

### Framework documentation and benchmarks

- Microsoft Orleans documentation — virtual actor model, grain persistence, AI agent positioning (2025)
- Apache Pekko documentation — cluster sharding, singleton safety
- Akka JMH benchmarks (Lightbend / Apache Pekko community) — ~8 µs local actor call floor
- OTP community benchmarks — `gen_server:call` ~50–100 µs (OTP 24+)
- Bucket4j / Guava RateLimiter benchmarks — sub-microsecond local token bucket overhead
- HLD Handbook rate limiter tier analysis (nanoseconds local, 0.5 ms co-located Redis, 10 ms cross-DC)

### kotlin-otp internal

- `docs/investigation/local-benchmark-results.md` — baseline performance numbers
- `docs/investigation/parity-matrix.md` — OTP semantic fidelity mapping
- `docs/investigation/ergonomics-audit.md` — API ergonomics scoring
- `docs/investigation/final-report.md` — investigation executive summary
- `samples/poolboy/` — worker pool port (production pattern validation)
- `samples/jobs/` — admission control port (production pattern validation)
