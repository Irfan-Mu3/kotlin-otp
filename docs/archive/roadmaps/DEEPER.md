# Going deeper: OTP → Kotlin (report)

This report assumes you have read [LIMITATIONS.md](../../LIMITATIONS.md) and [TRACEABILITY.md](../../TRACEABILITY.md). The goal below is **depth in layers**: deeper OTP fidelity first where it is mostly library work, then stronger runtime isolation, and only last resort JVM or BEAM work.

---

## 1. Near horizon (still “pure Kotlin” + coroutines)

These items extend what you already have without changing the JVM.

### Supervision parity

- **`one_for_all` and `rest_for_one`:** Implement the same state transitions as in OTP’s [`supervisor`](https://www.erlang.org/doc/man/supervisor.html) (which children restart, which are terminated first). Your local reference is [`system/doc/design_principles/sup_princ.md`](../../../system/doc/design_principles/sup_princ.md) plus `lib/stdlib/src/supervisor.erl` in this OTP tree.
- **`shutdown` semantics:** Map `brutal_kill` vs timeout vs `infinity` to `Job.cancel`, `withTimeout { cancelAndJoin }`, and ordered teardown. OTP stops children in **reverse start order**; document and test that your `SupervisorRef.shutdown()` mirrors it.
- **Restart delays / backoff:** OTP allows restart intensity plus optional backoff between restarts; add policy types and tests next to [`RestartPolicy.kt`](../../otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/RestartPolicy.kt).

### `gen_server` depth

- **Typed protocols:** Replace `Any` request/reply with generic `GenServer<S, Req, Rep>` or a sealed `Protocol` per actor to regain compile-time safety (closer to well-typed OTP interfaces, without `-behaviour` checks).
- **`handle_info` and system messages:** Model OTP-style `timeout`, `DOWN`, and internal ticks as sealed subtypes; optionally a small `select`-style API on top of [`ProcessMailbox`](../../otp-mailbox/src/main/kotlin/org/otpstudy/mailbox/ProcessMailbox.kt) (still not BEAM selective receive, but closer ergonomically).
- **`terminate` / `Stop` reasons:** Align reason taxonomy with OTP (`normal`, `shutdown`, `brutal_kill`, `{error, Term}`) for logging and supervision decisions.

### Naming and discovery

- **Process registry:** OTP’s `global`, `pg`, or `gproc`-style registration: a module `otp-registry` with string or typed keys → `GenServerRef`, integrated with supervision so restarted workers can re-register.

### Application layer

- **Phased startup:** `kernel` application’s `mod` `{M, Args}` and `start_phase` concepts as an explicit state machine in `otp-application` (optional dependency graph, not only a flat child list).
- **Configuration:** `.config` / env-style loading is not idiomatic on JVM; a thin `ApplicationEnv` (typesafe config or plain maps) plus hooks on `start/stop` is enough for parity of *ideas*.

---

## 2. Behaviours beyond `gen_server`

Each can be a **new Gradle module** mirroring OTP boundaries.

| OTP piece | Suggested module | Depth tactic |
|-----------|------------------|----------------|
| [`gen_statem`](https://www.erlang.org/doc/design_principles/statem.html) | `otp-gen-statem` | Event + timeout + state data; optional tracing; property tests for transition tables. |
| [`gen_event`](https://www.erlang.org/doc/design_principles/events.html) | `otp-gen-event` | Handler add/remove/notify; ordered notification; back-pressure policy. |
| **Specialised supervisors** | extend `otp-supervisor` | `simple_one_for_one`, dynamic children, `which_children` / `count_children` style introspection APIs. |

Primary reading in this repo: [`system/doc/design_principles/`](../../../system/doc/design_principles/) and the matching `lib/stdlib/src/*.erl` implementations.

---

## 3. Links, monitors, and “distribution” without BEAM

True distributed Erlang is a large fork; useful subsets on the JVM:

- **Links and monitors as APIs:** `link(ref)`, `monitor(ref)`, `unlink`, `demonitor`, and `DOWN`/`EXIT` reasons delivered to a mailbox or callback—not VM-native, but teach the same supervision and client patterns as [`erl_dist`](../../../system/doc/reference_manual/) concepts at a library level.
- **Clustering later:** If you need multi-node semantics, plan for **gRPC / Aeron / Kafka**-style messaging and explicit failure detectors (SWIM, etc.), not wire-compatible Erlang distribution.

---

## 4. Stronger isolation (before patching OpenJDK)

Ordered by increasing cost and isolation:

1. **Virtual threads (Loom):** Blocking `gen_server` style with bounded queues; good when coroutine coloured APIs are awkward. Still one heap.
2. **Process-per-actor (OS process):** JNI or `ProcessBuilder` worker with IPC; true memory fault isolation for untrusted code; operational cost is high.
3. **Isolates / multiple JVMs:** Same as above with a smaller runtime if you adopt GraalVM native or compact JDK images via `jlink`.

These align with the plan’s “worst case” ladder: **Loom and OS processes before Hotspot surgery.**

---

## 5. Observability and correctness

- **Structured logging:** Correlate `OtpProcessId`, supervisor id, and restart count (OpenTelemetry spans per `call` / restart).
- **Simulation tests:** Use `kotlinx-coroutines` test dispatchers and virtual time to test restart intensity and strategy without sleeping.
- **Stress:** Many `GenServer`s + supervisor churn to find cancellation races (your `AtomicReference` demo pattern is the right idea for dynamic refs).

---

## 6. When “deeper” means leaving Kotlin-only

| Goal | Realistic path |
|------|------------------|
| Hard preemption / per-actor GC | Not Kotlin libraries; research VM, or **embed BEAM** (JNI / sidecar) for the slice that needs BEAM semantics. |
| Wire-compatible `erl_distribution` | Implement **another language’s node protocol** (huge); interop via **HTTP/gRPC** is the pragmatic route. |
| Hot code upgrade | Class loaders + careful API versioning; or accept redeploy on JVM. |

Use this repo’s **OTP source** as the spec: `erts/` for runtime, `lib/kernel`, `lib/stdlib` for behaviours you care to match line-by-line in tests.

---

## 7. Suggested milestone sequence

1. **Supervisor strategies + shutdown + reverse stop order** (highest ROI vs complexity).  
2. **`simple_one_for_one` + dynamic children** (many pooled workers).  
3. **`gen_statem` module + tests** (state-heavy domains).  
4. **Registry + typed `gen_server`**.  
5. **Link/monitor API** on top of `Job` + mailboxes.  
6. **Observability** (metrics, traces).  
7. **Optional:** Loom-based execution profile alongside coroutines.

---

## 8. How to use this OTP checkout as a lab

- **Design intent:** [`system/doc/design_principles/`](../../../system/doc/design_principles/)  
- **Behaviour contracts:** `lib/stdlib/src/gen_server.erl`, `supervisor.erl`, `gen_statem.erl`  
- **Runtime truth:** `erts/emulator/beam/` (only when you are explicitly studying what cannot be ported)

Cross-link each new Kotlin API or test suite to a paragraph or function group in OTP (extend [TRACEABILITY.md](../../TRACEABILITY.md) as you go).

That is the deepest “library-first” path; JVM or BEAM moves stay **scoped** to a single measurable gap when a library truly cannot meet it.

**After the first implementation pass:** see [EVEN_DEEPER.md](EVEN_DEEPER.md) for the next concrete steps (timeouts, `gen_event`, application phases, telemetry, isolation ladder). After that pass, see [DEEPER_STILL.md](DEEPER_STILL.md) for async reply, `sys` introspection, selective receive, distribution concepts, ETS-style tables, and the observer layer.
