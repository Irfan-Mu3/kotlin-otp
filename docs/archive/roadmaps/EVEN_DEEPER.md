# Even deeper: what to do next

This doc is the **successor** to [DEEPER.md](DEEPER.md): it assumes the first “library-first” slice is in place (supervisor strategies, shutdown order, backoff, dynamic `simple_one_for_one`, `otp-gen-statem`, `otp-registry`, typed gen_server façade, library monitors/links, and logging hooks—see [LIMITATIONS.md](../../LIMITATIONS.md) and [TRACEABILITY.md](../../TRACEABILITY.md)). Below is **what still moves the needle** without pretending the JVM is the BEAM.

---

## 1. Tighten OTP fidelity (same stack: Kotlin + coroutines)

### Supervision

- **Spec-test against OTP:** For each strategy, add table-driven tests that mirror examples in OTP’s `supervisor.erl` / docs (edge cases: `Temporary` / `Transient` children, intensity with mixed restart counts, shutdown timeouts that overrun).
- **Supervisor-wide restart intensity:** Today intensity is tracked **per triggering child** in the static supervisor; compare with OTP’s supervisor-scoped limits and decide whether to offer **both** policies via a flag on [`SupervisorFlags`](../../otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/SupervisorFlags.kt).
- **Dynamic + strategies:** [`DynamicSupervisor`](../../otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/DynamicSupervisor.kt) is intentionally **`one_for_one` only**; either document that forever, or define how `one_for_all` / `rest_for_one` should behave with a **mutable** child set.

### `gen_server`

- **Sealed protocols:** Prefer `sealed interface Request` / `sealed interface Reply` per app (or a code generator) sitting on top of [`TypedGenServerRef`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/TypedGenServer.kt); keep the untyped path for interop.
- **System messages:** Sealed `InfoMsg` for synthetic `DOWN`, timers, and internal control; optionally drain **high-priority** control on a side channel while keeping FIFO for user messages ([`ProcessMailbox`](../../otp-mailbox/src/main/kotlin/org/otpstudy/mailbox/ProcessMailbox.kt)).
- **`TerminateReason` everywhere:** Thread [`TerminateReason`](../../otp-gen-server/src/main/kotlin/org/otpstudy/genserver/TerminateReason.kt) (including `BrutalKill`) through supervision decisions and logs consistently.

### `gen_statem`

- **Timeouts and cancelable timers:** Align with OTP’s state timeouts (named timers, `StateTimeout`, `EventTimeout`, `generic_timeout`) and cancellation on transition.
- **Callback modes:** Support something analogous to `state_functions` vs `handle_event_function` if you want closer surface parity with [`gen_statem`](https://www.erlang.org/doc/design_principles/statem.html).
- **Property tests:** [`kotest-property`](https://kotest.io/docs/proptest/property-based-testing.html) or similar over random event sequences vs a reference transition table.

### New behaviour module: `gen_event`

- New Gradle module `otp-gen-event`: handler add/remove/notify, ordering guarantees, and a documented **back-pressure** policy (drop vs block vs bounded mailbox).

---

## 2. Application layer and operations

- **Phased startup:** In [`otp-application`](../../otp-application/src/main/kotlin/org/otpstudy/application/OtpApplication.kt), model `start_phase` / dependency order as an explicit graph or state machine (still no `.rel` / release handler required for learning).
- **`ApplicationEnv`:** Typed or map-based config with lifecycle hooks on `start` / `stop` (idea parity with `.config`, not file format parity).
- **Registry + supervision:** Auto re-register on restart—hook [`ProcessRegistry`](../../otp-registry/src/main/kotlin/org/otpstudy/registry/ProcessRegistry.kt) into supervisor restart paths or document a small `GenServer` wrapper pattern.

---

## 3. Observability and hardening

- **Structured fields:** Standardise `OtpLogContext` (supervisor id, child id, restart count, `GenStateM` state name) and avoid overloading `tag` for unrelated labels.
- **OpenTelemetry:** Spans around `GenServerRef.call`, supervisor restarts, and `DynamicSupervisor` child churn; export metrics (restart counts, mailbox depth if bounded).
- **Stress and race tests:** Many `GenServer`s + [`DynamicSupervisor`](../../otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/DynamicSupervisor.kt) churn under `kotlinx-coroutines-test` virtual time; hunt cancellation races (epoch logic in [`Supervisor.kt`](../../otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/Supervisor.kt) is a good place to extend).

---

## 4. Isolation and “when Kotlin is not enough”

Follow the ladder in [DEEPER.md §4](DEEPER.md):

1. **Virtual threads (Loom):** Optional execution profile for blocking-style servers alongside coroutines.
2. **Process-per-actor or extra JVMs:** For fault or memory isolation when untrusted code or hard failure boundaries matter.

Keep embedding or sidecar **BEAM** in reserve for semantics that cannot be faked on the JVM ([DEEPER.md §6](DEEPER.md)).

---

## 5. Suggested order of attack

1. **`gen_statem` timeouts + tests** (high teaching value, contained module).  
2. **Sealed `gen_server` protocols + `InfoMsg` story** (ergonomics for real apps).  
3. **`otp-gen-event` module** (new behaviour boundary).  
4. **Application phases + `ApplicationEnv`** (how systems boot, not just actors).  
5. **Telemetry + stress** (confidence before more surface area).  
6. **Isolation experiments** (Loom or worker process) only when a measured gap appears.

Cross-link new APIs and test suites in [TRACEABILITY.md](../../TRACEABILITY.md) as you land each slice so the lab stays navigable.

**After this pass is complete:** see [DEEPER_STILL.md](DEEPER_STILL.md) for the next layer — bounded mailboxes, async reply, `sys` introspection, postpone/state-enter, crash reporters, selective receive, links with trap_exit, distribution concepts, ETS-style tables, and the observer data layer.
