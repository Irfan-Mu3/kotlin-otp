# Production deep — Hadal §12 (production readiness)

Companion to [THE_HADAL_ZONE.md](THE_HADAL_ZONE.md). That document keeps §1–§11 as the **design spec**; **production posture** (cheap, efficient, correct) lives here so operators and maintainers have one place to read without scrolling the full roadmap.

---

## What “production” means here

- **Correct:** structured concurrency and actor lifecycles are explicit; tests cover in-process, loopback TCP, and (where needed) a **subprocess** peer. Hangs and leaks are treated as bugs (see project rules on `runBlocking` + `GenServers.startLink`).
- **Efficient:** prefer **documentation, CI, and opt-in debug** over new frameworks. Reuse JVM tooling (JFR, heap dumps, `OtpStudyDebug`) before building custom observability servers.
- **Cheap:** avoid optional Gradle modules unless a boundary is genuinely needed; split packages only when release cycles or binary compatibility demand it.

## Checklist (prioritised by ROI)

1. **Doc truth** — Keep [TRACK_B_PROGRESS.md](TRACK_B_PROGRESS.md), [LIMITATIONS.md](LIMITATIONS.md), and [TRACEABILITY.md](TRACEABILITY.md) aligned with `settings.gradle.kts` and real package names (`Postmortem` lives in **`otp-observer`**, not a separate `otp-postmortem` artifact unless you split it later).
2. **CI gate** — Run `./gradlew test` (or a documented subset) on every merge; module-scoped tasks when iterating. Fail fast: class-level timeouts on tests that start processes or TCP.
3. **Distribution assumptions** — Document deployment expectations: **private network** or **TLS-terminated** path in front of `KotlinNodeTransport` if exposed beyond localhost; non-empty **`clusterSecret`** outside dev; payload and frame limits already enforced — call them out in runbooks.
4. **Observability defaults** — `OtpLogging` is a **no-op** until `setLogger` — document that for operators. Use **`-Dorg.otpstudy.debug=true`** / **`-Porg.otpstudy.debug=true`** (see [`.cursor/rules/kotlin-coroutines-boundaries.mdc`](../.cursor/rules/kotlin-coroutines-boundaries.mdc)) for ad-hoc stderr traces without wiring a logger.
5. **Profiling in prod** — `ProfiledGenServer` is ideal for **targeted** diagnosis; routine production profiling should lean on **JFR** or your APM. The reduction-injection plugin remains an optional fidelity experiment, not a deployment requirement.
6. **Leader and registry semantics** — Treat `GenLeaderServer` and `GlobalRegistry` / `pg` as **building blocks**: document failure modes (split brain, stale monitors) for your topology rather than promising full OTP `gen_leader` parity.
7. **Release discipline** — When you cut versions: semver for public `org.otpstudy.*` APIs, a short **CHANGELOG**, and a note that wire format (`DistMsg`) is a compatibility surface.

## Deliberate non-goals for this phase

- New wire codecs (CBOR, etc.) unless measured need.
- Full `otp-profiler` / `fprof`-style call-graph product — JFR fills most JVM needs.
- Chaos engineering frameworks — add **soak** or **multi-node** tests only when a regression appears.
