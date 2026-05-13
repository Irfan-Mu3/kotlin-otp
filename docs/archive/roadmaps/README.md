# Archived roadmap documents (DEEP series)

These files are **historical design roadmaps** for kotlin-otp. They are kept for narrative and OTP cross-references; **current** behaviour mapping lives in [TRACEABILITY.md](../../TRACEABILITY.md), limits in [LIMITATIONS.md](../../LIMITATIONS.md), and the active distribution / production story in [THE_HADAL_ZONE.md](../../THE_HADAL_ZONE.md) + [PRODUCTION_DEEP.md](../../PRODUCTION_DEEP.md).

## Reading order (chronological)

1. [DEEPER.md](DEEPER.md) — first “go deeper” report (supervision parity, gen\_statem depth, isolation ladder).
2. [EVEN_DEEPER.md](EVEN_DEEPER.md) — timeouts, `gen_event`, application phases, telemetry.
3. [DEEPER_STILL.md](DEEPER_STILL.md) — bounded mailboxes, `sys`, selective receive, distribution primitives in-JVM, ETS, observer layer (13 sections; implemented).
4. [THE_DEEP_END.md](THE_DEEP_END.md) — memory / reductions / `erl_dist` bridge / hot code / match specs / `process_info` (fourth roadmap).
5. [BEYOND_THE_DEEP_END.md](BEYOND_THE_DEEP_END.md) — `ActorArena`, reduction IR plugin, and related “beyond library-only” items (completed roadmap).

Later roadmaps live at the kotlin-otp root: [THE_ABYSS.md](../../THE_ABYSS.md), [THE_HADAL_ZONE.md](../../THE_HADAL_ZONE.md).

Internal links between files in this folder use **same-directory** names. Links to repo roots use `../../`.
