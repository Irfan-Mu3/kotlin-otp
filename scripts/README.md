# Scripts for local validation

Gradle output can be large; run tests **yourself** and paste only the failing block to an assistant if needed.

## Doc check (THE_HADAL_ZONE)

From `kotlin-otp/`:

```bash
chmod +x scripts/*.sh
./scripts/verify-hadal-doc.sh
```

Fails if legacy roadmap patterns reappear in `THE_HADAL_ZONE.md` (for example `BeamCompat`, `sendEtf`).

## Tests one module at a time

```bash
./scripts/test-module.sh otp-gen-server
```

Optional: single test class

```bash
./scripts/test-module.sh otp-gen-server 'org.otpstudy.genserver.HibernateTest'
```

## All modules, stop on first failure

```bash
./scripts/test-all-modules.sh
```

(`samples:demo` is not included so a failing sample does not block core modules.)

## Hadal-style implementation order (reference)

When coding against [THE_HADAL_ZONE.md](../THE_HADAL_ZONE.md), prefer vertical slices:

1. Local: §7 `sys`, §5 timer, §10 profiler (no TCP).
2. §3 `abcast` / `multiCall` with `InMemoryTransport` + local routing.
3. §2 `KotlinNodeTransport`.
4. §3 remote + §4 leader election.
5. §6 ports, §9 supervision tree, §8 post-mortem.

Run `./gradlew :<module>:test --console=plain --fail-fast` after each slice.
