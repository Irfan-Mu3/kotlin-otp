# Track B implementation progress (THE_HADAL_ZONE code)

Last updated: Track B code + tests + docs; run `./gradlew` locally and paste failures if any.

**Production / ops checklist:** [PRODUCTION_DEEP.md](PRODUCTION_DEEP.md) (split out from Hadal §12).

**Archived DEEP roadmaps:** [docs/archive/roadmaps/README.md](docs/archive/roadmaps/README.md) (historical narrative; not deleted).

## Done (expected to compile after your verification)

| Slice | Items | Location |
|-------|--------|----------|
| **B1** | `getState` / `replaceState` on `GenServerRef` | `otp-gen-server/GenServer.kt` |
| | Global `OtpTimer` (distinct from `OtpTimers`) | `otp-gen-server/OtpTimer.kt` |
| | `ProfiledGenServer` + `GenServer.profiled()` | `otp-gen-server/ProfiledGenServer.kt` |
| | `SupervisorRef.whichChildren()` + `SupervisorChildInfo` | `otp-supervisor/Supervisor.kt` |
| **B2** | `DistributedGenServers.abcast` / `multiCall` | `otp-distribution/DistributedGenServers.kt` |
| **B3** | `DistMsg`, `DistributionWire`, `NoSuchNodeException` | `otp-distribution/*.kt` |
| | `KotlinNodeTransport` (`wire`, `startAccepting`, `connectOut`) | `otp-distribution/KotlinNodeTransport.kt` |
| | Gradle: serialization plugin + `kotlinx-serialization-json` + `otp-registry` | `settings.gradle.kts`, `otp-distribution/build.gradle.kts` |
| **B4** | `GenLeaderServer`, `LeaderCallbacks`, `GenLeaders.startLink`, `LeaderMsg.Elect(exclude)` | `otp-distribution/GenLeader.kt` |
| **B5** | `OtpPort`, `PortData`, `PortExit` | `otp-gen-server/OtpPort.kt` |
| | `Postmortem.capture()` | `otp-observer/Postmortem.kt` |
| | `SupervisorTree.fromSupervisorRef` / `toAscii` / `toDot` | `otp-observer/SupervisorTree.kt` |

## Tests added (run when convenient)

- `otp-distribution`: [DistributionWireTest.kt](otp-distribution/src/test/kotlin/org/otpstudy/distribution/DistributionWireTest.kt), [DistributedGenServersTest.kt](otp-distribution/src/test/kotlin/org/otpstudy/distribution/DistributedGenServersTest.kt), [KotlinNodeTransportLoopbackTest.kt](otp-distribution/src/test/kotlin/org/otpstudy/distribution/KotlinNodeTransportLoopbackTest.kt), [KotlinNodeTransportMultiProcessTest.kt](otp-distribution/src/test/kotlin/org/otpstudy/distribution/KotlinNodeTransportMultiProcessTest.kt) (subprocess [DistEchoServerMain.kt](otp-distribution/src/test/kotlin/org/otpstudy/distribution/DistEchoServerMain.kt), 25× remote `call`)
- `otp-supervisor`: [SupervisorWhichChildrenTest.kt](otp-supervisor/src/test/kotlin/org/otpstudy/supervisor/SupervisorWhichChildrenTest.kt)
- `otp-observer`: [SupervisorTreeNestedTest.kt](otp-observer/src/test/kotlin/org/otpstudy/observer/SupervisorTreeNestedTest.kt) (`SupervisorTree.fromSupervisorRef` + `nestedSupervisors` map)

```bash
./gradlew :otp-distribution:test :otp-supervisor:test
```

## Not done here (follow-up you may want)

- Heavier soak / chaos (many subprocesses, connection churn) — [KotlinNodeTransportMultiProcessTest](otp-distribution/src/test/kotlin/org/otpstudy/distribution/KotlinNodeTransportMultiProcessTest.kt) is a baseline.
- Auto-discover nested `SupervisorRef` from `whichChildren` (would need richer child metadata than [SupervisorChildInfo](otp-supervisor/src/main/kotlin/org/otpstudy/supervisor/Supervisor.kt)).
- Custom wire codecs beyond [DistributionWire](otp-distribution/src/main/kotlin/org/otpstudy/distribution/DistributionWire.kt) JSON primitives / [JsonObject](otp-distribution/src/test/kotlin/org/otpstudy/distribution/DistributionWireTest.kt).

## Quick compile ideas (for you)

```bash
cd kotlin-otp
./gradlew :otp-supervisor:compileKotlin :otp-gen-server:compileKotlin :otp-distribution:compileKotlin :otp-observer:compileKotlin
```
