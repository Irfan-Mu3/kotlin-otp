# Investigation Benchmarks

Local benchmark harness for Kotlin-OTP investigation work.

## Run

- Quick profile (single round, fast baseline):
  - `./gradlew :samples:investigation:run --args="--profile=quick"`
- Long profile (multi-round, decision-grade trend signal):
  - `./gradlew :samples:investigation:run --args="--profile=long"`
- Override rounds for either profile:
  - `./gradlew :samples:investigation:run --args="--profile=long --rounds=7"`

Output includes:

- Per-round CSV rows
- Summary CSV rows with mean and standard deviation across rounds

The app prints CSV-formatted scenario metrics for:

- GenServer call roundtrip latency/throughput
- GenServer cast enqueue throughput
- Selective receive scan cost at different queue depths
- Supervisor restart storm recovery timing
- Distribution in-memory call overhead
- Coarse mailbox cast memory delta
