# Kotlin-OTP Ergonomics and Operability Audit

## Rubric

- **Learning curve**: how quickly users can map OTP mental models to Kotlin APIs.
- **Misuse susceptibility**: how easy it is to write code that is subtly incorrect.
- **Operational observability**: visibility into queue depth, crash causes, restart dynamics.
- **Recovery ergonomics**: clarity and safety of stop/restart/upgrade pathways.

## Findings

### 1) Learning Curve: Moderate-to-Strong

- `GenServer`/`Supervisor` naming and callback flow closely mirror OTP behavior APIs.
- Documentation comments often include OTP source references, which helps context transfer.
- Main gap: JVM coroutine semantics leak into the model (cancellation, dispatchers, structured concurrency).

## 2) Misuse Susceptibility: Moderate Risk

- `SelectiveMailbox` correctly documents `O(n)` replay behavior, but developers can still use it on hot paths unintentionally.
- `GenServerRef.call` defaults are ergonomic, but timeout/caller-job semantics require discipline in long-lived workflows.
- Dynamic supervisor sync-start handshake is feature-rich but complex and therefore easier to misuse.

## 3) Operational Observability: Good Foundations, Uneven Surface

- Supervisor and GenServer code paths include structured logging and crash reporting hooks.
- `whichChildren` snapshots provide useful supervisor visibility.
- Missing: standardized per-module performance counters (mailbox scan depth, restart histogram, call percentiles).

## 4) Recovery Ergonomics: Good API, Runtime Caveats

- Restart strategy and restart intensity controls are explicit and familiar to OTP users.
- Shutdown APIs exist for static and dynamic supervisors.
- JVM constraint caveat (cooperative cancellation vs BEAM `kill`) requires explicit operator understanding.

## API-Shape Scorecard (1-5)

- **Learning curve**: 4/5
- **Misuse resistance**: 3/5
- **Observability**: 3/5
- **Recovery ergonomics**: 4/5

## Recommendations

1. Add first-class runtime metrics APIs for mailbox and restart behavior.
2. Add "unsafe-by-default" footgun warnings in docs for selective receive and mailbox bounds.
3. Add ergonomics examples for common production workflows:
   - request-reply with deferred reply lifecycle
   - cancellation-safe supervision of worker pools
   - distribution failure handling patterns
