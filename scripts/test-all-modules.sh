#!/usr/bin/env bash
# Runs each subproject's test task; stops on first failure.
# Does not run samples:demo (run manually if needed).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODULES=(
  otp-core otp-mailbox otp-gen-server otp-gen-statem otp-supervisor
  otp-registry otp-application otp-gen-event otp-distribution otp-ets
  otp-observer otp-memory otp-hotcode otp-jinterface otp-pg otp-global
  otp-mnesia otp-logger otp-sasl otp-recon otp-trace otp-dets
)

for m in "${MODULES[@]}"; do
  echo "========== :$m:test =========="
  ./gradlew ":$m:test" --console=plain --fail-fast
done

echo "All listed module tests passed."
