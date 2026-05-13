#!/usr/bin/env bash
# Usage: ./scripts/test-module.sh otp-gen-server
# Optional: ./scripts/test-module.sh otp-gen-server 'org.otpstudy.genserver.HibernateTest'
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MOD="${1:?module name, e.g. otp-gen-server}"
shift || true
if [[ $# -gt 0 ]]; then
  ./gradlew ":$MOD:test" --console=plain --fail-fast --tests "$1"
else
  ./gradlew ":$MOD:test" --console=plain --fail-fast
fi
