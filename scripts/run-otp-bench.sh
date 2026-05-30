#!/usr/bin/env bash
# run-otp-bench.sh — Run the Erlang/OTP competitor benchmarks via escript.
#
# Usage:
#   ./scripts/run-otp-bench.sh [--profile=quick|long] [--rounds=N]
#
# The escript outputs CSV rows to stdout.  Pipe to a file or combine with the
# JVM benchmarks:
#
#   ./scripts/run-otp-bench.sh --profile=long --rounds=5 | tee otp-bench-$(date +%Y%m%d).csv
#   ./gradlew :samples:benchmarks:run --args="--profile=long --rounds=5 --otp"
#
# The --otp flag in the Gradle task also invokes this escript automatically via
# ProcessBuilder (see CompetitorBenchmarks.kt), so you rarely need to call this
# script directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ESCRIPT_FILE="$PROJECT_ROOT/samples/benchmarks/src/main/erlang/otp_bench.erl"

# Resolve escript binary (honour ESCRIPT_PATH env override)
ESCRIPT="${ESCRIPT_PATH:-$(command -v escript 2>/dev/null || echo "")}"

if [[ -z "$ESCRIPT" ]]; then
    echo "error: 'escript' not found on PATH. Install Erlang/OTP (e.g. brew install erlang)." >&2
    exit 1
fi

if [[ ! -f "$ESCRIPT_FILE" ]]; then
    echo "error: escript not found at $ESCRIPT_FILE" >&2
    exit 1
fi

exec "$ESCRIPT" "$ESCRIPT_FILE" "$@"
