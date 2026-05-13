#!/usr/bin/env bash
# Mechanical acceptance for THE_HADAL_ZONE.md (no Gradle).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOC="$ROOT/THE_HADAL_ZONE.md"
if [[ ! -f "$DOC" ]]; then
  echo "Missing: $DOC"
  exit 1
fi
if grep -nE 'BeamCompat|otp-etf|CrashDumpParser|sendEtf|ErlangCodec' "$DOC"; then
  echo "verify-hadal-doc: FAIL — legacy integration patterns found in THE_HADAL_ZONE.md"
  exit 1
fi
echo "verify-hadal-doc: OK (no forbidden patterns)"
for i in 1 2 3 4 5 6 7 8 9 10 11; do
  if ! grep -q "^## $i\. " "$DOC"; then
    echo "verify-hadal-doc: FAIL — missing heading for section $i"
    exit 1
  fi
done
echo "verify-hadal-doc: OK (sections 1–11 present)"
