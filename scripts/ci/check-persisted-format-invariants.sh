#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# A static policy guard only. Codec-owned tests prove actual wire admission;
# this scan does not claim to validate codec behavior or documentation prose.
format_version_matches="$(
  rg -n --no-heading --color=never \
    '(?:const\s+[A-Z0-9_]*VERSION[A-Z0-9_]*\s*:\s*(?:u8|u16|u32|u64|usize)\s*=\s*(?:[2-9]|[1-9][0-9]+)\s*;|const\s+CURRENT\s*:\s*Self\s*=\s*Self\((?:[2-9]|[1-9][0-9]+)\)\s*;)' \
    crates testing \
    --glob '*.rs' || {
      rg_status=$?
      if [[ $rg_status -ne 1 ]]; then
        exit "$rg_status"
      fi
    }
)"
if [[ -n "$format_version_matches" ]]
then
  printf '%s\n' "$format_version_matches"
  echo "[ERROR] An active format/version constant exceeds version 1." >&2
  exit 1
fi

echo "[OK] Persisted-format version policy scan passed."
