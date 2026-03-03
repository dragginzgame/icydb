#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# Architecture invariants must be enforced structurally, not by source-text
# scans over runtime modules.
literal_source_scans="$(
  rg -n --no-heading --color=never 'include_str!\s*\(\s*".*\.rs"' \
    crates/icydb-core/src || true
)"

concat_source_scans="$(
  rg -n --no-heading --color=never 'include_str!\s*\(\s*concat!\([^)]*\.rs' \
    crates/icydb-core/src || true
)"

if [[ -n "$literal_source_scans" || -n "$concat_source_scans" ]]; then
  echo "[ERROR] include_str!-based source text scans are prohibited in crates/icydb-core/src." >&2
  echo "[ERROR] Use structural/type/signature/invariant tests instead." >&2
  if [[ -n "$literal_source_scans" ]]; then
    echo "$literal_source_scans" >&2
  fi
  if [[ -n "$concat_source_scans" ]]; then
    echo "$concat_source_scans" >&2
  fi
  exit 1
fi

echo "[OK] No include_str!-based source text architecture scans detected."
