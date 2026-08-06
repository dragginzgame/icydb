#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

status=0

require_file() {
  local file="$1"

  if [[ ! -f "$file" ]]; then
    echo "[ERROR] Missing required durability document: $file" >&2
    status=1
  fi
}

require_pattern() {
  local file="$1"
  local pattern="$2"
  local message="$3"

  require_file "$file"
  if [[ ! -f "$file" ]]; then
    return
  fi

  if ! rg -n --no-heading --color=never "$pattern" "$file" >/dev/null; then
    echo "[ERROR] $message ($file, pattern: $pattern)" >&2
    status=1
  fi
}

# Guard maintained durability contracts only. Archived release evidence and
# changelog topology are historical records, not runtime durability authority.
require_file "docs/contracts/DURABILITY.md"
require_file "docs/contracts/PERSISTED_FORMAT_POLICY.md"
require_file "docs/contracts/PERSISTED_FORMAT_INVENTORY.md"
require_file "docs/operations/DURABILITY_GUIDE.md"

require_pattern \
  "docs/contracts/DURABILITY.md" \
  "supported backup/restore/import product" \
  "durability contract must keep raw backup/restore/import out of scope"
require_pattern \
  "docs/contracts/DURABILITY.md" \
  "does not add persisted checksums" \
  "durability contract must keep the no-checksum decision explicit"
require_pattern \
  "docs/contracts/DURABILITY.md" \
  "proof shapes and regression budgets" \
  "durability contract must not overclaim recovery-size evidence"
require_pattern \
  "docs/contracts/DURABILITY.md" \
  "instruction-budget guarantees" \
  "durability contract must keep production instruction-budget guarantee language"
require_pattern \
  "docs/contracts/DURABILITY.md" \
  "No persisted-format change is introduced by this document" \
  "durability contract must classify itself as documentation-only"

require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "persisted-format policy must link to the active surface inventory"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "^### Test-only harness change$" \
  "persisted-format policy must retain the test-only classification"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "^### Internal pre-1\\.0 hard cut$" \
  "persisted-format policy must retain the pre-1.0 hard-cut classification"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "prior decoder is deleted" \
  "pre-1.0 hard cuts must not retain a compatibility decoder"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "^### Backward-compatible reader extension$" \
  "persisted-format policy must retain the backward-compatible extension classification"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "^### Format-breaking migration$" \
  "persisted-format policy must retain the migration classification"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "Checksums are persisted format" \
  "persisted-format policy must classify checksum bytes"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "Persisted decoders must be bounded and fallible" \
  "persisted-format policy must keep the bounded-decoder rule"

require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "^## Active Durable Surfaces$" \
  "persisted-format inventory must retain the active durable surface table"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Commit control slot" \
  "persisted-format inventory must cover commit control-slot bytes"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Commit marker payload" \
  "persisted-format inventory must cover commit marker payloads"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Journal tail batches and sequences" \
  "persisted-format inventory must cover journal-tail batches"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Raw row envelopes" \
  "persisted-format inventory must cover row envelopes"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Accepted schema snapshots" \
  "persisted-format inventory must cover accepted schema snapshots"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Secondary-index keys" \
  "persisted-format inventory must cover secondary-index keys"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Cursor tokens" \
  "persisted-format inventory must cover cursor tokens"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "Does this change alter durable bytes" \
  "persisted-format inventory must retain the review rule"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_INVENTORY.md" \
  "no persisted-format change" \
  "persisted-format inventory must retain the no-change documentation rule"

format_version_matches="$(
  rg -n --no-heading --color=never \
    'const\s+[A-Z0-9_]*(?:FORMAT_VERSION|CODEC_VERSION|HEADER_VERSION|RECORD_VERSION|WIRE_VERSION|VERSION_CURRENT|CURRENT_VERSION)[A-Z0-9_]*\s*:[^=]+=[^;]*\b(?:[2-9]|[1-9][0-9]+)\b[^;]*;' \
    crates \
    --glob '*.rs' || {
      rg_status=$?
      if [[ $rg_status -ne 1 ]]; then
        exit "$rg_status"
      fi
    }
)"
unexpected_format_versions="$(
  printf '%s\n' "$format_version_matches" |
    rg -v \
      -e '^crates/icydb-core/src/db/commit/marker\.rs:[0-9]+:pub\(in crate::db\) const COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 2;$' \
      -e '^crates/icydb-core/src/db/integrity/progress_store\.rs:[0-9]+:const JOB_RECORD_VERSION: u8 = 2;$' || true
)"
if [[ -n "$unexpected_format_versions" ]]
then
  printf '%s\n' "$unexpected_format_versions"
  echo "[ERROR] An undocumented active format/version constant exceeds version 1." >&2
  status=1
fi

require_pattern \
  "docs/operations/DURABILITY_GUIDE.md" \
  'Use `storage\(journaled\(\.\.\.\)\)` for durable user data' \
  "operator guide must keep journaled storage as the durable lane"
require_pattern \
  "docs/operations/DURABILITY_GUIDE.md" \
  'Use `storage\(heap\(\)\)` only when volatility is intentional' \
  "operator guide must keep heap storage explicitly volatile"
require_pattern \
  "docs/operations/DURABILITY_GUIDE.md" \
  "\\*_many_non_atomic.*already committed prefix" \
  "operator guide must keep non-atomic prefix-commit warning"
require_pattern \
  "docs/operations/DURABILITY_GUIDE.md" \
  "do not claim raw backup/import support" \
  "operator guide checklist must keep backup/import non-claim"

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Durability documentation invariant checks failed." >&2
  exit 1
fi

echo "[OK] Durability documentation invariants verified."
