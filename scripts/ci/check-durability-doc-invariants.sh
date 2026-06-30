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

require_file "docs/contracts/DURABILITY.md"
require_file "docs/contracts/PERSISTED_FORMAT_POLICY.md"
require_file "docs/operations/DURABILITY_GUIDE.md"
require_file "docs/design/0.191-durability-productization-format-policy/0.191-evidence.md"
require_file "docs/design/0.191-durability-productization-format-policy/streaming-recovery-followup.md"
require_file "docs/changelog/0.191.md"

require_pattern \
  "CHANGELOG.md" \
  "Detailed notes: \\[docs/changelog/0\\.191\\.md\\]\\(docs/changelog/0\\.191\\.md\\)" \
  "root changelog must link to the 0.191 detailed changelog"
require_pattern \
  "docs/changelog/0.191.md" \
  "^## 0\\.191\\.1$" \
  "0.191 detailed changelog must include the current 0.191.1 slice"

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
  "^### Test-only harness change$" \
  "persisted-format policy must retain the test-only classification"
require_pattern \
  "docs/contracts/PERSISTED_FORMAT_POLICY.md" \
  "^### Internal pre-1\\.0 hard cut$" \
  "persisted-format policy must retain the pre-1.0 hard-cut classification"
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

require_pattern \
  "docs/design/0.191-durability-productization-format-policy/0.191-evidence.md" \
  "Persisted-format classification: no persisted-format change" \
  "0.191 evidence must classify documentation decisions as no persisted-format change"
require_pattern \
  "docs/design/0.191-durability-productization-format-policy/0.191-evidence.md" \
  "recovery_startup_rebuilds_mixed_index_shapes_host_floor" \
  "0.191 evidence must retain mixed-index recovery proof"
require_pattern \
  "docs/design/0.191-durability-productization-format-policy/0.191-evidence.md" \
  "sql_perf_journaled_upgrade_guarded_reentry_stays_bounded" \
  "0.191 evidence must retain PocketIC upgrade/reentry proof"
require_pattern \
  "docs/design/0.191-durability-productization-format-policy/0.191-evidence.md" \
  "^## Closeout Checklist$" \
  "0.191 evidence must retain closeout checklist"

require_pattern \
  "docs/design/0.191-durability-productization-format-policy/streaming-recovery-followup.md" \
  "Streaming recovery must be an explicit recovery mode" \
  "streaming follow-up must retain explicit recovery-mode requirement"
require_pattern \
  "docs/design/0.191-durability-productization-format-policy/streaming-recovery-followup.md" \
  "Progress bytes are persisted format" \
  "streaming follow-up must retain persisted progress classification"

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Durability documentation invariant checks failed." >&2
  exit 1
fi

echo "[OK] Durability documentation invariants verified."
