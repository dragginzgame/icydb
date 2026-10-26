#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# A static policy guard only. Codec-owned tests prove actual wire admission;
# this scan does not claim to validate codec behavior or documentation prose.
format_version_matches="$(
  rg -n --no-heading --color=never \
    '(?:const\s+[A-Z0-9_]*(?:VERSION[A-Z0-9_]*|REVISION|EPOCH|GENERATION)\s*:\s*(?:u8|u16|u32|u64|u128|usize)\s*=\s*(?:[2-9]|[1-9][0-9]+)\s*;|const\s+CURRENT\s*:\s*Self\s*=\s*Self\((?:[2-9]|[1-9][0-9]+)\)\s*;)' \
    crates testing \
    --glob '*.rs' || {
      rg_status=$?
      if [[ $rg_status -ne 1 ]]; then
        exit "$rg_status"
      fi
    }
)"
failed=false
while IFS=: read -r source_path source_line declaration; do
  [[ -n "$source_path" ]] || continue
  declaration="${declaration#"${declaration%%[![:space:]]*}"}"
  # Exact owner/name/type/value exceptions: semantic job identity inputs are
  # not wire discriminators. Changes still require retained-job review.
  case "$source_path:$declaration" in
    'crates/icydb-core/src/db/executor/mutation/job.rs:const RESUMABLE_UPDATE_ENTITY_REVISION_POLICY_REVISION: u32 = 2;'|\
    'crates/icydb-core/src/db/executor/mutation/job.rs:const RESUMABLE_UPDATE_MANAGED_WRITE_TIME_POLICY_REVISION: u32 = 2;'|\
    'crates/icydb-core/src/db/executor/mutation/job.rs:const RESUMABLE_UPDATE_EXECUTION_FAILURE_POLICY_REVISION: u32 = 2;') continue ;;
    # Fixed domain-history fixtures, never production format discriminators.
    'crates/icydb-core/src/db/session/write/identity_pre_key_tests/replay_construction_tests/candidate_authority_tests.rs:const CANDIDATE_GENERATION: u64 = 9;'|\
    'testing/integration/tests/group_path_measurement_contract.rs:const ROUNDS_PER_EPOCH: u8 = 60;') continue ;;
  esac
  printf '%s:%s:%s\n' "$source_path" "$source_line" "$declaration"
  failed=true
done <<< "$format_version_matches"
if [[ "$failed" == true ]]; then
  echo "[ERROR] An unclassified format/version or revision/epoch/generation constant exceeds 1." >&2
  exit 1
fi

echo "[OK] Persisted-format version policy scan passed."
