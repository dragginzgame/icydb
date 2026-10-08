#!/usr/bin/env bash
set -euo pipefail

# IcyDB selects the repository and evidence root; the shared runner owns target
# execution, per-target evidence and complete batch logs.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VALIDATION_REPOSITORY_ROOT="$ROOT" \
  VALIDATION_FAILURE_LOG_DIR="${ICYDB_VALIDATION_FAILURE_LOG_DIR:-$ROOT/target/validation-failures}" \
  exec bash "$ROOT/scripts/ci/run-validation-targets.sh" "$@"
