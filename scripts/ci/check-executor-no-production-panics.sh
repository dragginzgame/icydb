#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"
require_rg "critical runtime no-production-panics invariant"

# Require each inventory independently, including ignored recovery sources.
# Syn excludes complete test-only items; shell never approximates Rust braces.
roots=(
  crates/icydb-core/src/db/executor
  crates/icydb-core/src/db/commit
  crates/icydb-core/src/db/journal
  crates/icydb-core/src/db/startup
)
files=()
for root in "${roots[@]}"; do
  discovered="$(rg --files --hidden --no-ignore "$root" --glob '*.rs' "${COMMON_GLOBS[@]}")"
  while IFS= read -r file; do
    files+=("$file")
  done <<< "$discovered"
done

# Development dependencies are already selected in the root lockfile. Prepare
# them explicitly with make fetch; validation never downloads or changes them.
cargo run --locked --offline --quiet --manifest-path "$ROOT/Cargo.toml" \
  -p icydb-testing-integration --example check_runtime_panics -- "${files[@]}"
