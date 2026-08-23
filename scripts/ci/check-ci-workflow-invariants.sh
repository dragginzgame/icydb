#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

status=0

fail() {
  echo "[ERROR] $1" >&2
  status=1
}

shopt -s nullglob
workflow_files=(.github/workflows/*.yml .github/workflows/*.yaml)
if [[ ${#workflow_files[@]} -eq 0 ]]; then
  echo "[ERROR] no GitHub Actions workflows found" >&2
  exit 1
fi

if rg -n 'runs-on:[[:space:]]+ubuntu-latest' "${workflow_files[@]}"; then
  fail "workflow runners must use the fixed Ubuntu 24.04 image"
fi

mutable_actions="$({
  rg -n 'uses:[[:space:]]+[^[:space:]#]+@' "${workflow_files[@]}" || true
} | awk '$0 !~ /@[0-9a-f]{40}([[:space:]]|$)/')"
if [[ -n "$mutable_actions" ]]; then
  printf '%s\n' "$mutable_actions" >&2
  fail "third-party actions must use immutable 40-character revisions"
fi

for workflow in "${workflow_files[@]}"; do
  run_count="$(rg -c '^[[:space:]]+runs-on:' "$workflow" || true)"
  timeout_count="$(rg -c '^[[:space:]]+timeout-minutes:' "$workflow" || true)"
  if [[ "$run_count" != "$timeout_count" ]]; then
    fail "$workflow must bound every runner job with timeout-minutes"
  fi

  if ! rg -q '^permissions:$' "$workflow"; then
    fail "$workflow must declare top-level token permissions"
  fi

  checkout_count="$(rg -c 'uses:[[:space:]]+actions/checkout@' "$workflow" || true)"
  credential_count="$(rg -c 'persist-credentials:[[:space:]]+false' "$workflow" || true)"
  if [[ "$checkout_count" != "$credential_count" ]]; then
    fail "$workflow must disable persisted checkout credentials"
  fi

  if rg -q "(^|[[:space:]\"'])gh[[:space:]]+api([[:space:]\"']|$)" "$workflow" &&
     ! rg -q 'run:[[:space:]]+make install-gh' "$workflow"; then
    fail "$workflow uses gh without the shared make install-gh prerequisite"
  fi
done

if rg -q '^[[:space:]]+tags:' .github/workflows/ci.yml; then
  fail "CI must not duplicate the main release commit through a tag trigger"
fi

for job in static rust check; do
  if ! rg -q "^  ${job}:$" .github/workflows/ci.yml; then
    fail "CI is missing the ${job} validation job"
  fi
done

for lane in core workspace tier-a tier-b; do
  if ! rg -q --fixed-strings -- "- lane: $lane" .github/workflows/ci.yml; then
    fail "CI is missing the $lane Rust validation lane"
  fi
done

if ! rg -q '^[[:space:]]+fail-fast:[[:space:]]+false$' .github/workflows/ci.yml; then
  fail "parallel Rust validation must retain every lane after one lane fails"
fi

if ! rg -q --fixed-strings 'needs: [static, rust]' .github/workflows/ci.yml ||
   ! rg -q --fixed-strings 'needs: [dependency_msrv, check]' .github/workflows/ci.yml; then
  fail "the terminal check identity must aggregate every validation lane before Wasm evidence"
fi

for target in ci-static ci-core ci-workspace ci-sql-tier-a ci-sql-tier-b; do
  if ! rg -q "^${target}:$" Makefile; then
    fail "Make is missing the shared $target validation authority"
  fi
  if [[ "$target" != "ci-static" ]] &&
     ! rg -q --fixed-strings "make_target: $target" .github/workflows/ci.yml; then
    fail "CI is missing the shared $target validation authority"
  fi
done

if ! rg -q 'run:[[:space:]]+make ci-static' .github/workflows/ci.yml ||
   ! rg -q --fixed-strings 'make "$MAKE_TARGET"' .github/workflows/ci.yml; then
  fail "CI jobs must consume the shared local validation targets"
fi

if ! rg -q --fixed-strings 'bash scripts/ci/install-pocketic.sh' .github/workflows/ci.yml ||
   ! rg -q --fixed-strings 'bash scripts/ci/install-pocketic.sh' .github/workflows/sql-performance.yml ||
   ! rg -q --fixed-strings 'scripts/ci/run-with-pocketic-server.sh' Makefile ||
   ! rg -q --fixed-strings 'ICYDB_POCKET_IC_SERVER_URL' testing/integration/src/lib.rs; then
  fail "PocketIC workflows must install one locked binary and Tier B must use one governed server"
fi

tier_b_perf_target_refs="$(rg -c --fixed-strings '_ci-tier-b-0-237-perf-regressions' Makefile || true)"
if [[ "$tier_b_perf_target_refs" -lt 3 ]] ||
   ! rg -q '^_ci-tier-b-0-237-perf-regressions:$' Makefile ||
   ! rg -q --fixed-strings 'sql_perf_0_237_' Makefile; then
  fail "Tier B must retain the fixed 0.237 SQL performance regression gates"
fi

if rg -q '^[[:space:]]+CARGO_HOME:' .github/workflows/ci.yml &&
   ! rg -q --fixed-strings \
     "printf '%s\\n' \"\$CARGO_HOME/bin\" >> \"\$GITHUB_PATH\"" \
     .github/workflows/ci.yml; then
  fail "repo-local Cargo installs must expose their bin directory to later CI steps"
fi

if [[ ! -x scripts/ci/run-validation-targets.sh ]] ||
   ! rg -q --fixed-strings 'tee "$log"' scripts/ci/run-validation-targets.sh ||
   ! rg -q --fixed-strings 'Failure details (repeated from the full logs)' scripts/ci/run-validation-targets.sh ||
   ! rg -q --fixed-strings 'target/validation-failures' scripts/ci/run-validation-targets.sh ||
   ! rg -q --fixed-strings 'Full failure log retained at:' scripts/ci/run-validation-targets.sh ||
   ! rg -q --fixed-strings 'Latest complete failure log:' scripts/ci/run-validation-targets.sh ||
   ! rg -q --fixed-strings 'GITHUB_STEP_SUMMARY' scripts/ci/run-validation-targets.sh ||
   ! rg -q --fixed-strings 'ICYDB_VALIDATION_RUNNER_DEPTH' scripts/ci/run-validation-targets.sh ||
   ! rg -q '^validate-fast:$' Makefile ||
   ! rg -q '^test-integration-feedback:$' Makefile ||
   ! rg -q '^test-durability:$' Makefile; then
  fail "the local fast, focused, grouped, and failure-detail validation feedback loop is incomplete"
fi

for owned_path in \
  '/.github/workflows/' \
  '/.github/dependabot.yml' \
  '/scripts/ci/' \
  '/Makefile' \
  '/rust-toolchain.toml'
do
  if ! rg -q --fixed-strings "$owned_path @dragginzgame" .github/CODEOWNERS; then
    fail "CODEOWNERS is missing CI authority $owned_path"
  fi
done

if ! rg -q '^install-gh:$' Makefile ||
   ! rg -q 'bash scripts/ci/install-gh\.sh' Makefile ||
   ! rg -q 'run:[[:space:]]+make install-gh' .github/workflows/ci.yml; then
  fail "the shared GitHub CLI installation path is incomplete"
fi

if [[ $status -ne 0 ]]; then
  exit "$status"
fi

echo "CI workflow invariants passed"
