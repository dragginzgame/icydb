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
