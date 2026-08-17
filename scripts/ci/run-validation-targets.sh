#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: scripts/ci/run-validation-targets.sh <make-target>..." >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
failed_targets=()
targets=()
results=()
elapsed_seconds=()

for target in "$@"; do
  start="$SECONDS"
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    printf '::group::%s\n' "$target"
  else
    printf '\n==> %s\n' "$target"
  fi

  if make --no-print-directory -C "$ROOT" "$target"; then
    result="PASS"
  else
    failed_targets+=("$target")
    result="FAIL"
  fi

  elapsed="$((SECONDS - start))"
  targets+=("$target")
  results+=("$result")
  elapsed_seconds+=("$elapsed")
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    printf '::endgroup::\n'
  fi
done

printf '\nValidation summary:\n'
for index in "${!targets[@]}"; do
  printf '  %-4s %5ss  %s\n' \
    "${results[$index]}" \
    "${elapsed_seconds[$index]}" \
    "${targets[$index]}"
done

if [[ ${#failed_targets[@]} -ne 0 ]]; then
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    printf '::error title=Validation targets failed::%s\n' "${failed_targets[*]}"
  fi
  printf 'Failed targets: %s\n' "${failed_targets[*]}" >&2
  exit 1
fi

echo "All validation targets passed."
