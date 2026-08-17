#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: scripts/ci/run-validation-targets.sh <make-target>..." >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/icydb-validation.XXXXXX")"
trap 'rm -rf "$LOG_DIR"' EXIT

RUNNER_DEPTH="${ICYDB_VALIDATION_RUNNER_DEPTH:-0}"
export ICYDB_VALIDATION_RUNNER_DEPTH="$((RUNNER_DEPTH + 1))"
MAX_FAILURE_DETAIL_LINES=160
FAILURE_PATTERN='---- .* stdout ----|panicked at|failures:|test result: FAILED|error(\[[A-Z0-9]+\])?:|target failed|make(\[[0-9]+\])?: \*\*\*'

failed_targets=()
targets=()
results=()
elapsed_seconds=()
logs=()

print_failure_detail() {
  local log="$1"
  local target="$2"
  local clean_log="${log}.clean"
  local details
  local inherited

  # Cargo forces ANSI color in CI. Normalize only the temporary diagnostic
  # copy so matching remains deterministic while the live output stays colored.
  LC_ALL=C sed $'s/\033\[[0-9;]*[[:alpha:]]//g' "$log" > "$clean_log"

  inherited="$(grep '^\[validation-detail\]' "$clean_log" || true)"
  if [[ -n "$inherited" ]]; then
    printf '%s\n' "$inherited" |
      awk '!seen[$0]++' |
      tail -n "$MAX_FAILURE_DETAIL_LINES"
    return
  fi

  printf '[validation-detail] Target: %s\n' "$target"

  if command -v rg >/dev/null 2>&1; then
    details="$(rg --color never --no-heading -C 4 -- "$FAILURE_PATTERN" "$clean_log" || true)"
  else
    details="$(grep -E -C 4 -- "$FAILURE_PATTERN" "$clean_log" || true)"
  fi
  if [[ -z "$details" ]]; then
    details="$(tail -n 80 "$clean_log")"
  fi

  while IFS= read -r line; do
    printf '[validation-detail] %s\n' "$line"
  done < <(printf '%s\n' "$details" | tail -n "$((MAX_FAILURE_DETAIL_LINES - 1))")
}

write_github_summary() {
  if [[ "$RUNNER_DEPTH" != "0" || -z "${GITHUB_STEP_SUMMARY:-}" ]]; then
    return 0
  fi

  {
    echo "### Validation summary"
    echo
    echo "| Result | Seconds | Target |"
    echo "| --- | ---: | --- |"
    for index in "${!targets[@]}"; do
      printf '| %s | %s | %s |\n' \
        "${results[$index]}" \
        "${elapsed_seconds[$index]}" \
        "\`${targets[$index]}\`"
    done

    if [[ ${#failed_targets[@]} -ne 0 ]]; then
      echo
      echo "#### Failure details"
      echo
      echo '```text'
      for index in "${!targets[@]}"; do
        if [[ "${results[$index]}" == "FAIL" ]]; then
          echo
          print_failure_detail "${logs[$index]}" "${targets[$index]}"
        fi
      done
      echo '```'
    fi
  } >> "$GITHUB_STEP_SUMMARY"
}

for target in "$@"; do
  log="$LOG_DIR/${#targets[@]}.log"
  start="$SECONDS"
  if [[ "${GITHUB_ACTIONS:-}" == "true" && "$RUNNER_DEPTH" == "0" ]]; then
    printf '::group::%s\n' "$target"
  else
    printf '\n==> %s\n' "$target"
  fi

  if make --no-print-directory -C "$ROOT" "$target" 2>&1 | tee "$log"; then
    result="PASS"
  else
    failed_targets+=("$target")
    result="FAIL"
  fi

  elapsed="$((SECONDS - start))"
  targets+=("$target")
  results+=("$result")
  elapsed_seconds+=("$elapsed")
  logs+=("$log")
  if [[ "${GITHUB_ACTIONS:-}" == "true" && "$RUNNER_DEPTH" == "0" ]]; then
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

write_github_summary

if [[ ${#failed_targets[@]} -ne 0 ]]; then
  printf '\nFailure details (repeated from the full logs):\n'
  for index in "${!targets[@]}"; do
    if [[ "${results[$index]}" == "FAIL" ]]; then
      echo
      print_failure_detail "${logs[$index]}" "${targets[$index]}"
    fi
  done

  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    printf '::error title=Validation targets failed::%s\n' "${failed_targets[*]}"
  fi
  printf '\nVALIDATION FAILED: %s\n' "${failed_targets[*]}" >&2
  exit 1
fi

echo "VALIDATION PASSED: all requested targets succeeded."
