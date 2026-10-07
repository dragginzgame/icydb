#!/usr/bin/env bash
set -euo pipefail

[[ $# -gt 0 ]] || { echo 'usage: run-nonempty-cargo-test.sh <cargo-test-args...>' >&2; exit 2; }
# Use the caller's workspace; preserve Cargo's arguments and network policy.
output="$(mktemp "${TMPDIR:-/tmp}/nonempty-cargo-test.XXXXXX")"
keep=true
finish() {
    if [[ "$keep" == true ]]; then echo "Test output retained: $output" >&2;
    else rm -f "$output"; fi
}
trap finish EXIT
set +e
CARGO_TERM_COLOR=never cargo test "$@" 2>&1 | tee "$output"
statuses=("${PIPESTATUS[@]}")
set -e
[[ "${statuses[0]}" == 0 ]] || exit "${statuses[0]}"
[[ "${statuses[1]}" == 0 ]] || exit "${statuses[1]}"
passed="$(awk '/^test result: ok\. [0-9]+ passed;/ { total += $4 } END { print total + 0 }' "$output")"
[[ "$passed" -gt 0 ]] || { echo 'selected Cargo tests executed zero passing tests' >&2; exit 3; }
keep=false
echo "selected Cargo tests executed $passed passing test(s)"
