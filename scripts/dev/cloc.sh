#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if repo_root="$(git -C "${script_dir}" rev-parse --show-toplevel 2>/dev/null)"; then
    :
else
    repo_root="$(cd "${script_dir}/../.." && pwd)"
fi

crates_dir="${repo_root}/crates"

if ! command -v cloc >/dev/null 2>&1; then
    echo "error: cloc not found in PATH" >&2
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "error: jq not found in PATH (required for JSON parsing)" >&2
    exit 1
fi

tests_pattern='(^|/)(tests/|[^/]*tests\.rs$)'

printf "%-20s %12s %12s %10s\n" "crate" "runtime_loc" "test_loc" "test_%"
printf "%-20s %12s %12s %10s\n" "--------------------" "------------" "------------" "--------"

for crate_path in "${crates_dir}"/icydb*; do
    [[ -d "${crate_path}" ]] || continue
    crate_name="$(basename "${crate_path}")"

    # Test LOC (Rust only)
    test_loc=$(cloc "${crate_path}" \
        --fullpath \
        --match-f="${tests_pattern}" \
        --include-lang=Rust \
        --json 2>/dev/null \
        | jq '.Rust.code // 0')

    # Runtime LOC (Rust only)
    runtime_loc=$(cloc "${crate_path}" \
        --fullpath \
        --not-match-f="${tests_pattern}" \
        --include-lang=Rust \
        --json 2>/dev/null \
        | jq '.Rust.code // 0')

    total=$((runtime_loc + test_loc))

    if [[ "${total}" -gt 0 ]]; then
        test_pct=$(awk "BEGIN { printf \"%.1f\", (${test_loc}/${total})*100 }")
    else
        test_pct="0.0"
    fi

    printf "%-20s %12d %12d %9s%%\n" \
        "${crate_name}" \
        "${runtime_loc}" \
        "${test_loc}" \
        "${test_pct}"
done