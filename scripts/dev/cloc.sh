#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if repo_root="$(git -C "${script_dir}" rev-parse --show-toplevel 2>/dev/null)"; then
    :
else
    repo_root="$(cd "${script_dir}/../.." && pwd)"
fi
crates_dir="${repo_root}/crates"
target_dir="${crates_dir}"

# Match:
# 1) any file under a tests/ directory at any depth
# 2) any filename ending in tests.rs (covers tests.rs and *_tests.rs)
tests_pattern='(^|/)(tests/|[^/]*tests\.rs$)'

if ! command -v cloc >/dev/null 2>&1; then
    echo "error: cloc is not installed or not in PATH" >&2
    exit 1
fi

if [[ ! -d "${crates_dir}" ]]; then
    echo "error: crates directory not found at ${crates_dir}" >&2
    exit 1
fi

# Optional first argument can scope to a subdirectory under crates/.
# Example: scripts/dev/cloc.sh icydb-core/src/db --include-lang=Rust
if [[ $# -gt 0 && "${1}" != -* ]]; then
    scope_path="${1}"
    target_dir="${crates_dir}/${scope_path}"
    shift

    if [[ ! -d "${target_dir}" ]]; then
        echo "error: scoped directory not found at ${target_dir}" >&2
        exit 1
    fi
fi

echo "CLOC target: ${target_dir}"
echo "Test match pattern: ${tests_pattern}"
echo

echo "=== Test files ==="
cloc "${target_dir}" --fullpath --match-f="${tests_pattern}" "$@"
echo

echo "=== Non-test files ==="
cloc "${target_dir}" --fullpath --not-match-f="${tests_pattern}" "$@"
