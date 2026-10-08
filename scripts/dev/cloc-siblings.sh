#!/usr/bin/env bash
set -euo pipefail

ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"

usage() {
    cat <<'EOF'
Usage: cloc-siblings.sh [parent-directory]

Print one Rust LOC summary per immediate Git checkout in the parent directory.
The default is the parent of the checkout containing this script. Counts reuse
cloc.sh and cover each repository's root Cargo workspace, including apps/ and
crates/ members. Separate excluded workspaces are outside that report's scope.

Repositories without a root Cargo.toml show N/A. Failed counts show ERROR and
make the command fail after reporting the remaining repositories. Symlinked
directory aliases are skipped. Requires Git and the tools used by cloc.sh.
The final TOTAL sums successful reports; failures label it TOTAL (partial).
EOF
}

if [[ $# -gt 1 ]]; then
    usage >&2
    exit 2
fi
if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
    usage
    exit 0
fi

if ! parent_root="$(cd "${1:-$ROOT/..}" 2>/dev/null && pwd -P)"; then
    echo "error: parent directory not found: ${1:-$ROOT/..}" >&2
    exit 1
fi
if ! command -v git >/dev/null 2>&1; then
    echo 'error: git not found in PATH' >&2
    exit 1
fi

export LC_ALL=C
shopt -s nullglob dotglob
repos=()
needs_cargo=false
column_width=24
for repo in "${parent_root%/}"/*; do
    [[ -d "$repo" && ! -L "$repo" && -e "$repo/.git" ]] || continue
    name="${repo##*/}"
    if [[ "$name" == *$'\n'* || "$name" == *$'\r'* || "$name" == *$'\t'* ]]; then
        echo 'error: repository names containing tabs or newlines cannot be displayed' >&2
        exit 1
    fi
    repos+=("$repo")
    if [[ -f "$repo/Cargo.toml" ]]; then needs_cargo=true; fi
    if [[ ${#name} -gt $column_width ]]; then column_width=${#name}; fi
done
if [[ ${#repos[@]} == 0 ]]; then
    echo "error: no Git checkouts found in $parent_root" >&2
    exit 1
fi
if [[ "$needs_cargo" == true ]]; then
    bash "$ROOT/scripts/dev/cloc.sh" --check-tools
fi

print_row() {
    printf '%-*s %12s %12s %10s %9s %10s\n' "$column_width" "$@"
}
printf -v divider '%*s' "$column_width" ''
print_row repository runtime_loc test_loc test_% test_fns inline_fns
print_row "${divider// /-}" ------------ ------------ ---------- --------- ----------

status=0
counted_repos=0
total_runtime_loc=0
total_test_loc=0
total_test_fns=0
total_inline_fns=0
for repo in "${repos[@]}"; do
    name="${repo##*/}"
    if ! git_root="$(git -C "$repo" rev-parse --show-toplevel 2>/dev/null)" || [[ "$git_root" != "$repo" ]]; then
        print_row "$name" ERROR ERROR ERROR ERROR ERROR
        echo "error: not a Git checkout root: $repo" >&2
        status=1
        continue
    fi
    if [[ ! -f "$repo/Cargo.toml" ]]; then
        print_row "$name" N/A N/A N/A N/A N/A
        continue
    fi
    # Enter the checkout so Cargo reads its own .cargo/config.toml. Reuse the
    # complete workspace report, publishing only its final totals on success.
    if ! report="$(cd "$repo" && bash "$ROOT/scripts/dev/cloc.sh" "$repo")"; then
        print_row "$name" ERROR ERROR ERROR ERROR ERROR
        echo "error: unable to count $repo" >&2
        status=1
        continue
    fi
    read -r label runtime_loc test_loc test_pct test_fns inline_fns extra <<<"${report##*$'\n'}" || :
    if [[ "$label" != TOTAL || -n "$extra" ||
        ! "$runtime_loc" =~ ^[0-9]+$ || ! "$test_loc" =~ ^[0-9]+$ ||
        ! "$test_pct" =~ ^[0-9]+\.[0-9]+%$ ||
        ! "$test_fns" =~ ^[0-9]+$ || ! "$inline_fns" =~ ^[0-9]+$ ]]; then
        print_row "$name" ERROR ERROR ERROR ERROR ERROR
        echo "error: invalid workspace totals for $repo" >&2
        status=1
        continue
    fi
    print_row "$name" "$runtime_loc" "$test_loc" "$test_pct" "$test_fns" "$inline_fns"
    counted_repos=$((counted_repos + 1))
    total_runtime_loc=$((total_runtime_loc + runtime_loc))
    total_test_loc=$((total_test_loc + test_loc))
    total_test_fns=$((total_test_fns + test_fns))
    total_inline_fns=$((total_inline_fns + inline_fns))
done
print_row "${divider// /-}" ------------ ------------ ---------- --------- ----------
total_label=TOTAL
if [[ "$status" != 0 ]]; then total_label='TOTAL (partial)'; fi
if [[ "$counted_repos" == 0 ]]; then
    print_row "$total_label" N/A N/A N/A N/A N/A
else
    total_pct=0.0%
    if [[ $((total_runtime_loc + total_test_loc)) -gt 0 ]]; then
        total_pct="$(awk -v runtime="$total_runtime_loc" -v tests="$total_test_loc" \
            'BEGIN { printf "%.1f%%", 100 * tests / (runtime + tests) }')"
    fi
    print_row "$total_label" "$total_runtime_loc" "$total_test_loc" "$total_pct" "$total_test_fns" "$total_inline_fns"
fi
exit "$status"
