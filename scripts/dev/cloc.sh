#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: cloc.sh [repository]

Report Rust runtime/test lines and test-function totals for each Cargo
workspace member. The repository defaults to the current working directory.
EOF
}

if [[ "$#" -gt 1 ]]; then
    usage >&2
    exit 2
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

requested_root="${1:-${PWD}}"
if repo_root="$(git -C "${requested_root}" rev-parse --show-toplevel 2>/dev/null)"; then
    :
elif repo_root="$(cd "${requested_root}" 2>/dev/null && pwd)"; then
    :
else
    echo "error: repository directory not found: ${requested_root}" >&2
    exit 1
fi

manifest_path="${repo_root}/Cargo.toml"
if [[ ! -f "${manifest_path}" ]]; then
    echo "error: Cargo workspace manifest not found: ${manifest_path}" >&2
    exit 1
fi

for command in cargo cloc jq; do
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "error: ${command} not found in PATH" >&2
        exit 1
    fi
done

if ! metadata="$(
    cargo metadata \
        --format-version 1 \
        --manifest-path "${manifest_path}" \
        --no-deps
)"; then
    echo "error: unable to resolve Cargo workspace metadata" >&2
    exit 1
fi

if ! crate_rows="$(
    jq -r '
        .workspace_members as $members
        | .packages[]
        | select(.id as $id | $members | index($id))
        | [.name, (.manifest_path | sub("/Cargo.toml$"; ""))]
        | @tsv
    ' <<<"${metadata}"
)"; then
    echo "error: unable to parse Cargo workspace metadata" >&2
    exit 1
fi

if [[ -z "${crate_rows}" ]]; then
    echo "error: Cargo workspace contains no packages" >&2
    exit 1
fi

FILE_LIST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/shared-tooling-cloc.XXXXXX")"
trap 'rm -rf "$FILE_LIST_DIR"' EXIT

crates=()
while IFS= read -r crate_row; do
    crates[${#crates[@]}]="${crate_row}"
done < <(printf '%s\n' "${crate_rows}" | sort)

tests_pattern='(^|/)(tests/|[^/]*tests\.rs$)'
test_attr_pattern='^[[:space:]]*#\[(tokio::)?test([[:space:]]|\(|\])'
crate_column_width=24
total_runtime_loc=0
total_test_loc=0
total_test_fns=0
total_inline_test_fns=0

for crate_row in "${crates[@]}"; do
    IFS=$'\t' read -r crate_name _ <<<"${crate_row}"
    if [[ "${#crate_name}" -gt "${crate_column_width}" ]]; then
        crate_column_width="${#crate_name}"
    fi
done

printf -v crate_divider "%*s" "${crate_column_width}" ""
crate_divider="${crate_divider// /-}"

print_divider() {
    printf "%-*s %12s %12s %10s %9s %10s\n" \
        "${crate_column_width}" \
        "${crate_divider}" \
        "------------" \
        "------------" \
        "--------" \
        "---------" \
        "----------"
}

printf "%-*s %12s %12s %10s %9s %10s\n" \
    "${crate_column_width}" \
    "crate" \
    "runtime_loc" \
    "test_loc" \
    "test_%" \
    "test_fns" \
    "inline_fns"
print_divider

# Use the same file lists for LOC and test-attribute counts.
count_test_fns() {
    local file_list="$1"
    local total=0
    local rust_file
    local file_count

    while IFS= read -r rust_file; do
        file_count=$(grep -Ec "${test_attr_pattern}" "${rust_file}" || true)
        total=$((total + file_count))
    done <"$file_list"

    printf "%d\n" "${total}"
}

for crate_row in "${crates[@]}"; do
    IFS=$'\t' read -r crate_name crate_path <<<"${crate_row}"

    # Each member owns its subtree, excluding any nested workspace members.
    find_args=("$crate_path")
    for member_row in "${crates[@]}"; do
        IFS=$'\t' read -r _ member_path <<<"$member_row"
        if [[ "$member_path" == "$crate_path/"* ]]; then
            # find's -path takes a glob; escape literal path metacharacters.
            member_pattern="${member_path//\\/\\\\}"
            member_pattern="${member_pattern//\*/\\*}"
            member_pattern="${member_pattern//\?/\\?}"
            member_pattern="${member_pattern//\[/\\[}"
            find_args+=(-path "$member_pattern" -prune -o)
        fi
    done
    find "${find_args[@]}" -type f -name '*.rs' -print0 >"$FILE_LIST_DIR/files"
    : >"$FILE_LIST_DIR/runtime"
    : >"$FILE_LIST_DIR/tests"
    while IFS= read -r -d '' rust_file; do
        # cloc's --list-file format has one literal path per line.
        if [[ "$rust_file" == *$'\n'* ]]; then
            echo "error: cloc file lists cannot represent paths containing newlines" >&2
            exit 1
        fi
        relative_file="${rust_file#"$crate_path/"}"
        if [[ "$relative_file" =~ ${tests_pattern} ]]; then
            printf '%s\n' "$rust_file" >>"$FILE_LIST_DIR/tests"
        else
            printf '%s\n' "$rust_file" >>"$FILE_LIST_DIR/runtime"
        fi
    done <"$FILE_LIST_DIR/files"

    test_loc=$(cloc --list-file="$FILE_LIST_DIR/tests" \
        --include-lang=Rust \
        --json 2>/dev/null \
        | jq '.Rust.code // 0')

    runtime_loc=$(cloc --list-file="$FILE_LIST_DIR/runtime" \
        --include-lang=Rust \
        --json 2>/dev/null \
        | jq '.Rust.code // 0')

    inline_test_fns="$(count_test_fns "$FILE_LIST_DIR/runtime")"
    test_fns="$(count_test_fns "$FILE_LIST_DIR/tests")"
    test_fns=$((test_fns + inline_test_fns))
    crate_loc=$((runtime_loc + test_loc))

    if [[ "${crate_loc}" -gt 0 ]]; then
        test_pct=$(awk "BEGIN { printf \"%.1f\", (${test_loc}/${crate_loc})*100 }")
    else
        test_pct="0.0"
    fi

    printf "%-*s %12d %12d %9s%% %9d %10d\n" \
        "${crate_column_width}" \
        "${crate_name}" \
        "${runtime_loc}" \
        "${test_loc}" \
        "${test_pct}" \
        "${test_fns}" \
        "${inline_test_fns}"

    total_runtime_loc=$((total_runtime_loc + runtime_loc))
    total_test_loc=$((total_test_loc + test_loc))
    total_test_fns=$((total_test_fns + test_fns))
    total_inline_test_fns=$((total_inline_test_fns + inline_test_fns))
done

total_loc=$((total_runtime_loc + total_test_loc))
if [[ "${total_loc}" -gt 0 ]]; then
    total_test_pct=$(awk "BEGIN { printf \"%.1f\", (${total_test_loc}/${total_loc})*100 }")
else
    total_test_pct="0.0"
fi

print_divider
printf "%-*s %12d %12d %9s%% %9d %10d\n" \
    "${crate_column_width}" \
    "TOTAL" \
    "${total_runtime_loc}" \
    "${total_test_loc}" \
    "${total_test_pct}" \
    "${total_test_fns}" \
    "${total_inline_test_fns}"
