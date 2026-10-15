#!/usr/bin/env bash
set -euo pipefail

ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
export PATH="$ROOT/.tools/host/bin:$PATH"

usage() {
    cat <<'EOF'
Usage: cloc.sh [--json] [--manifest PATH] [repository]
       cloc.sh --check-tools

Report Rust runtime/test lines and test-function totals for each Cargo
workspace member. The repository defaults to the current working directory.
--manifest selects one independent Cargo workspace explicitly; its own Cargo
configuration and target directory apply. The default is the Git root workspace.
Prepared host tools beside this script's checkout take precedence over PATH.
--check-tools checks prerequisites without inspecting a workspace or installing.
--json emits one complete document with crate rows and totals on success.
EOF
}

check_tools() {
    local tool missing=""
    for tool in cargo cloc jq; do
        if ! command -v "$tool" >/dev/null 2>&1; then missing="$missing $tool"; fi
    done
    if [[ -n "$missing" ]]; then
        echo "error: missing LOC tools:$missing" >&2
        echo "Run make install-host-tools in $ROOT; prepare the Cargo toolchain separately if missing." >&2
        return 1
    fi
}

selected_manifest=''
json=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --json) json=true; shift ;;
        --manifest)
            [[ $# -ge 2 && -n "$2" && -z "$selected_manifest" ]] || { usage >&2; exit 2; }
            selected_manifest="$2"; shift 2 ;;
        *) break ;;
    esac
done
[[ $# -le 1 ]] || { usage >&2; exit 2; }

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi
if [[ "${1:-}" == --check-tools ]]; then
    check_tools
    exit 0
fi

resolve_directory() (
    directory="$1"
    [[ "$directory" == /* ]] || directory="$PWD/$directory"
    case "$directory" in
        *$'\n'*|*$'\r'*) echo 'error: directory contains line breaks' >&2; exit 1 ;;
    esac
    cd -P "$directory" || exit 1
    case "$PWD" in
        *$'\n'*|*$'\r'*) echo 'error: resolved directory contains line breaks' >&2; exit 1 ;;
    esac
    printf '%s\n' "$PWD"
)

if [[ -n "$selected_manifest" ]]; then
    case "$selected_manifest" in
        *$'\n'*|*$'\r'*) echo 'error: manifest path contains line breaks' >&2; exit 1 ;;
    esac
    [[ -f "$selected_manifest" && ! -L "$selected_manifest" ]] || {
        echo 'error: selected manifest must be a regular file' >&2; exit 1;
    }
    selected_directory="$(resolve_directory "$(dirname "$selected_manifest")")"
    selected_manifest="$selected_directory/$(basename "$selected_manifest")"
fi
requested_root="$(resolve_directory "${1:-${selected_directory:-$PWD}}")"
if repo_root="$(git -C "${requested_root}" rev-parse --show-toplevel 2>/dev/null)"; then
    :
else
    repo_root="$requested_root"
fi

repo_root="$(resolve_directory "$repo_root")"
manifest_path="${selected_manifest:-${repo_root}/Cargo.toml}"
case "$manifest_path" in
    "$repo_root/"*) ;;
    *) echo 'error: selected workspace must belong to the selected checkout' >&2; exit 1 ;;
esac
if [[ ! -f "${manifest_path}" ]]; then
    echo "error: Cargo workspace manifest not found: ${manifest_path}" >&2
    exit 1
fi

check_tools

if ! metadata="$(
    cd "$(dirname "$manifest_path")" &&
    RUSTUP_AUTO_INSTALL=0 cargo metadata \
        --format-version 1 \
        --manifest-path "${manifest_path}" \
        --locked --offline \
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

if ! target_path="$(jq -er '.target_directory | select(type == "string" and startswith("/"))' <<<"$metadata")"; then
    echo 'error: Cargo metadata has no absolute target directory' >&2
    exit 1
fi
target_paths=("$target_path")
# Cargo preserves a configured alias. find does not follow source symlinks, but
# can reach the existing output through its physical path elsewhere in a member.
# A target that has never been built need not exist.
if [[ -d "$target_path" ]]; then
    target_paths+=("$(cd "$target_path" && pwd -P)")
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

if [[ "$json" == false ]]; then
    printf "%-*s %12s %12s %10s %9s %10s\n" \
        "${crate_column_width}" \
        "crate" \
        "runtime_loc" \
        "test_loc" \
        "test_%" \
        "test_fns" \
        "inline_fns"
    print_divider
fi

# Both projections use the same counted rows, including the final totals.
print_counts() {
    if [[ "$json" == true ]]; then
        jq -cn --arg crate "$1" --argjson runtime_loc "$2" --argjson test_loc "$3" \
            --argjson test_pct "$4" --argjson test_fns "$5" --argjson inline_fns "$6" \
            '{crate:$crate,runtime_loc:$runtime_loc,test_loc:$test_loc,test_pct:$test_pct,
              test_fns:$test_fns,inline_fns:$inline_fns}' >> "$FILE_LIST_DIR/rows.jsonl"
    else
        printf '%-*s %12d %12d %9s%% %9d %10d\n' "$crate_column_width" "$@"
    fi
}

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

    # Cargo owns the build-output identity, including configured target paths.
    # Prune it before either LOC or test counting, even inside a package.
    find_args=("$crate_path")
    excluded_paths=("${target_paths[@]}")
    for member_row in "${crates[@]}"; do
        IFS=$'\t' read -r _ member_path <<<"$member_row"
        if [[ "$member_path" == "$crate_path/"* ]]; then
            excluded_paths+=("$member_path")
        fi
    done
    for excluded_path in "${excluded_paths[@]}"; do
        # find's -path takes a glob; escape literal path metacharacters.
        excluded_pattern="${excluded_path//\\/\\\\}"
        excluded_pattern="${excluded_pattern//\*/\\*}"
        excluded_pattern="${excluded_pattern//\?/\\?}"
        excluded_pattern="${excluded_pattern//\[/\\[}"
        find_args+=(-path "$excluded_pattern" -prune -o)
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

    print_counts \
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

if [[ "$json" == false ]]; then print_divider; fi
print_counts \
    "TOTAL" \
    "${total_runtime_loc}" \
    "${total_test_loc}" \
    "${total_test_pct}" \
    "${total_test_fns}" \
    "${total_inline_test_fns}"
if [[ "$json" == true ]]; then
    jq -s --arg manifest "$manifest_path" \
        '{manifest:$manifest,crates:.[0:-1],totals:(.[-1] | del(.crate))}' "$FILE_LIST_DIR/rows.jsonl"
fi
