#!/usr/bin/env bash
# Shared companions: scripts/dev/cloc-classify.pl
set -euo pipefail

ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
export PATH="$ROOT/.tools/host/bin:$PATH"
# Parent reporting enters child checkouts; preserve the caller's explicit route.
if [[ -n "${TMPDIR:-}" && "$TMPDIR" != /* ]]; then export TMPDIR="$PWD/$TMPDIR"; fi

usage() {
    cat <<'EOF'
Usage: cloc.sh [options] [directory | crate[::module]]

Count Rust LOC by category. Defaults to the current workspace;
a parent directory reports its sibling repositories.

Options:
  -d N            Show crates and N module levels (0 = crates only)
  --json          Full file counts and diagnostics for one workspace
  --manifest PATH Select an independent Cargo workspace
  --check-tools   Check prerequisites
  -h, --help      Show this help

Examples:
  cloc.sh -d 1    Top-level modules beneath each crate
  cloc.sh -d 2 .. Sibling crates with two module levels
  cloc.sh -d 1 my-crate::parser  Drill into one module

??? = unclassified LOC. Counting details: docs/fleet-tools.md
EOF
}

check_tools() {
    local tool missing=""
    for tool in cargo cloc jq perl; do
        if ! command -v "$tool" >/dev/null 2>&1; then missing="$missing $tool"; fi
    done
    if [[ -n "$missing" ]]; then
        echo "error: missing LOC tools:$missing" >&2
        echo "Run make install-host-tools in $ROOT; prepare the Cargo toolchain separately if missing." >&2
        return 1
    fi
}

report_repositories() (
    parent_root="$1"
    scratch="${TMPDIR:-$ROOT/target/tmp}"
    mkdir -p -- "$scratch"
    report_dir="$(mktemp -d "$scratch/shared-cloc-parent.XXXXXX")"
    trap 'rm -rf "$report_dir"' EXIT
    export LC_ALL=C
    shopt -s nullglob dotglob
    status=0
    count=0
    repositories=()
    for repo in "$parent_root"/*; do
        [[ -d "$repo" && ! -L "$repo" && -e "$repo/.git" ]] || continue
        repositories+=("$repo")
    done
    progress_width=0
    progress_active=false
    finish_progress() {
        if [[ "$progress_active" == true ]]; then
            printf '\r%*s\r' "$progress_width" '' >&2
            progress_active=false
        fi
    }
    : > "$report_dir/rows"
    for repo in ${repositories[@]+"${repositories[@]}"}; do
        count=$((count + 1))
        name="${repo##*/}"
        case "$name" in *$'\n'*|*$'\r'*|*$'\t'*) echo 'error: repository name contains line breaks or tabs' >&2; exit 1 ;; esac
        progress="cloc: scanning $count/${#repositories[@]} $name"
        if [[ ${#progress} -gt "$progress_width" ]]; then progress_width=${#progress}; fi
        printf '\r%-*s' "$progress_width" "$progress" >&2
        progress_active=true
        result=n/a
        if ! git_root="$(git -C "$repo" rev-parse --show-toplevel 2>/dev/null)" || [[ "$git_root" != "$repo" ]]; then
            result=ERROR
        elif [[ -f "$repo/Cargo.toml" ]]; then
            child_status=0
            (cd "$repo" && bash "$ROOT/scripts/dev/cloc.sh" --json "$repo") > "$report_dir/child.json" 2> "$report_dir/child.err" || child_status=$?
            if [[ -s "$report_dir/child.err" ]]; then
                finish_progress
                cat "$report_dir/child.err" >&2
            fi
            if [[ "$child_status" == 0 ]]; then
                jq -cn --arg name "$name" --slurpfile report "$report_dir/child.json" \
                    '{name:$name,report:$report[0]}' >> "$report_dir/rows"
                continue
            fi
            result=ERROR
        fi
        if [[ "$result" == ERROR ]]; then finish_progress; status=1; echo "error: unable to count $repo" >&2; fi
        jq -cn --arg name "$name" --arg status "$result" '{name:$name,status:$status}' >> "$report_dir/rows"
    done
    finish_progress
    [[ "$count" != 0 ]] || { echo "error: no Git checkouts found in $parent_root" >&2; exit 1; }
    jq -s --argjson partial "$status" '
        {workspaces:.,partial:($partial != 0),totals:
          (reduce .[] as $w ({library_app:0,tests:0,examples:0,benchmarks:0,build_tooling:0,unclassified:0,total:0};
            if $w.report then . as $sum | with_entries(.value += $w.report.totals[.key]) else . end))}
        | if all(.workspaces[]; .report == null) then .totals |= with_entries(.value="n/a") else . end
    ' "$report_dir/rows" > "$report_dir/report.json"
    render_table "$report_dir/report.json"
    exit "$status"
)

depth=''
selected_manifest=''
json=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        -d)
            [[ $# -ge 2 && "$2" =~ ^(0|[1-9][0-9]*)$ && ${#2} -le 6 ]] || { usage >&2; exit 2; }
            depth="$2"; shift 2 ;;
        --json) json=true; shift ;;
        --manifest)
            [[ $# -ge 2 && -n "$2" && -z "$selected_manifest" ]] || { usage >&2; exit 2; }
            selected_manifest="$2"; shift 2 ;;
        --help|-h|--check-tools) break ;;
        --) shift; break ;;
        -*) usage >&2; exit 2 ;;
        *) break ;;
    esac
done
[[ $# -le 1 ]] || { usage >&2; exit 2; }

render_table() {
    local flags=()
    if [[ -n "$depth" ]]; then flags=(-d "$depth"); fi
    perl "$ROOT/scripts/dev/cloc-classify.pl" --table "$1" ${flags[@]+"${flags[@]}"}
}

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
    printf '%s/.' "$PWD"
)

if [[ -n "$selected_manifest" ]]; then
    case "$selected_manifest" in
        *$'\n'*|*$'\r'*) echo 'error: manifest path contains line breaks' >&2; exit 1 ;;
    esac
    [[ -f "$selected_manifest" && ! -L "$selected_manifest" ]] || {
        echo 'error: selected manifest must be a regular file' >&2; exit 1;
    }
    selected_directory="$(resolve_directory "$(dirname "$selected_manifest")")"
    selected_directory="${selected_directory%/.}"
    selected_manifest="$selected_directory/$(basename "$selected_manifest")"
fi
selection=''
requested_directory="${1:-${selected_directory:-$PWD}}"
if [[ $# == 1 && ! -d "$1" && "$1" != */* ]]; then
    selection="$1"
    requested_directory="${selected_directory:-$PWD}"
fi
requested_root="$(resolve_directory "$requested_directory")"
requested_root="${requested_root%/.}"
has_checkouts=false
for child in "$requested_root"/* "$requested_root"/.[!.]*; do
    if [[ -d "$child" && ! -L "$child" && -e "$child/.git" ]]; then has_checkouts=true; break; fi
done
if [[ -z "$selected_manifest" && ! -f "$requested_root/Cargo.toml" && ! -e "$requested_root/.git" ]] &&
   { [[ "$has_checkouts" == true ]] || ! git -C "$requested_root" rev-parse --show-toplevel >/dev/null 2>&1; }; then
    [[ "$json" == false ]] || { echo 'error: --json requires one workspace' >&2; exit 2; }
    report_repositories "$requested_root"
    exit $?
fi
if repo_root="$(git -C "${requested_root}" rev-parse --show-toplevel 2>/dev/null)"; then
    :
else
    repo_root="$requested_root"
fi

repo_root="$(resolve_directory "$repo_root")"
repo_root="${repo_root%/.}"
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

if [[ -n "$selection" ]]; then
    crate="${selection%%::*}"
    if ! metadata="$(printf '%s\n' "$metadata" | jq -e --arg crate "$crate" '
        .workspace_members as $members |
        .packages |= map(select(.name == $crate and (.id as $id | $members | index($id)))) |
        if (.packages | length) == 1 then .workspace_members = [.packages[0].id]
        else error("unknown or ambiguous workspace crate: " + $crate) end
    ')"; then exit 1; fi
fi

scratch="${TMPDIR:-$(printf '%s\n' "$metadata" | jq -er .target_directory)/tmp}"
mkdir -p -- "$scratch"
FILE_LIST_DIR="$(mktemp -d "$scratch/shared-tooling-cloc.XXXXXX")"
trap 'rm -rf "$FILE_LIST_DIR"' EXIT
printf '%s\n' "$metadata" > "$FILE_LIST_DIR/metadata.json"
perl "$ROOT/scripts/dev/cloc-classify.pl" "$FILE_LIST_DIR/metadata.json" "$manifest_path" "$FILE_LIST_DIR" > "$FILE_LIST_DIR/report.json"
if [[ -n "$selection" ]]; then
    perl "$ROOT/scripts/dev/cloc-classify.pl" --select "$FILE_LIST_DIR/report.json" "$selection" > "$FILE_LIST_DIR/selected.json"
    mv "$FILE_LIST_DIR/selected.json" "$FILE_LIST_DIR/report.json"
fi
if [[ "$json" == true ]]; then
    cat "$FILE_LIST_DIR/report.json"
else
    render_table "$FILE_LIST_DIR/report.json"
fi
