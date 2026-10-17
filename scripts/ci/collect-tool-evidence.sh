#!/usr/bin/env bash
# Shared companions: scripts/ci/select-tool-evidence.sh scripts/ci/archive-evidence.sh
set -euo pipefail
# Caller owns log selections, outer archives, Git custody, uploads and retention.
# Stdout carries the complete archive path (or nothing); status 1 means admitted
# partial selection was archived, status 2 means collection/archive failed.
[[ $# -ge 6 && $(( ($# - 6) % 2 )) == 0 ]] || {
    echo 'usage: collect-tool-evidence.sh NEW-ARCHIVE REPOSITORY DIAGNOSTICS full|compact HOST-CATALOG IC-PINS [ROOT PATH]...' >&2; exit 2;
}
root="${BASH_SOURCE[0]}"
[[ "$root" == /* ]] || root="$PWD/$root"
root="$(cd -P "${root%/*}/../.." && printf '%s/.' "$PWD")"
root="${root%/.}"
output="$1" repository="$2" diagnostics="$3" mode="$4" host_catalog="$5" ic_pins="$6"
shift 6
case "$mode" in full|compact) ;; *) echo 'unknown evidence selection' >&2; exit 2 ;; esac
[[ -d "$diagnostics" && ! -L "$diagnostics" ]] || exit 2
diagnostics="$(cd -P "$diagnostics" && printf '%s/.' "$PWD")"
diagnostics="${diagnostics%/.}"
# These receipt names belong to this invocation; never replace older evidence.
for name in selections.nul selection.log selection-status archive-status; do
    [[ ! -e "$diagnostics/$name" && ! -L "$diagnostics/$name" ]] || exit 2
done
selection=("$mode" "$repository" "$diagnostics")
[[ "$mode" != compact ]] || selection+=("$host_catalog" "$ic_pins")
status=0
bash "$root/scripts/ci/select-tool-evidence.sh" "${selection[@]}" \
    > "$diagnostics/selections.nul" 2> "$diagnostics/selection.log" || status=$?
printf '%s\n' "$status" > "$diagnostics/selection-status"
if [[ "$status" != 0 ]]; then
    cat "$diagnostics/selection.log" >&2
    echo 'Tool evidence collection incomplete; retaining admitted local evidence and diagnostics.' >&2
fi
inputs=("$@")
if [[ "$status" == 0 ]]; then
    while IFS= read -r -d '' selected_root && IFS= read -r -d '' selected_path; do
        inputs+=("$selected_root" "$selected_path")
    done < "$diagnostics/selections.nul"
else
    # A failing producer cannot admit its partial physical-store selections.
    # Archive local references without following them, plus original diagnostics.
    if [[ ! -L "$repository/.tools" && -d "$repository/.tools" ]]; then
        if [[ ! -L "$repository/.tools/rust" && -d "$repository/.tools/rust" ]]; then
            inputs+=("$repository" .tools/rust)
        fi
        for candidate in "$repository"/.tools/host "$repository"/.tools/ic \
            "$repository"/.tools/host-set.* "$repository"/.tools/ic-set.*; do
            [[ -e "$candidate" || -L "$candidate" ]] || continue
            inputs+=("$repository" "${candidate#"$repository"/}")
        done
    fi
fi
# Preserve the caller-owned diagnostic directory alongside admitted payloads.
if [[ "$status" != 0 || -d "$diagnostics/host" || -d "$diagnostics/ic" ]]; then
    inputs+=("${diagnostics%/*}" "${diagnostics##*/}")
fi
if [[ ${#inputs[@]} != 0 ]]; then
    archive_status=0
    printf '0\n' > "$diagnostics/archive-status"
    bash "$root/scripts/ci/archive-evidence.sh" "$output" "${inputs[@]}" || archive_status=$?
    if [[ "$archive_status" != 0 ]]; then
        printf '%s\n' "$archive_status" > "$diagnostics/archive-status"
        exit 2
    fi
fi
[[ "$status" == 0 ]] || exit 1
