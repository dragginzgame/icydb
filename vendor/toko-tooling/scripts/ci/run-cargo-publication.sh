#!/usr/bin/env bash
# Shared companions: scripts/ci/check-make-execution.sh scripts/ci/cargo-publication-paths.sh
set -euo pipefail

# Execute one caller-admitted Cargo publication, retaining intent and results.
# Requires Bash 3.2, Cargo, Git, tee and standard Unix tools. No setup or retry.
fail() { printf 'publication attempt refused: %s\n' "$*" >&2; exit 2; }
[[ $# -ge 6 ]] || fail 'usage: run-cargo-publication.sh check|publish ATTEMPT SOURCE REGISTRY --package NAME@VERSION [OPTIONS]'
mode="$1" attempt="$2" source="$3" registry="$4"
shift 4
[[ "$mode" == check || "$mode" == publish ]] || fail 'select check or publish'
[[ "$source" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || fail 'invalid source commit'
[[ "$registry" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || fail 'select a named registry'
path_ok() { case "$1" in *$'\r'*|*$'\n'*) fail 'directory paths must not contain CR/LF' ;; esac; }
path_ok "$PWD"
path_ok "$attempt"
self="${BASH_SOURCE[0]}"
path_ok "$self"
[[ "$self" == /* ]] || self="$PWD/$self"
self="$(CDPATH='' cd -P -- "${self%/*}" && printf '%s/.' "$PWD")"
self="${self%/.}"
path_ok "$self"
# shellcheck source=scripts/ci/cargo-publication-paths.sh
source "$self/cargo-publication-paths.sh"
bash "$self/check-make-execution.sh"
cwd="$(CDPATH='' cd -P -- . && printf '%s/.' "$PWD")"
cwd="${cwd%/.}"
path_ok "$cwd"
checkout="$(git rev-parse --show-toplevel && printf '/.')"
checkout="${checkout%$'\n/.'}"
path_ok "$checkout"
checkout="$(CDPATH='' cd -P -- "$checkout" && printf '%s/.' "$PWD")"
checkout="${checkout%/.}"
path_ok "$checkout"
[[ "$attempt" == /* ]] || attempt="$cwd/$attempt"
parent="${attempt%/*}" name="${attempt##*/}"
[[ -n "$name" && "$name" != . && "$name" != .. ]] || fail 'select a fresh attempt directory'
parent="$(CDPATH='' cd -P -- "$parent" && printf '%s/.' "$PWD")"
parent="${parent%/.}"
path_ok "$parent"
attempt="$parent/$name"
[[ ! -e "$attempt" && ! -L "$attempt" ]] || fail 'attempt already exists; reconcile its evidence, never replay it'
arguments=(publish --locked --registry "$registry")
packages=()
seen='|'
while [[ $# -gt 0 ]]; do
    option="$1"; shift
    case "$option" in
        --package)
            [[ $# -gt 0 && "$1" =~ ^[A-Za-z][A-Za-z0-9_-]*@(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || fail 'packages require exact NAME@X.Y.Z selectors'
            package="${1%@*}"
            [[ "$seen" != *"|$package|"* ]] || fail 'duplicate package selection'
            seen="$seen$package|"
            packages+=("$1")
            arguments+=(--package "$1"); shift ;;
        --manifest-path|--target-dir)
            [[ $# -gt 0 && -n "$1" && "$1" != -* ]] || fail 'missing path option value'
            publication_path_option "$option" "$1" "$checkout"
            arguments+=("$option" "$1"); shift ;;
        --features)
            [[ $# -gt 0 && -n "$1" && "$1" != -* && "$1" != *$'\n'* && "$1" != *$'\r'* ]] || fail 'missing or invalid feature selection'
            arguments+=("$option" "$1"); shift ;;
        --all-features|--no-default-features|--no-verify) arguments+=("$option") ;;
        --allow-dirty)
            [[ "$mode" == check ]] || fail 'dirty upload is not supported'
            arguments+=("$option") ;;
        *) fail "unsupported option: $option" ;;
    esac
done
[[ ${#packages[@]} -gt 0 ]] || fail 'select at least one exact package'
[[ "$mode" != check ]] || arguments+=(--dry-run)
observed="$(git rev-parse --verify HEAD)"
[[ "$observed" == "$source" ]] || fail 'source changed before attempt'
dirty="$(git status --porcelain --untracked-files=all)"
[[ "$mode" != publish || -z "$dirty" ]] || fail 'upload requires clean source'
umask 077
lock="$parent/.cargo-publication.lock"
# Preserve mkdir's diagnostic/status, including non-occupancy failures.
mkdir "$lock"
finished=false
# shellcheck disable=SC2329 # Registered as the EXIT trap below.
finish() {
    local status=$?
    if [[ "$finished" == true ]]; then
        if ! { rm -f -- "$lock/owner" && rmdir -- "$lock"; }; then
            [[ "$status" != 0 ]] || status=1
        fi
    else
        printf 'Incomplete attempt; retain evidence and inspect lock: %s\n' "$lock" >&2 || :
        [[ "$status" != 0 ]] || status=1
    fi
    exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$$" > "$lock/owner"
mkdir "$attempt"
printf '%s\n' "$mode" "$source" "$registry" "${packages[@]}" > "$attempt/intent"
printf '%s\n' "$cwd" > "$attempt/cwd"
printf '%s\n' "$dirty" > "$attempt/working-status"
printf '%s\0' cargo "${arguments[@]}" > "$attempt/arguments"
# Evidence must live in an ignored location; never exempt arbitrary source files.
observed="$(git rev-parse --verify HEAD)"
observed_dirty="$(git status --porcelain --untracked-files=all)"
[[ "$observed" == "$source" && "$observed_dirty" == "$dirty" ]] || fail 'source changed before Cargo dispatch'
printf 'Publication evidence: %s\n' "$attempt"
printf 'started\n' > "$attempt/dispatch"
set +e
cargo "${arguments[@]}" 2>&1 | tee "$attempt/cargo.log"
statuses=("${PIPESTATUS[@]}")
set -e
# A logger failure must not hide Cargo's original failure or turn into success.
status="${statuses[0]}"
[[ "$status" != 0 ]] || status="${statuses[1]}"
printf '%s\n' "${statuses[0]}" > "$attempt/cargo-status"
printf '%s\n' "${statuses[1]}" > "$attempt/log-status"
printf '%s\n' "$status" > "$attempt/status"
finished=true
if [[ "$status" != 0 ]]; then
    printf 'Attempt failed; retain %s and reconcile before another upload.\n' "$attempt" >&2 || :
fi
exit "$status"
