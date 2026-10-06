#!/usr/bin/env bash
set -euo pipefail

# Read-only, offline checks; requires Git, jq and Mike Farah yq v4.47.2+.
# Cargo is needed only when Cargo manifests are present (workspace discovery).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
YQ="${YQ:-yq}"
usage() { echo 'usage: check-dependency-pins.sh [--consumer <repository>]' >&2; }
fail() { echo "dependency pin check failed: $*" >&2; exit 1; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer) [[ $# -ge 2 ]] || { usage; exit 2; }; ROOT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
ROOT="$(cd "$ROOT" && pwd -P)"
cd "$ROOT"
[[ "$(cd "$(git rev-parse --show-toplevel)" && pwd -P)" == "$ROOT" ]] || fail 'consumer must be the Git checkout root'
command -v jq >/dev/null || fail 'jq is required'
parser_version="$("$YQ" --version)"
[[ "$parser_version" == 'yq (https://github.com/mikefarah/yq/) version v4.'* ]] || fail 'Mike Farah yq v4.47.2+ is required'
parser_version="${parser_version##*v4.}"
[[ "$parser_version" =~ ^([0-9]+)\.([0-9]+)$ ]] || fail 'unsupported yq version output'
(( 10#${BASH_REMATCH[1]} > 47 || (10#${BASH_REMATCH[1]} == 47 && 10#${BASH_REMATCH[2]} >= 2) )) || fail 'Mike Farah yq v4.47.2+ is required'
temporary="$(mktemp -d "${TMPDIR:-/tmp}/dependency-pins.XXXXXX")"
trap 'rm -rf "$temporary"' EXIT
git ls-files --cached --others --exclude-standard -z > "$temporary/files"
: > "$temporary/findings"
: > "$temporary/workspaces"

exceptions=ci/dependency-pinning-exceptions.json
if [[ -e "$exceptions" || -L "$exceptions" ]]; then
    [[ -f "$exceptions" && ! -L "$exceptions" ]] || fail "$exceptions must be a regular file"
    cp "$exceptions" "$temporary/exceptions.json"
else
    printf '[]\n' > "$temporary/exceptions.json"
fi
jq -e '
  type == "array" and all(.[];
    type == "object" and
    (keys == ["evidence", "file", "reason", "rule", "subject", "value"]) and
    all(.[]; type == "string" and length > 0) and
    (.rule == "cargo-exact" or .rule == "cargo-external-path" or .rule == "checkout-ref") and
    (.file | startswith("/") or contains("..") | not) and
    (.evidence | startswith("/") or contains("..") | not)) and
  (length == (unique_by([.file,.rule,.subject,.value]) | length))
' "$temporary/exceptions.json" >/dev/null || fail 'malformed or duplicate pinning exception; see rules/dependency-pinning.md'
jq -r '.[].evidence | split("#")[0]' "$temporary/exceptions.json" > "$temporary/evidence"
while IFS= read -r evidence; do
    [[ -f "$evidence" && ! -L "$evidence" ]] || fail "exception evidence is missing: $evidence"
    git --literal-pathspecs ls-files --error-unmatch -- "$evidence" >/dev/null || fail "exception evidence must be tracked: $evidence"
done < "$temporary/evidence"

while IFS= read -r -d '' path; do
    case "$path" in
        Cargo.toml|*/Cargo.toml) kind=cargo; format=toml ;;
        .github/workflows/*.yml|.github/workflows/*.yaml) kind=workflow; format=yaml ;;
        action.yml|action.yaml|*/action.yml|*/action.yaml) kind=action; format=yaml ;;
        *) continue ;;
    esac
    [[ -f "$path" && ! -L "$path" && "$path" != *$'\n'* && "$path" != *$'\t'* ]] || fail "metadata must be a regular file with a single-line path: $path"
    case "$(cd "$(dirname "$path")" && pwd -P)/" in
        "$ROOT/"*) ;;
        *) fail "metadata escapes consumer through a symlink: $path" ;;
    esac
    "$YQ" -p "$format" -o json -I 0 '.' "$path" > "$temporary/parsed" || fail "cannot parse $path"
    jq -se 'length == 1 and (.[0] | type == "object")' "$temporary/parsed" >/dev/null || fail "expected one configuration object: $path"
    jq -c --arg file "$path" --arg kind "$kind" '{file: $file, kind: $kind, data: .}' \
        "$temporary/parsed" > "$temporary/document"
    jq -c -L "$SCRIPT_DIR" 'include "dependency-pins"; checks' \
        "$temporary/document" >> "$temporary/findings"
    if [[ "$kind" == cargo ]]; then
        command -v cargo >/dev/null || fail 'Cargo is required for workspace discovery'
        workspace="$(CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0 cargo locate-project --workspace --message-format plain --manifest-path "$path")" || fail "cannot locate Cargo workspace for $path; prepare its selected toolchain explicitly"
        workspace_root="$(cd "$(dirname "$workspace")" && pwd -P)"
        case "$workspace_root/" in "$ROOT/"*) ;; *) fail "workspace root escapes consumer: $path" ;; esac
        printf '%s\n' "$workspace_root" >> "$temporary/workspaces"
        jq -c -L "$SCRIPT_DIR" 'include "dependency-pins"; dependencies | select(.spec.path? != null)' \
            "$temporary/parsed" > "$temporary/paths"
        while IFS= read -r dependency; do
            name="$(jq -r '.name' <<< "$dependency")"
            selected_path="$(jq -r '.spec.path' <<< "$dependency")"
            case "$selected_path" in
                /*) dependency_path="$selected_path" ;;
                *) dependency_path="$(dirname "$path")/$selected_path" ;;
            esac
            resolved="$(cd "$dependency_path" && pwd -P)" || fail "dependency path is unavailable: $path: $name: $selected_path"
            case "$resolved/" in
                "$ROOT/"*) ;;
                *) jq -cn --arg file "$path" --arg subject "$name" --arg value "$selected_path" \
                    '{file:$file,rule:"cargo-external-path",subject:$subject,value:$value,message:"dependency outside the repository requires a documented development/qualification boundary"}' >> "$temporary/findings" ;;
            esac
        done < "$temporary/paths"
    fi
done < "$temporary/files"
sort -u "$temporary/workspaces" > "$temporary/unique-workspaces"
while IFS= read -r workspace_root; do
    lock="$workspace_root/Cargo.lock"
    [[ -f "$lock" && ! -L "$lock" ]] || fail "Cargo workspace requires a tracked lockfile: $workspace_root"
    git --literal-pathspecs ls-files --error-unmatch -- "${lock#"$ROOT/"}" >/dev/null || fail "Cargo lockfile is not tracked: $lock"
done < "$temporary/unique-workspaces"

jq -s --slurpfile exceptions "$temporary/exceptions.json" '
  unique_by([.file,.rule,.subject,.value]) |
  map(. as $finding | select(any($exceptions[0][];
    .file == $finding.file and .rule == $finding.rule and
    .subject == $finding.subject and .value == $finding.value) | not))
' "$temporary/findings" > "$temporary/rejected.json"
if [[ "$(jq length "$temporary/rejected.json")" != 0 ]]; then
    jq -r '.[] | "\(.file): [\(.rule)] \(.subject) = \(.value): \(.message)"' "$temporary/rejected.json" >&2
    exit 1
fi
echo 'dependency pins passed (declarations and tracked lockfiles; runtime qualification remains consumer-owned)'
