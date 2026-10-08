#!/usr/bin/env bash
# Shared companions: scripts/ci/dependency-pins.jq
set -euo pipefail

# Read-only, offline checks; requires Git, jq and Mike Farah yq v4.47.2+.
# Cargo is needed only when Cargo manifests are present (workspace discovery).
SCRIPT_DIR="${BASH_SOURCE[0]}"
[[ "$SCRIPT_DIR" == /* ]] || SCRIPT_DIR="$PWD/$SCRIPT_DIR"
SCRIPT_DIR="$(cd -P "${SCRIPT_DIR%/*}" && printf '%s/.' "$PWD")"
SCRIPT_DIR="${SCRIPT_DIR%/.}"
ROOT="$(cd -P "$SCRIPT_DIR/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
YQ="${YQ:-yq}"
inheritance=false
npm_root='' node_version='' npm_version=''
usage() { echo 'usage: check-dependency-pins.sh [--consumer <repository>] [--cargo-inheritance] [--npm-root RELATIVE-DIR --node-version X.Y.Z --npm-version X.Y.Z]' >&2; }
fail() { echo "dependency pin check failed: $*" >&2; exit 1; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer) [[ $# -ge 2 ]] || { usage; exit 2; }; ROOT="$2"; shift 2 ;;
        --cargo-inheritance) inheritance=true; shift ;;
        --npm-root|--node-version|--npm-version)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            case "$1" in
                --npm-root) [[ -z "$npm_root" ]] || { usage; exit 2; }; npm_root="$2" ;;
                --node-version) node_version="$2" ;;
                --npm-version) npm_version="$2" ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
if [[ -n "$npm_root$node_version$npm_version" ]]; then
    [[ -n "$npm_root" ]] || { usage; exit 2; }
    node_version="${node_version#v}"
    for version in "$node_version" "$npm_version"; do
        [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || fail 'npm checking requires exact stable Node and npm versions'
    done
    if [[ "$npm_root" != . ]]; then
        case "/$npm_root/" in //*|*/../*|*/./*|*//*) fail 'npm root must be a relative directory inside the checkout' ;; esac
    fi
fi
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "$ROOT" && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
cd "$ROOT"
repository_prefix="$(git rev-parse --show-prefix)" && [[ -z "$repository_prefix" ]] || fail 'consumer must be the Git checkout root'
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
    (.rule == "cargo-exact" or .rule == "cargo-external-path" or .rule == "checkout-ref" or .rule == "npm-external-path") and
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
    metadata_parent="$ROOT"
    [[ "$path" != */* ]] || metadata_parent="$ROOT/${path%/*}"
    case "$(cd -P "$metadata_parent" && printf '%s/' "$PWD")" in
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
        workspace_root="$(cd -P "${workspace%/*}" && printf '%s/.' "$PWD")"
        workspace_root="${workspace_root%/.}"
        case "$workspace_root/" in "$ROOT/"*) ;; *) fail "workspace root escapes consumer: $path" ;; esac
        printf '%s\n' "$workspace_root" >> "$temporary/workspaces"
        if [[ "$inheritance" == true ]]; then
            [[ -f "$workspace" && ! -L "$workspace" ]] || fail "workspace manifest must be a regular file: $workspace"
            "$YQ" -p toml -o json -I 0 '.' "$workspace" > "$temporary/root.json" || fail "cannot parse $workspace"
            jq -se 'length == 1 and (.[0] | type == "object")' "$temporary/root.json" >/dev/null || fail "expected one workspace object: $workspace"
            jq -c -L "$SCRIPT_DIR" --slurpfile root "$temporary/root.json" \
                --argjson member "$(if [[ "$ROOT/$path" == "$workspace_root/Cargo.toml" ]]; then echo false; else echo true; fi)" \
                'include "dependency-pins"; inheritance_checks($root[0]; $member)' \
                "$temporary/document" >> "$temporary/findings"
        fi
        jq -c -L "$SCRIPT_DIR" 'include "dependency-pins"; dependencies | select(.spec.path? != null)' \
            "$temporary/parsed" > "$temporary/paths"
        while IFS= read -r dependency; do
            name="$(jq -r '.name' <<< "$dependency")"
            selected_path="$(jq -r '.spec.path' <<< "$dependency")"
            case "$selected_path" in
                /*) dependency_path="$selected_path" ;;
                *) dependency_path="$(dirname "$path")/$selected_path" ;;
            esac
            [[ "$dependency_path" == /* ]] || dependency_path="$ROOT/$dependency_path"
            resolved="$(cd -P "$dependency_path" && printf '%s/.' "$PWD")" || fail "dependency path is unavailable: $path: $name: $selected_path"
            resolved="${resolved%/.}"
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

if [[ -n "$npm_root" ]]; then
    npm_prefix="$npm_root/"; [[ "$npm_root" != . ]] || npm_prefix=''
    manifest="${npm_prefix}package.json"
    lock="${npm_prefix}package-lock.json"
    [[ "$npm_root" != *$'\n'* && "$npm_root" != *$'\t'* && -d "$npm_root" ]] || fail 'npm root must be an existing single-line directory'
    npm_physical="$(cd -P "$npm_root" && printf '%s/.' "$PWD")"
    npm_physical="${npm_physical%/.}"
    case "$npm_physical/" in "$ROOT/"*) ;; *) fail 'npm root escapes consumer through a symlink' ;; esac
    [[ ! -e "${npm_prefix}npm-shrinkwrap.json" && ! -L "${npm_prefix}npm-shrinkwrap.json" ]] || fail 'npm-shrinkwrap.json takes precedence; this checker supports package-lock.json roots only'
    for path in "$manifest" "$lock"; do
        [[ -f "$path" && ! -L "$path" ]] || fail "npm input must be a regular file: $path"
        git --literal-pathspecs ls-files --error-unmatch -- "$path" >/dev/null || fail "npm input must be tracked: $path"
        jq -se 'length == 1 and (.[0] | type == "object")' "$path" >/dev/null || fail "expected one JSON object: $path"
    done
    jq -c -L "$SCRIPT_DIR" --arg file "$manifest" --arg node "$node_version" --arg npm "$npm_version" \
        --slurpfile lock "$lock" 'include "dependency-pins"; npm_checks($file; $lock[0]; $node; $npm)' \
        "$manifest" >> "$temporary/findings"
    jq -c -L "$SCRIPT_DIR" 'include "dependency-pins"; npm_dependencies | select(.spec | npm_local)' \
        "$manifest" > "$temporary/npm-paths"
    while IFS= read -r dependency; do
        name="$(jq -r '.name' <<< "$dependency")"
        selected_path="$(jq -r '.spec' <<< "$dependency")"
        dependency_path="${selected_path#file:}"
        [[ -n "$dependency_path" ]] || fail "npm file input requires a directory: $manifest: $name"
        [[ "$dependency_path" != /* ]] || fail "npm absolute file input is unsupported: $manifest: $name"
        [[ "$dependency_path" != *$'\n'* && "$dependency_path" != *$'\t'* ]] || fail "npm file input requires a single-line path: $manifest: $name"
        resolved="$(cd -P "$npm_physical/$dependency_path" && printf '%s/.' "$PWD")" || fail "npm file dependency directory is unavailable: $manifest: $name"
        resolved="${resolved%/.}"
        case "$resolved/" in
            "$ROOT/"*) ;;
            *) jq -cn --arg file "$manifest" --arg subject "$name" --arg value "$selected_path" \
                '{file:$file,rule:"npm-external-path",subject:$subject,value:$value,message:"dependency outside the repository requires a documented development/qualification boundary"}' >> "$temporary/findings" ;;
        esac
    done < "$temporary/npm-paths"
fi

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
[[ -z "$npm_root" ]] || printf 'npm root declarations passed: %s (Node %s; npm %s; resolution and runtime qualification remain consumer-owned)\n' "$npm_root" "$node_version" "$npm_version"
echo 'dependency pins passed (declarations and tracked lockfiles; runtime qualification remains consumer-owned)'
