#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="${BASH_SOURCE[0]}"
[[ "$SCRIPT_DIR" == /* ]] || SCRIPT_DIR="$PWD/$SCRIPT_DIR"
SCRIPT_DIR="$(cd -P "${SCRIPT_DIR%/*}" && printf '%s/.' "$PWD")"
SCRIPT_DIR="${SCRIPT_DIR%/.}"
DEFAULT_ROOT="$(cd -P "$SCRIPT_DIR/../.." && printf '%s/.' "$PWD")"
DEFAULT_ROOT="${DEFAULT_ROOT%/.}"
CONSUMER_ROOT="$DEFAULT_ROOT"
MANIFEST_PATH=".shared-tooling.snapshot"
read_manifest=''
verify_options=false

usage() {
    cat >&2 <<'USAGE'
usage: verify-shared-tooling-snapshot.sh [--consumer <repository>] [--manifest <relative-path>]
       verify-shared-tooling-snapshot.sh --read-manifest <file>

--read-manifest validates manifest syntax and emits its records, preserving order
and literal field bytes. No output is emitted on malformed input. It does not
inspect payloads or establish integrity, ownership or source trust.
USAGE
}

fail() {
    echo "shared-tooling snapshot verification failed: $1" >&2
    exit 1
}

# Check before command substitution can trim a pathname, and again after
# resolving aliases. Absolute cd inputs avoid CDPATH lookup and output.
resolve_directory() {
    local path="$1" resolved
    [[ "$path" == /* ]] || path="$PWD/$path"
    [[ "$path" != *$'\n'* && "$path" != *$'\r'* ]] ||
        fail "directory paths must not contain LF or CR characters"
    resolved="$(cd -P "$path" 2>/dev/null && printf '%s/.' "$PWD")" ||
        fail "directory does not exist: $path"
    resolved="${resolved%/.}"
    [[ "$resolved" != *$'\n'* && "$resolved" != *$'\r'* ]] ||
        fail "resolved directory paths must not contain LF or CR characters"
    printf '%s\n' "$resolved"
}

validate_relative_path() {
    local path="$1"

    [[ -n "$path" && "$path" != /* ]] || fail "path must be relative: $path"
    case "/$path/" in
    *'/../'* | *'/./'* | *'//'*) fail "path is not canonical: $path" ;;
    esac
    case "$path" in
    *$'\n'* | *$'\r'* | *$'\t'*) fail "path contains a forbidden control character" ;;
    esac
}

while [[ $# -gt 0 ]]; do
    case "$1" in
    --consumer)
        [[ $# -ge 2 ]] || { usage; exit 2; }
        CONSUMER_ROOT="$2"
        verify_options=true
        shift 2
        ;;
    --manifest)
        [[ $# -ge 2 ]] || { usage; exit 2; }
        MANIFEST_PATH="$2"
        verify_options=true
        shift 2
        ;;
    --read-manifest)
        [[ $# -ge 2 && -n "$2" && -z "$read_manifest" ]] || { usage; exit 2; }
        read_manifest="$2"
        shift 2
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        usage
        exit 2
        ;;
    esac
done

[[ -z "$read_manifest" || "$verify_options" == false ]] || { usage; exit 2; }
SCRIPT_DIR="$(resolve_directory "$SCRIPT_DIR")"
if [[ -n "$read_manifest" ]]; then
    manifest="$read_manifest"
    [[ "$manifest" == /* ]] || manifest="$PWD/$manifest"
else
    validate_relative_path "$MANIFEST_PATH"
    CONSUMER_ROOT="$(resolve_directory "$CONSUMER_ROOT")"
    [[ "$CONSUMER_ROOT" != "/" ]] || fail "consumer repository may not be the filesystem root"
    manifest="$CONSUMER_ROOT/$MANIFEST_PATH"
fi
[[ -f "$manifest" && ! -L "$manifest" ]] || fail "manifest is missing or symlinked: $manifest"
manifest_parent="$(resolve_directory "${manifest%/*}")"
if [[ -z "$read_manifest" ]]; then
    case "$manifest_parent/" in
        "$CONSUMER_ROOT/"*) ;;
        *) fail "manifest escapes the consumer repository: $MANIFEST_PATH" ;;
    esac
fi
# Bash line readers otherwise discard NUL bytes and can admit different fields
# from those recorded on disk. Capture once, checking that same literal stream
# before splitting it; a second read could observe a changed manifest.
manifest_contents=''
if IFS= read -r -d '' manifest_contents < "$manifest"; then fail "manifest contains a NUL byte"; fi

format_count=0
source_count=0
revision_count=0
version_count=0
snapshot_version=unrecorded
snapshot_revision=''
file_count=0
declared_files=()
declared_digests=()
declared_modes=()
records=()
entrypoints=()
checksum_tool_declared=false
snapshot_verifier_declared=false

while IFS= read -r manifest_line || [[ -n "$manifest_line" ]]; do
    IFS=$'\t' read -r record first second third extra <<< "$manifest_line"
    case "$record" in
    '# entrypoint')
        [[ -n "$first" && "$manifest_line" == $'# entrypoint\t'"$first" ]] || fail "malformed entrypoint annotation"
        validate_relative_path "$first"
        for existing in ${entrypoints[@]+"${entrypoints[@]}"}; do
            [[ "$existing" != "$first" ]] || fail "duplicate entrypoint: $first"
        done
        entrypoints+=("$first")
        ;;
    '# version')
        [[ "$first" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ && "$manifest_line" == "$record"$'\t'"$first" ]] ||
            fail "malformed version annotation"
        version_count=$((version_count + 1))
        snapshot_version="$first"
        ;;
    '' | \#*) continue ;;
    format)
        [[ "$first" == "1" && "$manifest_line" == "$record"$'\t'"$first" ]] ||
            fail "unsupported or malformed format record"
        format_count=$((format_count + 1))
        ;;
    source)
        [[ -n "$first" && "$manifest_line" == "$record"$'\t'"$first" ]] ||
            fail "malformed source record"
        source_count=$((source_count + 1))
        ;;
    revision)
        [[ "$first" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ && "$manifest_line" == "$record"$'\t'"$first" ]] ||
            fail "malformed revision record"
        revision_count=$((revision_count + 1))
        snapshot_revision="$first"
        ;;
    file)
        [[ "$first" =~ ^[0-9a-f]{64}$ && "$second" =~ ^(-|x)$ && -n "$third" && -z "$extra" && "$manifest_line" == "$record"$'\t'"$first"$'\t'"$second"$'\t'"$third" ]] ||
            fail "malformed file record"
        validate_relative_path "$third"
        # Bash 3.2 treats an empty array expansion as unset under nounset.
        for ((declared_index = 0; declared_index < file_count; declared_index++)); do
            [[ "${declared_files[$declared_index]}" != "$third" ]] || fail "duplicate file record: $third"
        done
        declared_files[file_count]="$third"
        declared_digests[file_count]="$first"
        declared_modes[file_count]="$second"
        [[ "$third" != "scripts/ci/verify-file-checksum.sh" ]] || checksum_tool_declared=true
        [[ "$third" != "scripts/ci/verify-shared-tooling-snapshot.sh" ]] || snapshot_verifier_declared=true
        file_count=$((file_count + 1))
        ;;
    *) fail "unknown manifest record: $record" ;;
    esac
    records+=("$manifest_line")
done <<< "$manifest_contents"

[[ "$format_count" -eq 1 ]] || fail "manifest must contain exactly one format record"
[[ "$source_count" -eq 1 ]] || fail "manifest must contain exactly one source record"
[[ "$revision_count" -eq 1 ]] || fail "manifest must contain exactly one revision record"
[[ "$version_count" -le 1 ]] || fail "manifest has duplicate version annotations"
[[ "$file_count" -gt 0 ]] || fail "manifest contains no files"
for entrypoint in ${entrypoints[@]+"${entrypoints[@]}"}; do
    selected=false
    for path in "${declared_files[@]}"; do [[ "$path" != "$entrypoint" ]] || selected=true; done
    [[ "$selected" == true ]] || fail "entrypoint is not a declared payload: $entrypoint"
done

if [[ -n "$read_manifest" ]]; then
    printf '%s\n' "${records[@]}"
    exit 0
fi

[[ "$checksum_tool_declared" == "true" ]] || fail "manifest does not declare the checksum verifier"
[[ "$snapshot_verifier_declared" == "true" ]] || fail "manifest does not declare the snapshot verifier"
# This is the integrity bootstrap: never execute code from the snapshot being
# inspected, including its checksum helper. Keep this small SHA-256 boundary
# independent of that helper; all other consumers reuse the shared digest tool.
if command -v sha256sum >/dev/null 2>&1; then
    checksum_command=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
    checksum_command=(shasum -a 256)
else
    fail 'no SHA-256 implementation is available'
fi
checksum_pattern='^([0-9a-f]{64}) [ *]-$'
for index in "${!declared_files[@]}"; do
    path="${declared_files[$index]}"
    target="$CONSUMER_ROOT/$path"
    [[ -f "$target" && ! -L "$target" ]] || fail "declared file is missing or symlinked: $path"
    if [[ "${declared_modes[$index]}" == x ]]; then
        [[ -x "$target" ]] || fail "declared file lost its executable mode: $path"
    else
        [[ ! -x "$target" ]] || fail "declared file gained executable mode: $path"
    fi
    resolved_parent="$(resolve_directory "${target%/*}")"
    case "$resolved_parent/" in
        "$CONSUMER_ROOT/"*) ;;
        *) fail "declared file escapes the consumer repository: $path" ;;
    esac
    checksum_output="$(LC_ALL=C "${checksum_command[@]}" < "$target")" || fail "checksum backend failed: $path"
    [[ "$checksum_output" =~ $checksum_pattern ]] || fail "malformed checksum output: $path"
    [[ "${BASH_REMATCH[1]}" == "${declared_digests[$index]}" ]] || fail "declared file differs from the snapshot: $path"
done

echo "shared-tooling snapshot verified: $file_count file(s), version $snapshot_version ($snapshot_revision)"
