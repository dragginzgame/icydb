#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
DEFAULT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
CONSUMER_ROOT="$DEFAULT_ROOT"
MANIFEST_PATH=".shared-tooling.snapshot"

usage() {
    cat >&2 <<'USAGE'
usage: verify-shared-tooling-snapshot.sh [--consumer <repository>] [--manifest <relative-path>]
USAGE
}

fail() {
    echo "shared-tooling snapshot verification failed: $1" >&2
    exit 1
}

validate_relative_path() {
    local path="$1"

    [[ -n "$path" && "$path" != /* ]] || fail "path must be relative: $path"
    case "/$path/" in
    *'/../'* | *'/./'* | *'//'*) fail "path is not canonical: $path" ;;
    esac
    case "$path" in
    *$'\n'* | *$'\t'*) fail "path contains a forbidden control character" ;;
    esac
}

while [[ $# -gt 0 ]]; do
    case "$1" in
    --consumer)
        [[ $# -ge 2 ]] || { usage; exit 2; }
        CONSUMER_ROOT="$2"
        shift 2
        ;;
    --manifest)
        [[ $# -ge 2 ]] || { usage; exit 2; }
        MANIFEST_PATH="$2"
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

validate_relative_path "$MANIFEST_PATH"
CONSUMER_ROOT="$(cd "$CONSUMER_ROOT" 2>/dev/null && pwd -P)" ||
    fail "consumer repository does not exist: $CONSUMER_ROOT"
[[ "$CONSUMER_ROOT" != "/" ]] || fail "consumer repository may not be the filesystem root"

manifest="$CONSUMER_ROOT/$MANIFEST_PATH"
[[ -f "$manifest" && ! -L "$manifest" ]] || fail "manifest is missing or symlinked: $manifest"
manifest_parent="$(cd "$(dirname "$manifest")" && pwd -P)"
case "$manifest_parent/" in
"$CONSUMER_ROOT/"*) ;;
*) fail "manifest escapes the consumer repository: $MANIFEST_PATH" ;;
esac

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

format_count=0
source_count=0
revision_count=0
file_count=0
declared_files=()
checksum_tool_declared=false
snapshot_verifier_declared=false

while IFS=$'\t' read -r record first second third extra || [[ -n "$record" ]]; do
    case "$record" in
    '' | \#*) continue ;;
    format)
        [[ "$first" == "1" && -z "$second" && -z "$third" && -z "$extra" ]] ||
            fail "unsupported or malformed format record"
        format_count=$((format_count + 1))
        ;;
    source)
        [[ -n "$first" && -z "$second" && -z "$third" && -z "$extra" ]] ||
            fail "malformed source record"
        source_count=$((source_count + 1))
        ;;
    revision)
        [[ "$first" =~ ^[0-9a-f]{40,64}$ && -z "$second" && -z "$third" && -z "$extra" ]] ||
            fail "malformed revision record"
        revision_count=$((revision_count + 1))
        ;;
    file)
        [[ "$first" =~ ^[0-9a-f]{64}$ && "$second" =~ ^(-|x)$ && -n "$third" && -z "$extra" ]] ||
            fail "malformed file record"
        validate_relative_path "$third"
        # Bash 3.2 treats an empty array expansion as unset under nounset.
        for ((declared_index = 0; declared_index < file_count; declared_index++)); do
            [[ "${declared_files[$declared_index]}" != "$third" ]] || fail "duplicate file record: $third"
        done
        declared_files[file_count]="$third"
        [[ "$third" != "scripts/ci/verify-file-checksum.sh" ]] || checksum_tool_declared=true
        [[ "$third" != "scripts/ci/verify-shared-tooling-snapshot.sh" ]] || snapshot_verifier_declared=true
        target="$CONSUMER_ROOT/$third"
        [[ -f "$target" && ! -L "$target" ]] ||
            fail "declared file is missing or symlinked: $third"
        if [[ "$second" == "x" ]]; then
            [[ -x "$target" ]] || fail "declared file lost its executable mode: $third"
        else
            [[ ! -x "$target" ]] || fail "declared file gained executable mode: $third"
        fi
        resolved_parent="$(cd "$(dirname "$target")" && pwd -P)"
        case "$resolved_parent/" in
        "$CONSUMER_ROOT/"*) ;;
        *) fail "declared file escapes the consumer repository: $third" ;;
        esac
        checksum_output="$(LC_ALL=C "${checksum_command[@]}" < "$target")" ||
            fail "checksum backend failed: $third"
        [[ "$checksum_output" =~ $checksum_pattern ]] || fail "malformed checksum output: $third"
        [[ "${BASH_REMATCH[1]}" == "$first" ]] || fail "declared file differs from the snapshot: $third"
        file_count=$((file_count + 1))
        ;;
    *) fail "unknown manifest record: $record" ;;
    esac
done <"$manifest"

[[ "$format_count" -eq 1 ]] || fail "manifest must contain exactly one format record"
[[ "$source_count" -eq 1 ]] || fail "manifest must contain exactly one source record"
[[ "$revision_count" -eq 1 ]] || fail "manifest must contain exactly one revision record"
[[ "$file_count" -gt 0 ]] || fail "manifest contains no files"
[[ "$checksum_tool_declared" == "true" ]] || fail "manifest does not declare the checksum verifier"
[[ "$snapshot_verifier_declared" == "true" ]] || fail "manifest does not declare the snapshot verifier"

echo "shared-tooling snapshot verified: $file_count file(s)"
