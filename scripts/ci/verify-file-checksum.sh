#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 3 ]]; then
    echo 'usage: verify-file-checksum.sh <sha256|sha512> <expected-hex> <file>' >&2
    echo '       verify-file-checksum.sh --print <sha256|sha512> <file>' >&2
    exit 2
fi

print_only=false
if [[ "$1" == --print ]]; then
    print_only=true
    algorithm="$2"
    expected=''
else
    algorithm="$1"
    expected="$2"
fi
file="$3"
[[ -f "$file" ]] || { echo "checksum input is not a file: $file" >&2; exit 1; }
case "$algorithm" in
    sha256) bits=256; expected_length=64 ;;
    sha512) bits=512; expected_length=128 ;;
    *) echo "unsupported checksum algorithm: $algorithm" >&2; exit 2 ;;
esac
if [[ "$print_only" == false && ! "$expected" =~ ^[0-9a-f]{$expected_length}$ ]]; then
    echo "invalid expected $algorithm digest for $file" >&2
    exit 2
fi

# Hash stdin so filenames never become options or escaped checksum records.
# Do not fall back after a selected backend fails: a read/tool error is a failure.
if command -v "${algorithm}sum" >/dev/null 2>&1; then
    output="$(LC_ALL=C "${algorithm}sum" < "$file")" || {
        status=$?; echo "$algorithm backend failed for $file" >&2; exit "$status";
    }
elif command -v shasum >/dev/null 2>&1; then
    output="$(LC_ALL=C shasum -a "$bits" < "$file")" || {
        status=$?; echo "$algorithm backend failed for $file" >&2; exit "$status";
    }
else
    echo "no SHA-$bits implementation is available" >&2
    exit 1
fi
pattern="^([0-9a-f]{$expected_length}) [ *]-$"
if [[ ! "$output" =~ $pattern ]]; then
    echo "malformed $algorithm backend output for $file" >&2
    exit 1
fi
actual="${BASH_REMATCH[1]}"
if [[ "$print_only" == true ]]; then
    printf '%s\n' "$actual"
elif [[ "$actual" != "$expected" ]]; then
    echo "$algorithm checksum mismatch for $file" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    exit 1
fi
