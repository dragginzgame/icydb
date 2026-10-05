#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: verify-file-checksum.sh <sha256|sha512> <expected-hex> <file>" >&2
    exit 2
fi

algorithm="$1"
expected="$2"
file="$3"

if [ ! -f "$file" ]; then
    echo "checksum input is not a file: $file" >&2
    exit 1
fi

case "$algorithm" in
sha256)
    expected_length=64
    if command -v sha256sum >/dev/null 2>&1; then
        output="$(sha256sum "$file")"
    elif command -v shasum >/dev/null 2>&1; then
        output="$(shasum -a 256 "$file")"
    else
        echo "no SHA-256 implementation is available" >&2
        exit 1
    fi
    ;;
sha512)
    expected_length=128
    if command -v sha512sum >/dev/null 2>&1; then
        output="$(sha512sum "$file")"
    elif command -v shasum >/dev/null 2>&1; then
        output="$(shasum -a 512 "$file")"
    else
        echo "no SHA-512 implementation is available" >&2
        exit 1
    fi
    ;;
*)
    echo "unsupported checksum algorithm: $algorithm" >&2
    exit 2
    ;;
esac

if [[ ! "$expected" =~ ^[0-9a-f]{$expected_length}$ ]]; then
    echo "invalid expected $algorithm digest for $file" >&2
    exit 2
fi

actual="${output%% *}"
if [ "$actual" != "$expected" ]; then
    echo "$algorithm checksum mismatch for $file" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    exit 1
fi
