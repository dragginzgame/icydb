#!/usr/bin/env bash
set -euo pipefail

# Fail closed when CI scripts depend on the local dfx environment but the
# binary is missing or the local replica endpoint is unreachable.
if ! command -v dfx >/dev/null 2>&1; then
    echo "[ci] required tool 'dfx' is not installed or not on PATH" >&2
    exit 1
fi

if ! dfx ping local >/dev/null 2>&1; then
    echo "[ci] local dfx replica is unreachable; expected 'dfx ping local' to succeed" >&2
    exit 1
fi
