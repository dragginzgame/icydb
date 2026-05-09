#!/usr/bin/env bash
set -euo pipefail

# Fail closed when CI scripts depend on the local ICP environment but the
# binary is missing or the local replica endpoint is unreachable.
if ! command -v icp >/dev/null 2>&1; then
    echo "[ci] required tool 'icp' is not installed or not on PATH" >&2
    exit 1
fi

if ! icp network ping local >/dev/null 2>&1; then
    echo "[ci] local ICP replica is unreachable; expected 'icp network ping local' to succeed" >&2
    exit 1
fi
