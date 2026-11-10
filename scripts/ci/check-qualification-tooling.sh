#!/usr/bin/env bash
set -euo pipefail

# The shared adapter owns declaration/lock admission; all workspaces consume one snapshot.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$PATH"
product="$(perl "$ROOT/scripts/dev/tooling-lock.pl" "$ROOT")"
qualification="$(perl "$ROOT/scripts/dev/tooling-lock.pl" "$ROOT/qualification")"
if [[ "$qualification" != "$product" ]]; then
    echo 'Native qualification tooling differs from the installed product selection' >&2
    exit 1
fi
echo 'Product and native qualification tooling selections agree.'
