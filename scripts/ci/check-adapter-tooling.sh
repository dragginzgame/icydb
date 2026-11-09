#!/usr/bin/env bash
set -euo pipefail

# The repository installs one snapshot; both Cargo workspaces must select its
# bytes. Shared owns pin/member/lock admission. This caller compares selections.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$PATH"
root_selection="$(perl "$ROOT/scripts/dev/tooling-lock.pl" "$ROOT")"
adapter_selection="$(perl "$ROOT/scripts/dev/tooling-lock.pl" "$ROOT/testing/canic-adapter")"
if [[ "$adapter_selection" != "$root_selection" ]]; then
    echo 'Canic tooling selection differs from the installed root selection' >&2
    exit 1
fi
echo 'Root and Canic workspace tooling selections agree.'
