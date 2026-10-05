#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/ci/wasm-optimizer-pin.sh
source "$ROOT/scripts/ci/wasm-optimizer-pin.sh"

if ! command -v wasm-opt >/dev/null 2>&1; then
    echo "missing pinned wasm optimizer; run 'bash scripts/ci/install-wasm-optimizer.sh'" >&2
    exit 1
fi

wasm_opt_bin="$(command -v wasm-opt)"
bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$WASM_OPT_SHA256" "$wasm_opt_bin"
observed_version="$("$wasm_opt_bin" --version)"

if [[ "$observed_version" != "$WASM_OPT_VERSION" ]]; then
    echo "unexpected wasm optimizer version '$observed_version'; expected '$WASM_OPT_VERSION'" >&2
    exit 1
fi
echo "[OK] pinned wasm optimizer verified: $observed_version ($WASM_OPT_SHA256)"
