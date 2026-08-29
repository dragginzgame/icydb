#!/usr/bin/env bash
set -euo pipefail

expected_version="wasm-opt version 132 (version_132)"
expected_sha256="1014958e6f20d412f1542320b43970214b0fb1ed780595e8f7c0d8761ed53725"

if ! command -v wasm-opt >/dev/null 2>&1; then
    echo "missing pinned wasm optimizer; run 'bash scripts/ci/install-wasm-optimizer.sh'" >&2
    exit 1
fi

wasm_opt_bin="$(command -v wasm-opt)"
observed_version="$("$wasm_opt_bin" --version)"
observed_sha256="$(sha256sum "$wasm_opt_bin" | cut -d ' ' -f 1)"

if [[ "$observed_version" != "$expected_version" ]]; then
    echo "unexpected wasm optimizer version '$observed_version'; expected '$expected_version'" >&2
    exit 1
fi
if [[ "$observed_sha256" != "$expected_sha256" ]]; then
    echo "unexpected wasm optimizer SHA-256 '$observed_sha256'; expected '$expected_sha256'" >&2
    exit 1
fi

echo "[OK] pinned wasm optimizer verified: $observed_version ($observed_sha256)"
