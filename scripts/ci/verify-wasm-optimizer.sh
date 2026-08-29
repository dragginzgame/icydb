#!/usr/bin/env bash
set -euo pipefail

expected_version="wasm-opt version 108 (version_108)"
expected_sha256="36f78112c8d629e27f8c68be89bee47c245cbde8794e1ff56c03212c02dc8484"

if ! command -v wasm-opt >/dev/null 2>&1; then
    echo "missing pinned wasm optimizer; run 'canic toolchain install'" >&2
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
