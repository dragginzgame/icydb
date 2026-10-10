#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "$(uname -s):$(uname -m)" in
  Linux:x86_64|Linux:amd64) platform=linux_x86_64 ;;
  Darwin:x86_64|Darwin:amd64) platform=darwin_x86_64 ;;
  Darwin:arm64|Darwin:aarch64) platform=darwin_arm64 ;;
  *) echo 'unsupported Binaryen host' >&2; exit 1 ;;
esac
WASM_OPT_SHA256="$(awk -v platform="$platform" '$1 == platform {print $2}' "$ROOT/scripts/ci/wasm-optimizer-checksums.tsv")"
[[ "$WASM_OPT_SHA256" =~ ^[0-9a-f]{64}$ ]] || { echo 'invalid optimizer digest' >&2; exit 1; }
# This identity is IcyDB's frozen optimization policy, not a second tool selection.
WASM_OPT_VERSION='wasm-opt version 133 (version_133)'
awk -F '\t' '$1 == "wasm-opt" { n++; if ($2 != "133") bad=1 } END { if (n != 3 || bad) exit 1 }' "$ROOT/ci/ic-tools.tsv" || {
    echo 'IC tool pins do not match the qualified IcyDB optimizer' >&2; exit 1;
}
wasm_opt_bin="${ICYDB_WASM_OPT_BIN:-$ROOT/.tools/ic/bin/wasm-opt}"
if [[ ! -x "$wasm_opt_bin" ]]; then
    echo "missing pinned wasm optimizer; run 'make install-ic-tools'" >&2
    exit 1
fi
bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$WASM_OPT_SHA256" "$wasm_opt_bin"
observed_version="$("$wasm_opt_bin" --version)"

if [[ "$observed_version" != "$WASM_OPT_VERSION" ]]; then
    echo "unexpected wasm optimizer version '$observed_version'; expected '$WASM_OPT_VERSION'" >&2
    exit 1
fi
echo "[OK] pinned wasm optimizer verified: $observed_version ($WASM_OPT_SHA256)"
