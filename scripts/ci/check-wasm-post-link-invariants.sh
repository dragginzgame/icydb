#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
failures=0

require_text() {
    local path="$1"
    local text="$2"
    local message="$3"
    if ! rg -Fq -- "$text" "$ROOT/$path"; then
        echo "[ERROR] $message" >&2
        failures=1
    fi
}

require_text \
    testing/integration/src/lib.rs \
    'wasm_optimizer::optimize_deployable_wasm(' \
    'fixture builds must invoke the canonical post-link artifact producer.'
require_text \
    testing/integration/src/lib.rs \
    '&configured.compiler_emitted,' \
    'fixture post-link builds must consume the compiler-emitted artifact.'
require_text \
    testing/integration/src/lib.rs \
    '&configured.final_deployable,' \
    'fixture post-link builds must write the canonical deployable artifact.'
require_text \
    testing/integration/src/lib.rs \
    'final_deployable: configured.final_deployable,' \
    'fixture builds must publish the canonical post-link artifact.'
require_text \
    scripts/app/build.sh \
    '--bin build_fixture_canister' \
    'the maintained application build must use the canonical fixture artifact producer.'
require_text \
    .github/workflows/ci.yml \
    'make build-canister-production CANISTER=default_empty' \
    'release packaging must consume the canonical post-link artifact producer.'
require_text \
    testing/integration/src/wasm_optimizer.rs \
    'binaryen-108-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v2' \
    'the post-link pipeline identity must remain explicit.'
require_text \
    testing/integration/src/wasm_optimizer.rs \
    '--one-caller-inline-max-function-size=0' \
    'the post-link pipeline must prevent unbounded one-caller inlining.'

if [[ "$failures" -ne 0 ]]; then
    echo "[FAIL] Wasm post-link invariants failed." >&2
    exit 1
fi

echo "[OK] Wasm post-link invariants verified."
