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
    'cache_post_link_wasm(&PostLinkCacheRequest {' \
    'fixture builds must invoke the canonical cached post-link artifact producer.'
require_text \
    testing/integration/src/canister_build_cache.rs \
    'build_artifact_caches_batch(&specs' \
    'whole-fleet post-link validation must use the collect-all artifact cache batch.'
require_text \
    testing/integration/src/canister_build_cache.rs \
    'LabeledArtifactCacheSpec::new(' \
    'whole-fleet post-link validation must retain stable caller labels across batch reports.'
require_text \
    testing/integration/src/canister_build_cache.rs \
    'LabeledWasmBuildSpec::new(' \
    'whole-fleet Cargo validation must retain stable caller labels across batch reports.'
require_text \
    testing/integration/src/canister_build_cache.rs \
    'failure.timings(),' \
    'whole-fleet Cargo failures must retain partial phase timings.'
require_text \
    testing/integration/src/lib.rs \
    'WasmBuildInputSnapshot::prepare_assuming_sources_immutable(' \
    'the two-profile artifact contract must prepare one guarded immutable input snapshot.'
require_text \
    testing/integration/src/canister_build_cache.rs \
    'snapshot.build_batch_with_progress(' \
    'whole-fleet profile readers must consume the prepared input snapshot.'
require_text \
    testing/integration/src/canister_build_cache.rs \
    'optimize_deployable_wasm_with_optimizer(entry.compiler_emitted, &output, &optimizer)' \
    'the post-link artifact cache must invoke the canonical optimizer on cache misses.'
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
    'bash scripts/ci/wasm-size-report.sh --profile wasm-release --canister default_empty' \
    'release evidence must invoke the canonical post-link artifact producer.'
require_text \
    .github/workflows/ci.yml \
    'cp release-input/default_empty.wasm-release.final-deployable.wasm artifacts/default_empty.wasm' \
    'release packaging must consume the verified canonical post-link artifact.'
require_text \
    testing/integration/src/wasm_optimizer.rs \
    'binaryen-132-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1' \
    'the post-link pipeline identity must remain explicit.'
require_text \
    testing/integration/src/wasm_optimizer.rs \
    '--one-caller-inline-max-function-size=0' \
    'the post-link pipeline must prevent unbounded one-caller inlining.'
require_text \
    scripts/ci/install-wasm-optimizer.sh \
    'WebAssembly/binaryen/releases/download' \
    'the optimizer installer must use the checksum-pinned official Binaryen release.'
require_text \
    scripts/ci/install-wasm-optimizer.sh \
    'BINARYEN_VERSION="version_132"' \
    'the optimizer installer must retain the qualified Binaryen release.'
require_text \
    scripts/dev/workstation-setup.sh \
    "bash \"\$ROOT/scripts/ci/install-wasm-optimizer.sh\"" \
    'workstation setup must install the repository-owned Binaryen executable.'
require_text \
    scripts/dev/workstation-setup.sh \
    "bash \"\$ROOT/scripts/ci/install-wasm-optimizer.sh\" --check-latest" \
    'workstation updates must report whether the Binaryen pin matches the latest release.'
require_text \
    .github/workflows/ci.yml \
    'bash scripts/ci/install-wasm-optimizer.sh' \
    'CI must install the repository-owned Binaryen executable.'
require_text \
    .github/workflows/sql-performance.yml \
    'bash scripts/ci/install-wasm-optimizer.sh' \
    'SQL performance CI must install the repository-owned Binaryen executable.'
if rg -Fiq -- 'canic' \
    "$ROOT/scripts/ci/install-wasm-optimizer.sh" \
    "$ROOT/scripts/ci/verify-wasm-optimizer.sh" \
    "$ROOT/testing/integration/src/wasm_optimizer.rs"; then
    echo '[ERROR] the IcyDB optimizer contract must not depend on Canic.' >&2
    failures=1
fi
if rg -Fq -- 'binaryen' \
    "$ROOT/scripts/dev/workstation-setup.sh" \
    "$ROOT/.github/workflows/ci.yml" \
    "$ROOT/.github/workflows/sql-performance.yml"; then
    echo '[ERROR] maintained setup paths must delegate Binaryen installation to the canonical script.' >&2
    failures=1
fi

if [[ "$failures" -ne 0 ]]; then
    echo "[FAIL] Wasm post-link invariants failed." >&2
    exit 1
fi

echo "[OK] Wasm post-link invariants verified."
