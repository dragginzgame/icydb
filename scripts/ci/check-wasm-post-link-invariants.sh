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
    qualification/build/src/lib.rs \
    'cache_post_link_wasm(&PostLinkCacheRequest {' \
    'fixture builds must invoke the canonical cached post-link artifact producer.'
require_text \
    qualification/build/src/canister_build_cache/mod.rs \
    'build_artifact_caches_batch(&specs' \
    'whole-fleet post-link validation must use the collect-all artifact cache batch.'
require_text \
    qualification/build/src/canister_build_cache/mod.rs \
    'LabeledArtifactCacheSpec::new(' \
    'whole-fleet post-link validation must retain stable caller labels across batch reports.'
require_text \
    qualification/build/src/canister_build_cache/mod.rs \
    'LabeledWasmBuildSpec::new(' \
    'whole-fleet Cargo validation must retain stable caller labels across batch reports.'
require_text \
    qualification/build/src/lib.rs \
    'entry.failure().map(|(error, details)| {' \
    'whole-fleet Cargo failures must retain the paired error and partial phase details.'
# Scope this handoff to its real owner: a checked call elsewhere in the file
# must not mask a bypass in the concurrent two-profile path. Planning remains
# fallible; Build 0.83 spec construction is infallible after that admission.
if ! perl -0777 -ne '
    my ($body) = /^pub fn build_maintained_canister_contract_profiles\b(.*?)^}/ms;
    exit 1 unless defined $body;
    exit 1 unless $body =~ /\.collect::<Result<Vec<_>, _>>\(\)\?;/;
    exit($body =~ /let specs = plan\.cargo_specs\(root\);\s*let cargo_report = build_cached_cargo_wasm_batch\(&specs\);\s*finish_maintained_canister_builds\(root, plan, cargo_report\)/s ? 0 : 1);
' "$ROOT/qualification/build/src/lib.rs"; then
    echo '[ERROR] the two-profile contract must propagate planning errors and use the checked Cargo batch owner.' >&2
    failures=1
fi
require_text \
    qualification/build/src/canister_build_cache/mod.rs \
    'let report = build_wasm_canisters_cached_batch(' \
    'whole-fleet profile readers must retain checked input resolution and collect-all failures.'
require_text \
    qualification/build/src/canister_build_cache/mod.rs \
    'optimize_deployable_wasm_with_optimizer(entry.compiler_emitted, &output, &optimizer)' \
    'the post-link artifact cache must invoke the canonical optimizer on cache misses.'
require_text \
    qualification/build/src/lib.rs \
    '&artifacts.compiler_emitted,' \
    'fixture post-link builds must consume the retained compiler-emitted artifact.'
require_text \
    qualification/build/src/lib.rs \
    '&configured.final_deployable,' \
    'fixture post-link builds must write the canonical deployable artifact.'
require_text \
    qualification/build/src/lib.rs \
    'artifacts.retain_post_link(outcome.record().clone())?' \
    'fixture builds must retain the canonical post-link artifact through consumption.'
require_text \
    scripts/app/build.sh \
    '--bin build_fixture_canister' \
    'the maintained application build must use the canonical fixture artifact producer.'
# Composed canister evidence and packaging belong to adapter qualification;
# product CI remains independent of the selected Canic stack.
require_text \
    .github/workflows/canic-adapter.yml \
    'bash scripts/ci/wasm-size-report.sh --profile wasm-release --canister default_empty' \
    'release evidence must invoke the canonical post-link artifact producer.'
require_text \
    .github/workflows/canic-adapter.yml \
    'cp release-input/default_empty.wasm-release.final-deployable.wasm artifacts/default_empty.wasm' \
    'release packaging must consume the verified canonical post-link artifact.'
require_text \
    qualification/build/src/wasm_optimizer/mod.rs \
    'binaryen-133-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1' \
    'the post-link pipeline identity must remain explicit.'
require_text \
    qualification/build/src/wasm_optimizer/mod.rs \
    '--one-caller-inline-max-function-size=0' \
    'the post-link pipeline must prevent unbounded one-caller inlining.'
require_text \
    Makefile \
    "include \$(ROOT_DIR)/vendor/toko-tooling/make/consumer.mk" \
    'tool installation must delegate to the reviewed shared Make include.'
require_text \
    vendor/toko-tooling/ci/ic-tools.tsv \
    $'wasm-opt\t133\t' \
    'the IC toolset must retain the qualified optimizer release.'
require_text \
    scripts/dev/workstation-setup.sh \
    'install-tools' \
    'workstation setup must prepare the common local toolset.'
# check-ci-workflow-invariants.sh owns parsed CI tool setup/check admission;
# check-invariants runs that gate before this post-link contract checker.

if [[ "$failures" -ne 0 ]]; then
    echo "[FAIL] Wasm post-link invariants failed." >&2
    exit 1
fi

bash "$ROOT/scripts/ci/test-wasm-audit-report.sh"

echo "[OK] Wasm post-link invariants verified."
