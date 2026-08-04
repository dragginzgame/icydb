#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
out_dir="$ROOT/artifacts/wasm-size"
profile="wasm-release"
sql_variants_mode="sql-on"
canister_names=()
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

# shellcheck source=scripts/ci/wasm-report-common.sh
source "$ROOT/scripts/ci/wasm-report-common.sh"

usage() {
    cat <<'EOF'
usage: wasm-size-report.sh [--profile debug|release|wasm-release] [--sql-variants sql-on|sql-off|both] [--canister name]

Defaults to wasm-release, sql-on, and the standard audit canister set.
Repeat --canister to build more than one specific canister.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            profile="${2:-}"
            shift 2
            ;;
        --sql-variants)
            sql_variants_mode="${2:-}"
            shift 2
            ;;
        --canister)
            canister_names+=("${2:-}")
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "[wasm-size] unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ -z "$profile" ]]; then
    echo "[wasm-size] --profile requires a value" >&2
    exit 1
fi
if [[ -z "$sql_variants_mode" ]]; then
    echo "[wasm-size] --sql-variants requires a value" >&2
    exit 1
fi
for canister_name in "${canister_names[@]}"; do
    if [[ -z "$canister_name" ]]; then
        echo "[wasm-size] --canister requires a value" >&2
        exit 1
    fi
    if ! wasm_report_canister_is_maintained_subject "$canister_name"; then
        echo "[wasm-size] canister '$canister_name' is outside the 0.220 measurement contract" >&2
        exit 1
    fi
done
if [[ "${#canister_names[@]}" -eq 0 ]]; then
    mapfile -t canister_names < <(wasm_report_default_canisters)
fi

mkdir -p "$out_dir"

if ! command -v ic-wasm >/dev/null 2>&1; then
    echo "[wasm-size] missing required tool: ic-wasm" >&2
    exit 1
fi
IC_WASM_BIN="$(command -v ic-wasm)"
if ! command -v wasm-opt >/dev/null 2>&1; then
    echo "[wasm-size] missing required tool: wasm-opt" >&2
    exit 1
fi
WASM_OPT_BIN="$(command -v wasm-opt)"
if ! command -v candid-extractor >/dev/null 2>&1; then
    echo "[wasm-size] missing required tool: candid-extractor" >&2
    exit 1
fi

# The wasm size report consumes locally staged canister artifacts under
# `.icp/local/canisters/<name>/`, but the staging step is owned by
# `build_fixture_canister` and does not require `icp` or a live local replica.
# Keep this script independent from the local ICP environment so CI can run
# wasm-size measurements without provisioning replica tooling it never uses.

if sql_variants_output="$(wasm_report_sql_variants "$sql_variants_mode" yes)"; then
    mapfile -t sql_variants <<<"$sql_variants_output"
else
    echo "[wasm-size] invalid --sql-variants value '$sql_variants_mode'; expected 'sql-on', 'sql-off', or 'both'" >&2
    exit 1
fi

build_variant() {
    local canister_name="$1"
    local sql_variant="$2"
    local sql_mode
    local artifact_suffix
    local stem=""

    sql_mode="${sql_variant#sql-}"
    artifact_suffix="$(wasm_report_size_suffix "$sql_variant" "${#sql_variants[@]}")"
    stem="${canister_name}.${profile}${artifact_suffix}"

    echo "[wasm-size] Building '$canister_name' using profile '$profile' ($sql_variant)"
    (
        cd "$ROOT"
        cargo run -p icydb-testing-integration --bin build_fixture_canister --locked -- \
            "$canister_name" \
            --build-profile production \
            --profile "$profile" \
            --sql-mode "$sql_mode" \
            --candid-export on
    )

    ICP_DIR="$ROOT/.icp/local/canisters/$canister_name"
    FINAL_WASM="$ICP_DIR/$canister_name.wasm"
    COMPILER_WASM="$ICP_DIR/$canister_name.compiler.wasm"
    RAW_DID="$ICP_DIR/$canister_name.did"

    if [[ ! -f "$FINAL_WASM" ]]; then
        echo "[wasm-size] expected final deployable wasm missing: $FINAL_WASM" >&2
        exit 1
    fi
    if [[ ! -f "$COMPILER_WASM" ]]; then
        echo "[wasm-size] expected compiler-emitted wasm missing: $COMPILER_WASM" >&2
        exit 1
    fi

    COMPILER_COPY="$out_dir/${stem}.compiler-emitted.wasm"
    FINAL_COPY="$out_dir/${stem}.final-deployable.wasm"
    FINAL_GZ="$out_dir/${stem}.final-deployable.wasm.gz"
    DID_COPY="$out_dir/${stem}.did"
    COMPILER_DID="$out_dir/${stem}.compiler-emitted.did"
    COMPILER_INFO="$out_dir/${stem}.compiler-emitted.info.txt"
    FINAL_INFO="$out_dir/${stem}.final-deployable.info.txt"
    REPORT_JSON="$out_dir/${stem}.report.json"
    SUMMARY_MD="$out_dir/${stem}.summary.md"

    cp "$COMPILER_WASM" "$COMPILER_COPY"
    cp "$FINAL_WASM" "$FINAL_COPY"
    rm -f "$DID_COPY"
    if [[ -f "$RAW_DID" ]]; then
        cp "$RAW_DID" "$DID_COPY"
    fi
    candid-extractor "$COMPILER_COPY" > "$COMPILER_DID"
    if ! cmp -s "$COMPILER_DID" "$DID_COPY"; then
        echo "[wasm-size] Candid drifted between compiler and final Wasm for '$canister_name'" >&2
        exit 1
    fi
    gzip -n -9 -c "$FINAL_COPY" > "$FINAL_GZ"

    ic-wasm "$COMPILER_COPY" info > "$COMPILER_INFO"
    ic-wasm "$FINAL_COPY" info > "$FINAL_INFO"

    (
        cd "$ROOT"
        cargo run -p icydb-testing-integration --bin write_wasm_size_report --locked -- \
            --canister "$canister_name" \
            --profile "$profile" \
            --sql-variant "$sql_variant" \
            --did "$DID_COPY" \
            --compiler-wasm "$COMPILER_COPY" \
            --final-wasm "$FINAL_COPY" \
            --final-gz "$FINAL_GZ" \
            --compiler-info "$COMPILER_INFO" \
            --final-info "$FINAL_INFO" \
            --report-json "$REPORT_JSON" \
            --summary-md "$SUMMARY_MD" \
            --ic-wasm-bin "$IC_WASM_BIN" \
            --wasm-opt-bin "$WASM_OPT_BIN"
    )

    echo "[wasm-size] Wrote report: $REPORT_JSON"
    echo "[wasm-size] Wrote summary: $SUMMARY_MD"
}

for canister_name in "${canister_names[@]}"; do
    for sql_variant in "${sql_variants[@]}"; do
        build_variant "$canister_name" "$sql_variant"
    done
done
