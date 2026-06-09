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
done
if [[ "${#canister_names[@]}" -eq 0 ]]; then
    mapfile -t canister_names < <(wasm_report_default_canisters)
fi

mkdir -p "$out_dir"

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
            --profile "$profile" \
            --sql-mode "$sql_mode"
    )

    ICP_DIR="$ROOT/.icp/local/canisters/$canister_name"
    RAW_WASM="$ICP_DIR/$canister_name.wasm"
    RAW_GZ_EMITTED="$ICP_DIR/$canister_name.wasm.gz"
    RAW_DID="$ICP_DIR/$canister_name.did"

    if [[ ! -f "$RAW_WASM" ]]; then
        echo "[wasm-size] expected wasm missing: $RAW_WASM" >&2
        exit 1
    fi

    RAW_COPY="$out_dir/${stem}.icp-built.wasm"
    RAW_GZ_DETERMINISTIC="$out_dir/${stem}.icp-built.wasm.gz"
    RAW_GZ_EMITTED_COPY="$out_dir/${stem}.icp-emitted.wasm.gz"
    DID_COPY="$out_dir/${stem}.did"
    SHRUNK_WASM="$out_dir/${stem}.icp-shrunk.wasm"
    SHRUNK_GZ="$out_dir/${stem}.icp-shrunk.wasm.gz"
    RAW_INFO="$out_dir/${stem}.icp-built.info.txt"
    SHRUNK_INFO="$out_dir/${stem}.icp-shrunk.info.txt"
    REPORT_JSON="$out_dir/${stem}.report.json"
    SUMMARY_MD="$out_dir/${stem}.summary.md"

    cp "$RAW_WASM" "$RAW_COPY"
    rm -f "$DID_COPY"
    if [[ -f "$RAW_DID" ]]; then
        cp "$RAW_DID" "$DID_COPY"
    fi
    gzip -n -9 -c "$RAW_COPY" > "$RAW_GZ_DETERMINISTIC"

    if [[ -f "$RAW_GZ_EMITTED" ]]; then
        cp "$RAW_GZ_EMITTED" "$RAW_GZ_EMITTED_COPY"
    fi

    ic-wasm "$RAW_COPY" -o "$SHRUNK_WASM" shrink
    gzip -n -9 -c "$SHRUNK_WASM" > "$SHRUNK_GZ"

    ic-wasm "$RAW_COPY" info > "$RAW_INFO"
    ic-wasm "$SHRUNK_WASM" info > "$SHRUNK_INFO"

    (
        cd "$ROOT"
        cargo run -p icydb-testing-integration --bin write_wasm_size_report --locked -- \
            --canister "$canister_name" \
            --profile "$profile" \
            --sql-variant "$sql_variant" \
            --did "$DID_COPY" \
            --raw-wasm "$RAW_COPY" \
            --raw-gz "$RAW_GZ_DETERMINISTIC" \
            --raw-gz-emitted "$RAW_GZ_EMITTED_COPY" \
            --shrunk-wasm "$SHRUNK_WASM" \
            --shrunk-gz "$SHRUNK_GZ" \
            --raw-info "$RAW_INFO" \
            --shrunk-info "$SHRUNK_INFO" \
            --report-json "$REPORT_JSON" \
            --summary-md "$SUMMARY_MD"
    )

    echo "[wasm-size] Wrote report: $REPORT_JSON"
    echo "[wasm-size] Wrote summary: $SUMMARY_MD"
}

for canister_name in "${canister_names[@]}"; do
    for sql_variant in "${sql_variants[@]}"; do
        build_variant "$canister_name" "$sql_variant"
    done
done
