#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
out_dir="$ROOT/artifacts/wasm-size"
profile="wasm-release"
sql_variants_mode="sql-on"
canister_names=()
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

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
    canister_names=(minimal one_simple one_complex ten_simple ten_complex)
fi

mkdir -p "$out_dir"

# The wasm size report consumes locally staged canister artifacts under
# `.icp/local/canisters/<name>/`, but the staging step is owned by
# `build_fixture_canister` and does not require `icp` or a live local replica.
# Keep this script independent from the local ICP environment so CI can run
# wasm-size measurements without provisioning replica tooling it never uses.

case "$sql_variants_mode" in
    both)
        sql_variants=("sql-on" "sql-off")
        ;;
    sql-on|on|enabled)
        sql_variants=("sql-on")
        ;;
    sql-off|off|disabled)
        sql_variants=("sql-off")
        ;;
    *)
        echo "[wasm-size] invalid --sql-variants value '$sql_variants_mode'; expected 'sql-on', 'sql-off', or 'both'" >&2
        exit 1
        ;;
esac

build_variant() {
    local canister_name="$1"
    local sql_variant="$2"
    local sql_mode="on"
    local artifact_suffix=""
    local stem=""

    if [[ "$sql_variant" == "sql-off" ]]; then
        sql_mode="off"
    fi

    if [[ "${#sql_variants[@]}" -gt 1 || "$sql_variant" == "sql-off" ]]; then
        artifact_suffix=".$sql_variant"
    fi

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

    export CANISTER_NAME="$canister_name"
    export PROFILE="$profile"
    export RAW_COPY RAW_GZ_DETERMINISTIC RAW_GZ_EMITTED_COPY SHRUNK_WASM SHRUNK_GZ RAW_INFO SHRUNK_INFO DID_COPY REPORT_JSON SUMMARY_MD
    export SQL_VARIANT="$sql_variant"
    python3 - <<'PY'
import hashlib
import json
import os
import re
from pathlib import Path


def sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def file_meta(path: Path) -> dict:
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def parse_info(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")

    def int_field(pattern: str):
        match = re.search(pattern, text)
        return int(match.group(1)) if match else None

    exports = []
    export_block = re.search(r"Exported methods:\s*\[(.*?)\]\s*", text, re.S)
    if export_block:
        for line in export_block.group(1).splitlines():
            line = line.strip().rstrip(",")
            if line.startswith('"') and line.endswith('"'):
                exports.append(line.strip('"'))

    return {
        "function_count": int_field(r"Number of functions:\s*(\d+)"),
        "callback_count": int_field(r"Number of callbacks:\s*(\d+)"),
        "data_section_count": int_field(r"Number of data sections:\s*(\d+)"),
        "data_section_bytes": int_field(r"Size of data sections:\s*(\d+) bytes"),
        "exported_method_count": len(exports),
        "exported_methods": exports,
    }


canister = os.environ["CANISTER_NAME"]
profile = os.environ["PROFILE"]
sql_variant = os.environ["SQL_VARIANT"]
raw_wasm = Path(os.environ["RAW_COPY"])
raw_gz = Path(os.environ["RAW_GZ_DETERMINISTIC"])
raw_gz_emitted = Path(os.environ["RAW_GZ_EMITTED_COPY"])
shrunk_wasm = Path(os.environ["SHRUNK_WASM"])
shrunk_gz = Path(os.environ["SHRUNK_GZ"])
raw_info = Path(os.environ["RAW_INFO"])
shrunk_info = Path(os.environ["SHRUNK_INFO"])
did_path = Path(os.environ["DID_COPY"])
report_path = Path(os.environ["REPORT_JSON"])
summary_path = Path(os.environ["SUMMARY_MD"])

raw_wasm_meta = file_meta(raw_wasm)
raw_gz_meta = file_meta(raw_gz)
shrunk_wasm_meta = file_meta(shrunk_wasm)
shrunk_gz_meta = file_meta(shrunk_gz)
raw_info_meta = parse_info(raw_info)
shrunk_info_meta = parse_info(shrunk_info)

emitted_gz_meta = file_meta(raw_gz_emitted) if raw_gz_emitted.exists() else None

report = {
    "canister": canister,
    "profile": profile,
    "sql_variant": sql_variant,
    "artifacts": {
        "did": file_meta(did_path) if did_path.exists() else None,
        "candid_export": "available" if did_path.exists() else "unavailable",
        "icp_built_wasm": raw_wasm_meta,
        "icp_built_wasm_gz_deterministic": raw_gz_meta,
        "icp_built_wasm_gz_emitted": emitted_gz_meta,
        "icp_shrunk_wasm": shrunk_wasm_meta,
        "icp_shrunk_wasm_gz": shrunk_gz_meta,
    },
    "analysis": {
        "icp_built": raw_info_meta,
        "icp_shrunk": shrunk_info_meta,
    },
    "deltas": {
        "shrink_wasm_bytes": raw_wasm_meta["bytes"] - shrunk_wasm_meta["bytes"],
        "shrink_wasm_gz_bytes": raw_gz_meta["bytes"] - shrunk_gz_meta["bytes"],
    },
}

report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

summary_lines = [
    f"## Wasm Size Report: `{canister}` ({profile}, {sql_variant})",
    "",
    "| Artifact | Bytes |",
    "| --- | ---: |",
    f"| icp-built `.wasm` | {raw_wasm_meta['bytes']} |",
    f"| icp-built deterministic `.wasm.gz` | {raw_gz_meta['bytes']} |",
]

if emitted_gz_meta is not None:
    summary_lines.append(
        f"| icp-emitted `.wasm.gz` | {emitted_gz_meta['bytes']} |"
    )

summary_lines.append(
    f"| candid export | {report['artifacts']['candid_export']} |"
)

summary_lines.extend(
    [
        f"| icp-shrunk `.wasm` (canonical) | {shrunk_wasm_meta['bytes']} |",
        f"| icp-shrunk `.wasm.gz` (canonical) | {shrunk_gz_meta['bytes']} |",
        f"| Shrink delta `.wasm` | {report['deltas']['shrink_wasm_bytes']} |",
        f"| Shrink delta `.wasm.gz` | {report['deltas']['shrink_wasm_gz_bytes']} |",
        "",
        f"SQL variant: `{sql_variant}`",
        "",
        f"Exports (shrunk): {shrunk_info_meta['exported_method_count']}",
        "",
        f"JSON report: `{report_path}`",
    ]
)

summary = "\n".join(summary_lines) + "\n"
summary_path.write_text(summary, encoding="utf-8")

step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
if step_summary:
    with open(step_summary, "a", encoding="utf-8") as handle:
        handle.write(summary)
PY

    echo "[wasm-size] Wrote report: $REPORT_JSON"
    echo "[wasm-size] Wrote summary: $SUMMARY_MD"
}

for canister_name in "${canister_names[@]}"; do
    for sql_variant in "${sql_variants[@]}"; do
        build_variant "$canister_name" "$sql_variant"
    done
done
