#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
OUT_DIR="$ROOT/artifacts/wasm-size"
CANISTER_NAME="${WASM_CANISTER_NAME:-minimal}"
PROFILE="${WASM_PROFILE:-wasm-release}"

mkdir -p "$OUT_DIR"

echo "[wasm-size] Building '$CANISTER_NAME' using profile '$PROFILE'"
(
    cd "$ROOT"
    export ICYDB_CANISTER_WASM_PROFILE="$PROFILE"
    export QUICKSTART_WASM_PROFILE="$PROFILE"
    cargo run -p icydb-testing-integration --bin build_quickstart_canister --locked -- "$CANISTER_NAME"
)

DFX_DIR="$ROOT/.dfx/local/canisters/$CANISTER_NAME"
RAW_WASM="$DFX_DIR/$CANISTER_NAME.wasm"
RAW_DID="$DFX_DIR/$CANISTER_NAME.did"
RAW_GZ_EMITTED="$DFX_DIR/$CANISTER_NAME.wasm.gz"

if [[ ! -f "$RAW_WASM" ]]; then
    echo "[wasm-size] expected wasm missing: $RAW_WASM" >&2
    exit 1
fi

if [[ ! -f "$RAW_DID" ]]; then
    echo "[wasm-size] expected did missing: $RAW_DID" >&2
    exit 1
fi

RAW_COPY="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.dfx-built.wasm"
RAW_GZ_DETERMINISTIC="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.dfx-built.wasm.gz"
RAW_GZ_EMITTED_COPY="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.dfx-emitted.wasm.gz"
DID_COPY="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.did"
SHRUNK_WASM="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.dfx-shrunk.wasm"
SHRUNK_GZ="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.dfx-shrunk.wasm.gz"
RAW_INFO="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.dfx-built.info.txt"
SHRUNK_INFO="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.dfx-shrunk.info.txt"
REPORT_JSON="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.report.json"
SUMMARY_MD="$OUT_DIR/${CANISTER_NAME}.${PROFILE}.summary.md"

cp "$RAW_WASM" "$RAW_COPY"
cp "$RAW_DID" "$DID_COPY"
gzip -n -9 -c "$RAW_COPY" > "$RAW_GZ_DETERMINISTIC"

if [[ -f "$RAW_GZ_EMITTED" ]]; then
    cp "$RAW_GZ_EMITTED" "$RAW_GZ_EMITTED_COPY"
fi

ic-wasm "$RAW_COPY" -o "$SHRUNK_WASM" shrink
gzip -n -9 -c "$SHRUNK_WASM" > "$SHRUNK_GZ"

ic-wasm "$RAW_COPY" info > "$RAW_INFO"
ic-wasm "$SHRUNK_WASM" info > "$SHRUNK_INFO"

export CANISTER_NAME PROFILE RAW_COPY RAW_GZ_DETERMINISTIC RAW_GZ_EMITTED_COPY SHRUNK_WASM SHRUNK_GZ RAW_INFO SHRUNK_INFO DID_COPY REPORT_JSON SUMMARY_MD
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
    "artifacts": {
        "did": file_meta(did_path),
        "dfx_built_wasm": raw_wasm_meta,
        "dfx_built_wasm_gz_deterministic": raw_gz_meta,
        "dfx_built_wasm_gz_emitted": emitted_gz_meta,
        "dfx_shrunk_wasm": shrunk_wasm_meta,
        "dfx_shrunk_wasm_gz": shrunk_gz_meta,
    },
    "analysis": {
        "dfx_built": raw_info_meta,
        "dfx_shrunk": shrunk_info_meta,
    },
    "deltas": {
        "shrink_wasm_bytes": raw_wasm_meta["bytes"] - shrunk_wasm_meta["bytes"],
        "shrink_wasm_gz_bytes": raw_gz_meta["bytes"] - shrunk_gz_meta["bytes"],
    },
}

report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

summary_lines = [
    f"## Wasm Size Report: `{canister}` ({profile})",
    "",
    "| Artifact | Bytes |",
    "| --- | ---: |",
    f"| dfx-built `.wasm` | {raw_wasm_meta['bytes']} |",
    f"| dfx-built deterministic `.wasm.gz` | {raw_gz_meta['bytes']} |",
]

if emitted_gz_meta is not None:
    summary_lines.append(
        f"| dfx-emitted `.wasm.gz` | {emitted_gz_meta['bytes']} |"
    )

summary_lines.extend(
    [
        f"| dfx-shrunk `.wasm` (canonical) | {shrunk_wasm_meta['bytes']} |",
        f"| dfx-shrunk `.wasm.gz` (canonical) | {shrunk_gz_meta['bytes']} |",
        f"| Shrink delta `.wasm` | {report['deltas']['shrink_wasm_bytes']} |",
        f"| Shrink delta `.wasm.gz` | {report['deltas']['shrink_wasm_gz_bytes']} |",
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
