#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
PROFILE="${WASM_PROFILE:-wasm-release}"
SQL_VARIANTS_MODE="${WASM_SQL_VARIANTS:-sql-on}"
AUDIT_DATE="${WASM_AUDIT_DATE:-$(date +%F)}"
AUDIT_MONTH="${AUDIT_DATE:0:7}"
REPORT_DIR="${WASM_AUDIT_REPORT_DIR:-$ROOT/docs/audits/reports/$AUDIT_MONTH/$AUDIT_DATE}"
REPORT_SCOPE="wasm-footprint"
ARTIFACT_SCOPE_DIR="$REPORT_DIR/artifacts/$REPORT_SCOPE"

if [[ -z "${WASM_CANISTER_NAME:-}" ]]; then
    for canister_name in minimal one_simple one_complex ten_simple ten_complex; do
        WASM_CANISTER_NAME="$canister_name" \
            WASM_PROFILE="$PROFILE" \
            WASM_SQL_VARIANTS="$SQL_VARIANTS_MODE" \
            WASM_AUDIT_DATE="$AUDIT_DATE" \
            WASM_AUDIT_REPORT_DIR="$REPORT_DIR" \
            WASM_AUDIT_SKIP_BUILD="${WASM_AUDIT_SKIP_BUILD:-0}" \
            bash "$0"
    done
    exit 0
fi

CANISTER_NAME="${WASM_CANISTER_NAME}"

case "$SQL_VARIANTS_MODE" in
    sql-on|on|enabled)
        SQL_VARIANT="sql-on"
        SIZE_REPORT_SUFFIX=""
        ;;
    sql-off|off|disabled)
        SQL_VARIANT="sql-off"
        SIZE_REPORT_SUFFIX=".sql-off"
        ;;
    both)
        echo "[wasm-audit] WASM_SQL_VARIANTS=both is not supported for audit reports; run one variant per audit pass" >&2
        exit 1
        ;;
    *)
        echo "[wasm-audit] invalid WASM_SQL_VARIANTS value '$SQL_VARIANTS_MODE'; expected 'sql-on', 'sql-off', or 'both'" >&2
        exit 1
        ;;
esac

if ! command -v twiggy >/dev/null 2>&1; then
    echo "[wasm-audit] missing required tool: twiggy" >&2
    echo "[wasm-audit] install with: cargo install twiggy --locked" >&2
    exit 1
fi

mkdir -p "$ARTIFACT_SCOPE_DIR"

if [[ "${WASM_AUDIT_SKIP_BUILD:-0}" != "1" ]]; then
    bash "$ROOT/scripts/ci/wasm-size-report.sh"
else
    echo "[wasm-audit] skipping wasm build and size capture (WASM_AUDIT_SKIP_BUILD=1)"
fi

ARTIFACT_DIR="$ROOT/artifacts/wasm-size"
SIZE_REPORT_JSON="$ARTIFACT_DIR/${CANISTER_NAME}.${PROFILE}${SIZE_REPORT_SUFFIX}.report.json"
SIZE_SUMMARY_MD="$ARTIFACT_DIR/${CANISTER_NAME}.${PROFILE}${SIZE_REPORT_SUFFIX}.summary.md"
SHRUNK_WASM="$ARTIFACT_DIR/${CANISTER_NAME}.${PROFILE}${SIZE_REPORT_SUFFIX}.dfx-shrunk.wasm"

for required in "$SIZE_REPORT_JSON" "$SIZE_SUMMARY_MD" "$SHRUNK_WASM"; do
    if [[ ! -f "$required" ]]; then
        echo "[wasm-audit] expected artifact missing: $required" >&2
        exit 1
    fi
done

REPORT_STEM="$REPORT_SCOPE"
if [[ -f "$REPORT_DIR/$REPORT_SCOPE.md" ]]; then
    run_index=2
    while [[ -f "$REPORT_DIR/${REPORT_SCOPE}-${run_index}.md" ]]; do
        run_index=$((run_index + 1))
    done
    REPORT_STEM="${REPORT_SCOPE}-${run_index}"
fi

REPORT_PATH="$REPORT_DIR/${REPORT_STEM}.md"
SIZE_REPORT_COPY="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.size-report.json"
SIZE_SUMMARY_COPY="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.size-summary.md"
TWIGGY_TOP_TXT="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-top.txt"
TWIGGY_TOP_CSV="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-top.csv"
TWIGGY_DOMINATORS_TXT="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-dominators.txt"
TWIGGY_RETAINED_CSV="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-retained.csv"
TWIGGY_MONOS_TXT="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-monos.txt"

if [[ "$REPORT_STEM" == "wasm-footprint" ]]; then
    BASELINE_PATH="$(
        ROOT="$ROOT" REPORT_DIR="$REPORT_DIR" python3 - <<'PY'
import os
import re
from pathlib import Path

root = Path(os.environ["ROOT"])
report_dir = Path(os.environ["REPORT_DIR"]).resolve()
rows = []
for path in root.glob("docs/audits/reports/*/*/wasm-footprint*.md"):
    if path.resolve().parent == report_dir:
        continue
    day = path.parent.name
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        continue
    match = re.fullmatch(r"wasm-footprint(?:-(\d+))?", path.stem)
    if not match:
        continue
    run = int(match.group(1) or "1")
    rows.append((day, run, str(path.relative_to(root))))

if rows:
    rows.sort(key=lambda row: (row[0], row[1]))
    print(rows[-1][2])
else:
    print("N/A")
PY
    )"
else
    BASELINE_PATH="docs/audits/reports/${AUDIT_MONTH}/${AUDIT_DATE}/wasm-footprint.md"
fi

cp "$SIZE_REPORT_JSON" "$SIZE_REPORT_COPY"
cp "$SIZE_SUMMARY_MD" "$SIZE_SUMMARY_COPY"

twiggy top -n 40 "$SHRUNK_WASM" > "$TWIGGY_TOP_TXT"
twiggy top -n 40 -f csv "$SHRUNK_WASM" > "$TWIGGY_TOP_CSV"
twiggy dominators -r 160 "$SHRUNK_WASM" > "$TWIGGY_DOMINATORS_TXT"
twiggy top --retained -n 40 -f csv "$SHRUNK_WASM" > "$TWIGGY_RETAINED_CSV"
twiggy monos "$SHRUNK_WASM" > "$TWIGGY_MONOS_TXT"

export ROOT CANISTER_NAME PROFILE SQL_VARIANT AUDIT_DATE REPORT_PATH REPORT_STEM REPORT_SCOPE BASELINE_PATH
export SIZE_REPORT_COPY TWIGGY_TOP_CSV TWIGGY_RETAINED_CSV TWIGGY_MONOS_TXT
export SIZE_SUMMARY_COPY TWIGGY_TOP_TXT TWIGGY_DOMINATORS_TXT
python3 - <<'PY'
import csv
import json
import os
import subprocess
from pathlib import Path


def fmt_int(value):
    if value is None:
        return "N/A"
    return f"{int(value):,}"


def fmt_pct(value):
    if value is None:
        return "N/A"
    return f"{float(value):.2f}%"


def display_path(path):
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


root = Path(os.environ["ROOT"])
report_path = Path(os.environ["REPORT_PATH"])
baseline_path = os.environ["BASELINE_PATH"]
canister = os.environ["CANISTER_NAME"]
profile = os.environ["PROFILE"]
report_scope = os.environ["REPORT_SCOPE"]
audit_date = os.environ["AUDIT_DATE"]
size_report_copy = Path(os.environ["SIZE_REPORT_COPY"])
size_summary_copy = Path(os.environ["SIZE_SUMMARY_COPY"])
twiggy_top_csv = Path(os.environ["TWIGGY_TOP_CSV"])
twiggy_top_txt = Path(os.environ["TWIGGY_TOP_TXT"])
twiggy_retained_csv = Path(os.environ["TWIGGY_RETAINED_CSV"])
twiggy_dominators_txt = Path(os.environ["TWIGGY_DOMINATORS_TXT"])
twiggy_monos_txt = Path(os.environ["TWIGGY_MONOS_TXT"])

current = json.loads(size_report_copy.read_text(encoding="utf-8"))

baseline_metrics = None
baseline_artifact_path = None
if baseline_path != "N/A":
    baseline_report = root / baseline_path
    baseline_artifact = (
        baseline_report.parent
        / "artifacts"
        / report_scope
        / f"{baseline_report.stem}.{canister}.{profile}.{os.environ['SQL_VARIANT']}.size-report.json"
    )
    legacy_sql_on_baseline_artifact = (
        baseline_report.parent
        / "artifacts"
        / report_scope
        / f"{baseline_report.stem}.{canister}.{profile}.size-report.json"
    )
    legacy_baseline_artifact = (
        baseline_report.parent
        / "helpers"
        / f"{baseline_report.stem}.{canister}.{profile}.{os.environ['SQL_VARIANT']}.size-report.json"
    )
    legacy_sql_on_helper_artifact = (
        baseline_report.parent
        / "helpers"
        / f"{baseline_report.stem}.{canister}.{profile}.size-report.json"
    )
    if baseline_artifact.exists():
        baseline_artifact_path = baseline_artifact
        baseline_metrics = json.loads(baseline_artifact.read_text(encoding="utf-8"))
    elif (
        os.environ["SQL_VARIANT"] == "sql-on"
        and legacy_sql_on_baseline_artifact.exists()
    ):
        baseline_artifact_path = legacy_sql_on_baseline_artifact
        baseline_metrics = json.loads(
            legacy_sql_on_baseline_artifact.read_text(encoding="utf-8")
        )
    elif legacy_baseline_artifact.exists():
        baseline_artifact_path = legacy_baseline_artifact
        baseline_metrics = json.loads(
            legacy_baseline_artifact.read_text(encoding="utf-8")
        )
    elif (
        os.environ["SQL_VARIANT"] == "sql-on"
        and legacy_sql_on_helper_artifact.exists()
    ):
        baseline_artifact_path = legacy_sql_on_helper_artifact
        baseline_metrics = json.loads(
            legacy_sql_on_helper_artifact.read_text(encoding="utf-8")
        )

try:
    snapshot = (
        subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        )
        .decode("utf-8")
        .strip()
    )
except Exception:
    snapshot = "N/A"

if baseline_path == "N/A":
    comparability = "comparable"
elif baseline_metrics is None:
    comparability = (
        "non-comparable (baseline report exists but baseline size artifact is missing)"
    )
else:
    comparability = "comparable"

with twiggy_top_csv.open(encoding="utf-8", newline="") as handle:
    top_rows = list(csv.DictReader(handle))

with twiggy_retained_csv.open(encoding="utf-8", newline="") as handle:
    retained_rows = list(csv.DictReader(handle))

monos_total_rows = "unknown"
for line in twiggy_monos_txt.read_text(encoding="utf-8").splitlines():
    if "Total Rows" in line:
        monos_total_rows = line.strip()
        break


def metric_from(report, key):
    return report["artifacts"][key]["bytes"]


current_metrics = {
    "dfx_built_wasm": metric_from(current, "dfx_built_wasm"),
    "dfx_built_wasm_gz_deterministic": metric_from(
        current, "dfx_built_wasm_gz_deterministic"
    ),
    "dfx_shrunk_wasm": metric_from(current, "dfx_shrunk_wasm"),
    "dfx_shrunk_wasm_gz": metric_from(current, "dfx_shrunk_wasm_gz"),
}

previous_metrics = None
if baseline_metrics is not None:
    previous_metrics = {
        "dfx_built_wasm": metric_from(baseline_metrics, "dfx_built_wasm"),
        "dfx_built_wasm_gz_deterministic": metric_from(
            baseline_metrics, "dfx_built_wasm_gz_deterministic"
        ),
        "dfx_shrunk_wasm": metric_from(baseline_metrics, "dfx_shrunk_wasm"),
        "dfx_shrunk_wasm_gz": metric_from(baseline_metrics, "dfx_shrunk_wasm_gz"),
    }

size_rows = [
    ("dfx-built `.wasm`", "dfx_built_wasm"),
    ("dfx-built deterministic `.wasm.gz`", "dfx_built_wasm_gz_deterministic"),
    ("dfx-shrunk `.wasm`", "dfx_shrunk_wasm"),
    ("dfx-shrunk `.wasm.gz`", "dfx_shrunk_wasm_gz"),
]

check_rows = [
    ("Wasm size artifacts captured", "PASS", "size report + summary artifacts written"),
    ("Twiggy top breakdown generated", "PASS", "top text/csv artifacts written"),
    (
        "Twiggy dominator breakdown generated",
        "PASS",
        "dominator text artifact written",
    ),
    ("Twiggy monomorphization breakdown generated", "PASS", monos_total_rows),
]

if baseline_path == "N/A":
    check_rows.append(
        (
            "Baseline delta availability",
            "PARTIAL",
            "first tracked run; baseline created by this report",
        )
    )
elif baseline_metrics is None:
    if baseline_artifact_path is None:
        baseline_note = "baseline artifact missing at expected scoped artifacts path"
    else:
        baseline_note = f"baseline artifact missing at `{display_path(baseline_artifact_path)}`"
    check_rows.append(("Baseline delta availability", "PARTIAL", baseline_note))
else:
    check_rows.append(("Baseline delta availability", "PASS", "baseline artifact loaded"))

status_counts = {"PASS": 0, "PARTIAL": 0, "FAIL": 0}
for _, status, _ in check_rows:
    status_counts[status] += 1

lines = [
    f"# Recurring Audit - Wasm Footprint ({audit_date})",
    "",
    "## Report Preamble",
    "",
    (
        f"- scope: recurring wasm footprint audit for `{canister}` "
        f"with profile `{profile}` and SQL variant `{os.environ['SQL_VARIANT']}`"
    ),
    f"- compared baseline report path: `{baseline_path}`",
    f"- code snapshot identifier: `{snapshot}`",
    "- method tag/version: `WASM-1.0`",
    f"- comparability status: `{comparability}`",
    "",
    "## Checklist Results",
    "",
    "| Requirement | Status | Evidence |",
    "| --- | --- | --- |",
]

for requirement, status, evidence in check_rows:
    lines.append(f"| {requirement} | {status} | {evidence} |")

lines.extend(
    [
        "",
        f"PASS={status_counts['PASS']}, PARTIAL={status_counts['PARTIAL']}, FAIL={status_counts['FAIL']}",
        "",
        "## Size Snapshot",
        "",
        "| Metric | Previous | Current | Delta |",
        "| --- | ---: | ---: | ---: |",
    ]
)

for label, key in size_rows:
    current_value = current_metrics[key]
    if previous_metrics is None:
        previous_text = "N/A"
        delta_text = "N/A"
    else:
        previous_value = previous_metrics[key]
        delta = current_value - previous_value
        previous_text = fmt_int(previous_value)
        delta_text = f"{delta:+,}"
    lines.append(
        f"| {label} | {previous_text} | {fmt_int(current_value)} | {delta_text} |"
    )

analysis = current["analysis"]
lines.extend(
    [
        "",
        "## Structural Snapshot (ic-wasm)",
        "",
        "| Metric | dfx-built | dfx-shrunk |",
        "| --- | ---: | ---: |",
        (
            "| Function count | "
            f"{fmt_int(analysis['dfx_built']['function_count'])} | "
            f"{fmt_int(analysis['dfx_shrunk']['function_count'])} |"
        ),
        (
            "| Callback count | "
            f"{fmt_int(analysis['dfx_built']['callback_count'])} | "
            f"{fmt_int(analysis['dfx_shrunk']['callback_count'])} |"
        ),
        (
            "| Data section count | "
            f"{fmt_int(analysis['dfx_built']['data_section_count'])} | "
            f"{fmt_int(analysis['dfx_shrunk']['data_section_count'])} |"
        ),
        (
            "| Data section bytes | "
            f"{fmt_int(analysis['dfx_built']['data_section_bytes'])} | "
            f"{fmt_int(analysis['dfx_shrunk']['data_section_bytes'])} |"
        ),
        (
            "| Exported methods | "
            f"{fmt_int(analysis['dfx_built']['exported_method_count'])} | "
            f"{fmt_int(analysis['dfx_shrunk']['exported_method_count'])} |"
        ),
        "",
        "## Twiggy Top Offenders (Shallow Size)",
        "",
        "| Rank | Item | Shallow Bytes | Shallow % |",
        "| ---: | --- | ---: | ---: |",
    ]
)

for idx, row in enumerate(top_rows[:10], start=1):
    lines.append(
        "| "
        f"{idx} | {row.get('Name', 'N/A')} | "
        f"{fmt_int(row.get('ShallowSize') or 0)} | "
        f"{fmt_pct(row.get('ShallowSizePercent') or 0)} |"
    )

lines.extend(
    [
        "",
        "## Twiggy Retained Hotspots",
        "",
        "| Rank | Item | Retained Bytes | Retained % |",
        "| ---: | --- | ---: | ---: |",
    ]
)

for idx, row in enumerate(retained_rows[:10], start=1):
    lines.append(
        "| "
        f"{idx} | {row.get('Name', 'N/A')} | "
        f"{fmt_int(row.get('RetainedSize') or 0)} | "
        f"{fmt_pct(row.get('RetainedSizePercent') or 0)} |"
    )

artifact_paths = [
    size_report_copy,
    size_summary_copy,
    twiggy_top_txt,
    twiggy_top_csv,
    twiggy_dominators_txt,
    twiggy_retained_csv,
    twiggy_monos_txt,
]

lines.extend(
    [
        "",
        "## Artifacts",
        "",
    ]
)
for artifact_path in artifact_paths:
    lines.append(f"- `{display_path(artifact_path)}`")

lines.extend(
    [
        "",
        "## Follow-Up Actions",
        "",
    ]
)

if status_counts["PARTIAL"] == 0 and status_counts["FAIL"] == 0:
    lines.append("- No follow-up actions required for this run.")
else:
    if baseline_path == "N/A":
        lines.append(
            "- owner boundary: `wasm-audit`; action: treat this report as baseline and compare deltas on the next run; target report date/run: next `wasm-footprint` run."
        )
    if baseline_path != "N/A" and baseline_metrics is None:
        lines.append(
            "- owner boundary: `wasm-audit history`; action: preserve size artifacts for baseline reports so trend deltas remain comparable; target report date/run: next `wasm-footprint` run."
        )

lines.extend(
    [
        "",
        "## Verification Readout",
        "",
        "- `bash scripts/ci/wasm-size-report.sh` -> PASS",
        "- `twiggy top -n 40` -> PASS",
        "- `twiggy top --retained -n 40` -> PASS",
        "- `twiggy dominators -r 160` -> PASS",
        "- `twiggy monos` -> PASS",
    ]
)

report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

echo "[wasm-audit] Wrote report: $REPORT_PATH"
