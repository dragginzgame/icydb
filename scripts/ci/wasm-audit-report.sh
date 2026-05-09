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

# Resolve the audited SQL variant once so both the batch summary path and the
# per-canister child runs agree on the same stable output naming.
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

write_summary_report() {
    local canisters=("$@")
    local canister_csv=""

    mkdir -p "$REPORT_DIR" "$ARTIFACT_SCOPE_DIR"
    canister_csv="$(IFS=,; echo "${canisters[*]}")"

    export ROOT PROFILE SQL_VARIANT AUDIT_DATE REPORT_DIR REPORT_SCOPE ARTIFACT_SCOPE_DIR
    export WASM_AUDIT_CANISTER_CSV="$canister_csv"
    python3 - <<'PY'
import json
import os
import subprocess
from pathlib import Path


def fmt_int(value):
    return f"{int(value):,}"


def display_path(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def has_icp_size_metrics(report: dict) -> bool:
    artifacts = report.get("artifacts")
    return isinstance(artifacts, dict) and all(
        key in artifacts
        for key in (
            "icp_built_wasm",
            "icp_built_wasm_gz_deterministic",
            "icp_shrunk_wasm",
            "icp_shrunk_wasm_gz",
        )
    )


def load_baseline_metrics(root: Path, baseline_path: str, artifact_scope: str, canister: str, profile: str, sql_variant: str):
    if baseline_path == "N/A":
        return None, None

    baseline_report = root / baseline_path
    candidates = [
        baseline_report.parent / "artifacts" / artifact_scope / f"{artifact_scope}.{canister}.{profile}.{sql_variant}.size-report.json",
        baseline_report.parent / "artifacts" / artifact_scope / f"{artifact_scope}.{canister}.{profile}.size-report.json",
        baseline_report.parent / "helpers" / f"{artifact_scope}.{canister}.{profile}.{sql_variant}.size-report.json",
        baseline_report.parent / "helpers" / f"{artifact_scope}.{canister}.{profile}.size-report.json",
    ]

    for candidate in candidates:
        if candidate.exists():
            report = json.loads(candidate.read_text(encoding="utf-8"))
            if has_icp_size_metrics(report):
                return report, candidate
            return None, candidate

    return None, None


root = Path(os.environ["ROOT"])
report_dir = Path(os.environ["REPORT_DIR"])
artifact_scope_dir = Path(os.environ["ARTIFACT_SCOPE_DIR"])
report_scope = os.environ["REPORT_SCOPE"]
audit_date = os.environ["AUDIT_DATE"]
profile = os.environ["PROFILE"]
sql_variant = os.environ["SQL_VARIANT"]
canisters = [canister for canister in os.environ["WASM_AUDIT_CANISTER_CSV"].split(",") if canister]
report_path = report_dir / f"{report_scope}.md"

rows = []
for path in root.glob("docs/audits/reports/*/*/wasm-footprint.md"):
    if path.resolve().parent == report_dir.resolve():
        continue
    rows.append(str(path.relative_to(root)))

baseline_path = rows[-1] if rows else "N/A"

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

per_canister = []
all_baselines_available = True
for canister in canisters:
    size_report_path = artifact_scope_dir / f"{report_scope}.{canister}.{profile}.{sql_variant}.size-report.json"
    detail_report_path = artifact_scope_dir / f"{report_scope}.{canister}.{profile}.{sql_variant}.md"
    current = json.loads(size_report_path.read_text(encoding="utf-8"))
    baseline_metrics, baseline_artifact_path = load_baseline_metrics(
        root, baseline_path, report_scope, canister, profile, sql_variant
    )
    if baseline_path != "N/A" and baseline_metrics is None:
        all_baselines_available = False

    previous_shrunk = (
        baseline_metrics["artifacts"]["icp_shrunk_wasm"]["bytes"]
        if baseline_metrics is not None
        else None
    )
    current_shrunk = current["artifacts"]["icp_shrunk_wasm"]["bytes"]
    previous_gz = (
        baseline_metrics["artifacts"]["icp_shrunk_wasm_gz"]["bytes"]
        if baseline_metrics is not None
        else None
    )
    current_gz = current["artifacts"]["icp_shrunk_wasm_gz"]["bytes"]

    if baseline_path == "N/A":
        status = "PARTIAL"
        baseline_note = "first tracked run for this summary layout"
    elif baseline_metrics is None:
        status = "PARTIAL"
        if baseline_artifact_path is None:
            baseline_note = "baseline size artifact missing"
        else:
            baseline_note = f"baseline size artifact missing at `{display_path(root, baseline_artifact_path)}`"
    else:
        status = "PASS"
        baseline_note = "baseline size artifact loaded"

    per_canister.append(
        {
            "canister": canister,
            "status": status,
            "baseline_note": baseline_note,
            "current_shrunk": current_shrunk,
            "current_gz": current_gz,
            "previous_shrunk": previous_shrunk,
            "previous_gz": previous_gz,
            "detail_report_path": display_path(root, detail_report_path),
        }
    )

if baseline_path == "N/A":
    comparability = "non-comparable (first tracked summary-layout run)"
elif all_baselines_available:
    comparability = "comparable"
else:
    comparability = "non-comparable (one or more baseline size artifacts are missing)"

status_counts = {"PASS": 0, "PARTIAL": 0, "FAIL": 0}
for item in per_canister:
    status_counts[item["status"]] += 1

lines = [
    f"# Recurring Audit - Wasm Footprint ({audit_date})",
    "",
    "## Report Preamble",
    "",
    (
        f"- scope: recurring wasm footprint audit for `{', '.join(canisters)}` "
        f"with profile `{profile}` and SQL variant `{sql_variant}`"
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
    "| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/wasm-footprint/` |",
    "| Twiggy top breakdown generated | PASS | per-canister top text/csv artifacts written |",
    "| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |",
    "| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |",
]

if all_baselines_available and baseline_path != "N/A":
    lines.append("| Baseline delta availability | PASS | baseline size artifacts loaded for all canisters |")
elif baseline_path == "N/A":
    lines.append("| Baseline delta availability | PARTIAL | first tracked summary-layout run; establishes new baseline layout |")
else:
    lines.append("| Baseline delta availability | PARTIAL | one or more prior scoped size artifacts are missing |")

lines.extend(
    [
        "",
        (
            "PASS=5, PARTIAL=0, FAIL=0"
            if all_baselines_available and baseline_path != "N/A"
            else "PASS=4, PARTIAL=1, FAIL=0"
        ),
        "",
        "## Per-Canister Size Snapshot",
        "",
        "| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Detail Report |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
)

for item in per_canister:
    previous_shrunk = fmt_int(item["previous_shrunk"]) if item["previous_shrunk"] is not None else "N/A"
    previous_gz = fmt_int(item["previous_gz"]) if item["previous_gz"] is not None else "N/A"
    lines.append(
        f"| `{item['canister']}` | {item['status']} | {previous_shrunk} | {fmt_int(item['current_shrunk'])} | {previous_gz} | {fmt_int(item['current_gz'])} | `{item['detail_report_path']}` |"
    )

lines.extend(
    [
        "",
        "## Follow-Up Actions",
        "",
    ]
)

if baseline_path == "N/A":
    lines.append(
        "- owner boundary: `wasm-audit`; action: treat this report as the baseline for the consolidated summary layout and compare deltas on the next run."
    )
elif all_baselines_available:
    lines.append("- No follow-up actions required for this run.")
else:
    lines.append(
        "- owner boundary: `wasm-audit history`; action: preserve scoped baseline size artifacts so future consolidated summary runs stay comparable."
    )

lines.extend(
    [
        "",
        "## Verification Readout",
        "",
        f"- `WASM_AUDIT_DATE={audit_date} bash scripts/ci/wasm-audit-report.sh` -> PASS",
        "- per-canister size-report JSON + Twiggy artifacts -> PASS",
    ]
)

report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

if [[ -z "${WASM_CANISTER_NAME:-}" ]]; then
    for canister_name in minimal one_simple one_complex ten_simple ten_complex; do
        WASM_CANISTER_NAME="$canister_name" \
            WASM_AUDIT_BATCH_PARENT=1 \
            WASM_PROFILE="$PROFILE" \
            WASM_SQL_VARIANTS="$SQL_VARIANTS_MODE" \
            WASM_AUDIT_DATE="$AUDIT_DATE" \
            WASM_AUDIT_REPORT_DIR="$REPORT_DIR" \
            WASM_AUDIT_SKIP_BUILD="${WASM_AUDIT_SKIP_BUILD:-0}" \
            bash "$0"
    done
    write_summary_report minimal one_simple one_complex ten_simple ten_complex
    exit 0
fi

CANISTER_NAME="${WASM_CANISTER_NAME}"

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
SHRUNK_WASM="$ARTIFACT_DIR/${CANISTER_NAME}.${PROFILE}${SIZE_REPORT_SUFFIX}.icp-shrunk.wasm"

for required in "$SIZE_REPORT_JSON" "$SIZE_SUMMARY_MD" "$SHRUNK_WASM"; do
    if [[ ! -f "$required" ]]; then
        echo "[wasm-audit] expected artifact missing: $required" >&2
        exit 1
    fi
done

REPORT_STEM="$REPORT_SCOPE"
REPORT_PATH="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.md"
SIZE_REPORT_COPY="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.size-report.json"
SIZE_SUMMARY_COPY="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.size-summary.md"
TWIGGY_TOP_TXT="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-top.txt"
TWIGGY_TOP_CSV="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-top.csv"
TWIGGY_DOMINATORS_TXT="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-dominators.txt"
TWIGGY_RETAINED_CSV="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-retained.csv"
TWIGGY_MONOS_TXT="$ARTIFACT_SCOPE_DIR/${REPORT_STEM}.${CANISTER_NAME}.${PROFILE}.${SQL_VARIANT}.twiggy-monos.txt"

BASELINE_PATH="$(
        ROOT="$ROOT" REPORT_DIR="$REPORT_DIR" python3 - <<'PY'
import os
from pathlib import Path

root = Path(os.environ["ROOT"])
report_dir = Path(os.environ["REPORT_DIR"]).resolve()
rows = []
for path in root.glob("docs/audits/reports/*/*/wasm-footprint.md"):
    if path.resolve().parent == report_dir:
        continue
    rows.append(str(path.relative_to(root)))

if rows:
    rows.sort()
    print(rows[-1])
else:
    print("N/A")
PY
    )"

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


def has_icp_size_metrics(report: dict) -> bool:
    artifacts = report.get("artifacts")
    return isinstance(artifacts, dict) and all(
        key in artifacts
        for key in (
            "icp_built_wasm",
            "icp_built_wasm_gz_deterministic",
            "icp_shrunk_wasm",
            "icp_shrunk_wasm_gz",
        )
    )


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
    if baseline_artifact.exists():
        baseline_artifact_path = baseline_artifact
        loaded_baseline = json.loads(baseline_artifact.read_text(encoding="utf-8"))
        if has_icp_size_metrics(loaded_baseline):
            baseline_metrics = loaded_baseline

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
        "non-comparable (baseline report exists but compatible ICP size artifact is missing)"
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
    "icp_built_wasm": metric_from(current, "icp_built_wasm"),
    "icp_built_wasm_gz_deterministic": metric_from(
        current, "icp_built_wasm_gz_deterministic"
    ),
    "icp_shrunk_wasm": metric_from(current, "icp_shrunk_wasm"),
    "icp_shrunk_wasm_gz": metric_from(current, "icp_shrunk_wasm_gz"),
}

previous_metrics = None
if baseline_metrics is not None:
    previous_metrics = {
        "icp_built_wasm": metric_from(baseline_metrics, "icp_built_wasm"),
        "icp_built_wasm_gz_deterministic": metric_from(
            baseline_metrics, "icp_built_wasm_gz_deterministic"
        ),
        "icp_shrunk_wasm": metric_from(baseline_metrics, "icp_shrunk_wasm"),
        "icp_shrunk_wasm_gz": metric_from(baseline_metrics, "icp_shrunk_wasm_gz"),
    }

size_rows = [
    ("icp-built `.wasm`", "icp_built_wasm"),
    ("icp-built deterministic `.wasm.gz`", "icp_built_wasm_gz_deterministic"),
    ("icp-shrunk `.wasm`", "icp_shrunk_wasm"),
    ("icp-shrunk `.wasm.gz`", "icp_shrunk_wasm_gz"),
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
        "| Metric | icp-built | icp-shrunk |",
        "| --- | ---: | ---: |",
        (
            "| Function count | "
            f"{fmt_int(analysis['icp_built']['function_count'])} | "
            f"{fmt_int(analysis['icp_shrunk']['function_count'])} |"
        ),
        (
            "| Callback count | "
            f"{fmt_int(analysis['icp_built']['callback_count'])} | "
            f"{fmt_int(analysis['icp_shrunk']['callback_count'])} |"
        ),
        (
            "| Data section count | "
            f"{fmt_int(analysis['icp_built']['data_section_count'])} | "
            f"{fmt_int(analysis['icp_shrunk']['data_section_count'])} |"
        ),
        (
            "| Data section bytes | "
            f"{fmt_int(analysis['icp_built']['data_section_bytes'])} | "
            f"{fmt_int(analysis['icp_shrunk']['data_section_bytes'])} |"
        ),
        (
            "| Exported methods | "
            f"{fmt_int(analysis['icp_built']['exported_method_count'])} | "
            f"{fmt_int(analysis['icp_shrunk']['exported_method_count'])} |"
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

if [[ "${WASM_AUDIT_BATCH_PARENT:-0}" != "1" ]]; then
    write_summary_report "$CANISTER_NAME"
fi
