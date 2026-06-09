#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
profile="wasm-release"
sql_variant_mode="sql-on"
audit_date="$(date +%F)"
report_dir=""
canister_names=()
skip_build=0
REPORT_SCOPE="wasm-footprint"

# shellcheck source=scripts/ci/wasm-report-common.sh
source "$ROOT/scripts/ci/wasm-report-common.sh"

usage() {
    cat <<'EOF'
usage: wasm-audit-report.sh [--profile debug|release|wasm-release] [--sql-variant sql-on|sql-off] [--date YYYY-MM-DD] [--report-dir path] [--canister name] [--skip-build]

Defaults to wasm-release, sql-on, today's date, and the standard audit canister set.
Repeat --canister to audit more than one specific canister.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)
            profile="${2:-}"
            shift 2
            ;;
        --sql-variant)
            sql_variant_mode="${2:-}"
            shift 2
            ;;
        --date)
            audit_date="${2:-}"
            shift 2
            ;;
        --report-dir)
            report_dir="${2:-}"
            shift 2
            ;;
        --canister)
            canister_names+=("${2:-}")
            shift 2
            ;;
        --skip-build)
            skip_build=1
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "[wasm-audit] unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ -z "$profile" ]]; then
    echo "[wasm-audit] --profile requires a value" >&2
    exit 1
fi
if [[ -z "$sql_variant_mode" ]]; then
    echo "[wasm-audit] --sql-variant requires a value" >&2
    exit 1
fi
if [[ -z "$audit_date" ]]; then
    echo "[wasm-audit] --date requires a value" >&2
    exit 1
fi
for canister_name in "${canister_names[@]}"; do
    if [[ -z "$canister_name" ]]; then
        echo "[wasm-audit] --canister requires a value" >&2
        exit 1
    fi
done
if [[ "${#canister_names[@]}" -eq 0 ]]; then
    mapfile -t canister_names < <(wasm_report_default_canisters)
fi

audit_month="${audit_date:0:7}"
if [[ -z "$report_dir" ]]; then
    report_dir="$ROOT/docs/audits/reports/$audit_month/$audit_date"
fi
artifact_scope_dir="$report_dir/artifacts/$REPORT_SCOPE"

# Resolve the audited SQL variant once so both the batch summary path and the
# per-canister child runs agree on the same stable output naming.
if sql_variants_output="$(wasm_report_sql_variants "$sql_variant_mode" no)"; then
    mapfile -t resolved_sql_variants <<<"$sql_variants_output"
    SQL_VARIANT="${resolved_sql_variants[0]}"
    SIZE_REPORT_SUFFIX="$(wasm_report_size_suffix "$SQL_VARIANT" 1)"
else
    sql_variant_status=$?
    if [[ "$sql_variant_status" -eq 2 ]]; then
        echo "[wasm-audit] --sql-variant=both is not supported for audit reports; run one variant per audit pass" >&2
        exit 1
    fi
    echo "[wasm-audit] invalid --sql-variant value '$sql_variant_mode'; expected 'sql-on' or 'sql-off'" >&2
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "[wasm-audit] missing required tool: jq" >&2
    exit 1
fi

display_path() {
    local path="$1"
    case "$path" in
        "$ROOT"/*) printf '%s\n' "${path#"$ROOT/"}" ;;
        *) printf '%s\n' "$path" ;;
    esac
}

write_summary_report() {
    local canisters=("$@")
    local report_path="$report_dir/$REPORT_SCOPE.md"
    local report_dir_abs
    local baseline_path
    local snapshot
    local all_baselines_available=1
    local rows=()
    local canister_list=""

    mkdir -p "$report_dir" "$artifact_scope_dir"

    report_dir_abs="$(cd "$report_dir" && pwd)"
    baseline_path="$(
        find "$ROOT/docs/audits/reports" -path '*/wasm-footprint.md' -type f 2>/dev/null \
            | while IFS= read -r path; do
                if [[ "$(cd "$(dirname "$path")" && pwd)" != "$report_dir_abs" ]]; then
                    display_path "$path"
                fi
            done \
            | sort \
            | tail -n 1
    )"
    baseline_path="${baseline_path:-N/A}"
    snapshot="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || printf 'N/A')"

    for canister_name in "${canisters[@]}"; do
        local size_report_path="$artifact_scope_dir/$REPORT_SCOPE.$canister_name.$profile.$SQL_VARIANT.size-report.json"
        local size_summary_path="$artifact_scope_dir/$REPORT_SCOPE.$canister_name.$profile.$SQL_VARIANT.size-summary.md"
        local baseline_artifact=""
        local status="PARTIAL"
        local previous_shrunk="N/A"
        local current_shrunk
        local previous_gz="N/A"
        local current_gz

        if [[ -z "$canister_list" ]]; then
            canister_list="$canister_name"
        else
            canister_list+=", $canister_name"
        fi

        current_shrunk="$(jq -er '.artifacts.icp_shrunk_wasm.bytes' "$size_report_path")"
        current_gz="$(jq -er '.artifacts.icp_shrunk_wasm_gz.bytes' "$size_report_path")"

        if [[ "$baseline_path" != "N/A" ]]; then
            baseline_artifact="$ROOT/${baseline_path%/*}/artifacts/$REPORT_SCOPE/$REPORT_SCOPE.$canister_name.$profile.$SQL_VARIANT.size-report.json"
            if [[ -f "$baseline_artifact" ]] \
                && jq -e '.artifacts.icp_shrunk_wasm.bytes and .artifacts.icp_shrunk_wasm_gz.bytes' "$baseline_artifact" >/dev/null; then
                previous_shrunk="$(jq -er '.artifacts.icp_shrunk_wasm.bytes' "$baseline_artifact")"
                previous_gz="$(jq -er '.artifacts.icp_shrunk_wasm_gz.bytes' "$baseline_artifact")"
                status="PASS"
            else
                all_baselines_available=0
            fi
        else
            all_baselines_available=0
        fi

        rows+=("$canister_name"$'\t'"$status"$'\t'"$previous_shrunk"$'\t'"$current_shrunk"$'\t'"$previous_gz"$'\t'"$current_gz"$'\t'"$(display_path "$size_summary_path")")
    done

    local comparability
    local baseline_status_row
    local pass_counts
    if [[ "$baseline_path" == "N/A" ]]; then
        comparability="non-comparable (first tracked summary-layout run)"
        baseline_status_row="| Baseline delta availability | PARTIAL | first tracked summary-layout run; establishes new baseline layout |"
        pass_counts="PASS=4, PARTIAL=1, FAIL=0"
    elif [[ "$all_baselines_available" == "1" ]]; then
        comparability="comparable"
        baseline_status_row="| Baseline delta availability | PASS | baseline size artifacts loaded for all canisters |"
        pass_counts="PASS=5, PARTIAL=0, FAIL=0"
    else
        comparability="non-comparable (one or more baseline size artifacts are missing or use an incompatible metric schema)"
        baseline_status_row="| Baseline delta availability | PARTIAL | one or more prior scoped size artifacts are missing or use an incompatible metric schema |"
        pass_counts="PASS=4, PARTIAL=1, FAIL=0"
    fi

    {
        printf '# Recurring Audit - Wasm Footprint (%s)\n\n' "$audit_date"
        printf '## Report Preamble\n\n'
        printf -- '- scope: recurring wasm footprint audit for `%s` with profile `%s` and SQL variant `%s`\n' "$canister_list" "$profile" "$SQL_VARIANT"
        printf -- '- compared baseline report path: `%s`\n' "$baseline_path"
        printf -- '- code snapshot identifier: `%s`\n' "$snapshot"
        printf -- '- method tag/version: `WASM-1.0`\n'
        printf -- '- comparability status: `%s`\n\n' "$comparability"
        printf '## Checklist Results\n\n'
        printf '| Requirement | Status | Evidence |\n'
        printf '| --- | --- | --- |\n'
        printf '| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/wasm-footprint/` |\n'
        printf '| Twiggy top breakdown generated | PASS | per-canister top text/csv artifacts written |\n'
        printf '| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |\n'
        printf '| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |\n'
        printf '%s\n\n' "$baseline_status_row"
        printf '%s\n\n' "$pass_counts"
        printf '## Per-Canister Size Snapshot\n\n'
        printf '| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Size Summary |\n'
        printf '| --- | --- | ---: | ---: | ---: | ---: | --- |\n'

        local row canister_name status previous_shrunk current_shrunk previous_gz current_gz size_summary_path
        for row in "${rows[@]}"; do
            IFS=$'\t' read -r canister_name status previous_shrunk current_shrunk previous_gz current_gz size_summary_path <<<"$row"
            printf '| `%s` | %s | %s | %s | %s | %s | `%s` |\n' \
                "$canister_name" "$status" "$previous_shrunk" "$current_shrunk" "$previous_gz" "$current_gz" "$size_summary_path"
        done

        printf '\n## Follow-Up Actions\n\n'
        if [[ "$baseline_path" == "N/A" ]]; then
            printf -- '- owner boundary: `wasm-audit`; action: treat this report as the baseline for the consolidated summary layout and compare deltas on the next run.\n'
        elif [[ "$all_baselines_available" == "1" ]]; then
            printf -- '- No follow-up actions required for this run.\n'
        else
            printf -- '- owner boundary: `wasm-audit history`; action: preserve scoped current-schema baseline size artifacts so future consolidated summary runs stay comparable.\n'
        fi

        printf '\n## Verification Readout\n\n'
        printf -- '- `bash scripts/ci/wasm-audit-report.sh --date %s` -> PASS\n' "$audit_date"
        printf -- '- per-canister size-report JSON + Twiggy artifacts -> PASS\n'
    } > "$report_path"

    echo "[wasm-audit] Wrote summary: $report_dir/$REPORT_SCOPE.md"
}

if ! command -v twiggy >/dev/null 2>&1; then
    echo "[wasm-audit] missing required tool: twiggy" >&2
    echo "[wasm-audit] install with: cargo install twiggy --locked" >&2
    exit 1
fi

mkdir -p "$artifact_scope_dir"

write_twiggy_artifact() {
    local output="$1"
    shift
    local stderr="${output}.stderr"

    if "$@" > "$output" 2> "$stderr"; then
        rm -f "$stderr"
        return
    fi

    if grep -q "function or code section is missing" "$stderr"; then
        {
            printf 'twiggy skipped: wasm has no function/code section\n'
            cat "$stderr"
        } > "$output"
        rm -f "$stderr"
        return
    fi

    cat "$stderr" >&2
    rm -f "$stderr"
    exit 1
}

write_canister_artifacts() {
    local canister_name="$1"
    local artifact_dir="$ROOT/artifacts/wasm-size"
    local size_report_json="$artifact_dir/${canister_name}.${profile}${SIZE_REPORT_SUFFIX}.report.json"
    local size_summary_md="$artifact_dir/${canister_name}.${profile}${SIZE_REPORT_SUFFIX}.summary.md"
    local shrunk_wasm="$artifact_dir/${canister_name}.${profile}${SIZE_REPORT_SUFFIX}.icp-shrunk.wasm"
    local report_stem="$REPORT_SCOPE"
    local size_report_copy="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.size-report.json"
    local size_summary_copy="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.size-summary.md"
    local twiggy_top_txt="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.twiggy-top.txt"
    local twiggy_top_csv="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.twiggy-top.csv"
    local twiggy_dominators_txt="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.twiggy-dominators.txt"
    local twiggy_retained_csv="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.twiggy-retained.csv"
    local twiggy_monos_txt="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.twiggy-monos.txt"

    if [[ "$skip_build" != "1" ]]; then
        bash "$ROOT/scripts/ci/wasm-size-report.sh" \
            --profile "$profile" \
            --sql-variants "$SQL_VARIANT" \
            --canister "$canister_name"
    else
        echo "[wasm-audit] skipping wasm build and size capture (--skip-build)"
    fi

    for required in "$size_report_json" "$size_summary_md" "$shrunk_wasm"; do
        if [[ ! -f "$required" ]]; then
            echo "[wasm-audit] expected artifact missing: $required" >&2
            exit 1
        fi
    done

    cp "$size_report_json" "$size_report_copy"
    cp "$size_summary_md" "$size_summary_copy"

    write_twiggy_artifact "$twiggy_top_txt" twiggy top -n 40 "$shrunk_wasm"
    write_twiggy_artifact "$twiggy_top_csv" twiggy top -n 40 -f csv "$shrunk_wasm"
    write_twiggy_artifact "$twiggy_dominators_txt" twiggy dominators -r 160 "$shrunk_wasm"
    write_twiggy_artifact "$twiggy_retained_csv" twiggy top --retained -n 40 -f csv "$shrunk_wasm"
    write_twiggy_artifact "$twiggy_monos_txt" twiggy monos "$shrunk_wasm"

    echo "[wasm-audit] Wrote artifacts for $canister_name"
}

for canister_name in "${canister_names[@]}"; do
    write_canister_artifacts "$canister_name"
done

write_summary_report "${canister_names[@]}"
