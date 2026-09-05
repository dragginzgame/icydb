#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
profile="wasm-release"
sql_variant_mode="sql-on"
audit_date="$(date +%F)"
report_dir=""
report_run=""
report_scope_dir=""
canister_names=()
skip_build=0
batch_identity=""
REPORT_SCOPE="wasm-footprint"

# shellcheck source=scripts/ci/wasm-report-common.sh
source "$ROOT/scripts/ci/wasm-report-common.sh"

usage() {
    cat <<'EOF'
usage: wasm-audit-report.sh [--profile debug|release|wasm-release] [--sql-variant sql-on|sql-off] [--date YYYY-MM-DD] [--report-dir path] [--canister name] [--skip-build]

Defaults to wasm-release, sql-on, today's date, and the standard audit canister set.
Default output uses docs/reports/recurring/YYYY/MM/DD/wasm-footprint/<run>/.
Repeat --canister to audit more than one specific canister.
Every invocation writes a new report directory, including --skip-build.
An existing --report-dir is rejected; use a new path for each run.
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

audit_year="${audit_date:0:4}"
audit_month="${audit_date:5:2}"
audit_day="${audit_date:8:2}"
report_scope_dir="$ROOT/docs/reports/recurring/$audit_year/$audit_month/$audit_day/$REPORT_SCOPE"
if [[ -z "$report_dir" ]]; then
    report_run="01"
    while [[ -e "$report_scope_dir/$report_run" ]]; do
        report_run_number=$((10#$report_run + 1))
        printf -v report_run '%02d' "$report_run_number"
    done
    report_dir="$report_scope_dir/$report_run"
fi
artifact_scope_dir="$report_dir/artifacts"

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

# Validate the captured bytes before attribution, and require one recorded
# source/build identity across the batch. Dirty sources remain non-comparable.
validate_capture() {
    local report="$1" wasm="$2" gz="$3" canister="$4"
    local wasm_hash gz_hash identity
    wasm_hash="$(sha256sum "$wasm")"
    gz_hash="$(sha256sum "$gz")"
    if ! jq -e \
        --arg canister "$canister" --arg profile "$profile" --arg sql "$SQL_VARIANT" \
        --arg wasm_hash "${wasm_hash%% *}" --arg gz_hash "${gz_hash%% *}" \
        --argjson wasm_bytes "$(wc -c < "$wasm")" --argjson gz_bytes "$(wc -c < "$gz")" '
        def git_id: type == "string" and test("^[0-9a-f]{40}([0-9a-f]{24})?$");
        def sha256: type == "string" and test("^[0-9a-f]{64}$");
        def nonempty: type == "string" and length > 0;
        .format_version == 1
        and .measurement_profile.identity == "icydb-wasm-footprint/0.251/v1"
        and .canister == $canister and .profile == $profile and .sql_variant == $sql
        and (.provenance.source_revision | git_id)
        and (.provenance.source_tree | git_id)
        and (.provenance.source_dirty | type == "boolean")
        and (.provenance.lockfile_sha256 | sha256)
        and (.provenance.workspace_root | nonempty)
        and (.provenance.cargo_target_dir | nonempty)
        and (.provenance.rust_toolchain | nonempty)
        and .pipeline.build_profile == "production"
        and .pipeline.candid_metadata == "enabled"
        and .pipeline.post_link_transform == "binaryen-132-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1"
        and .pipeline.final_deployable_stage == "binaryen_oz_wasm"
        and .pipeline.path_remapping == "workspace=/w;cargo-registry=/c;rust-library=/r"
        and (.tools.ic_wasm_sha256 | sha256)
        and (.tools.wasm_opt_sha256 | sha256)
        and .artifacts.final_deployable_wasm.sha256 == $wasm_hash
        and .artifacts.final_deployable_wasm.bytes == $wasm_bytes
        and .artifacts.final_deployable_wasm_gz.sha256 == $gz_hash
        and .artifacts.final_deployable_wasm_gz.bytes == $gz_bytes
    ' "$report" >/dev/null; then
        echo "[wasm-audit] captured artifact identity or measurement contract failed: $report" >&2
        exit 1
    fi
    identity="$(jq -cS '{provenance, tools, pipeline}' "$report")"
    if [[ -n "$batch_identity" && "$identity" != "$batch_identity" ]]; then
        echo "[wasm-audit] mixed source/build provenance in batch: $report" >&2
        exit 1
    fi
    batch_identity="$identity"
}

# Selection and summary rendering use the same per-actor comparison contract.
baseline_artifact_matches() {
    local baseline="$1" current="$2"
    [[ -f "$baseline" ]] && jq -e --slurpfile current "$current" '
        .format_version == 1
        and .measurement_profile.identity == "icydb-wasm-footprint/0.251/v1"
        and .provenance.source_dirty == false
        and $current[0].provenance.source_dirty == false
        and .pipeline.final_deployable_stage == "binaryen_oz_wasm"
        and .artifacts.final_deployable_wasm.bytes
        and .artifacts.final_deployable_wasm_gz.bytes
        and .provenance.workspace_root == $current[0].provenance.workspace_root
        and .provenance.cargo_target_dir == $current[0].provenance.cargo_target_dir
        and .provenance.rust_toolchain == $current[0].provenance.rust_toolchain
        and .tools == $current[0].tools
        and .pipeline == $current[0].pipeline
        and .profile == $current[0].profile
        and .sql_variant == $current[0].sql_variant
        and .build.exact_features == $current[0].build.exact_features
    ' "$baseline" >/dev/null
}

select_baseline() {
    local report_dir_abs="$1"
    shift
    local daily="$report_scope_dir/01/report.md"
    local candidate relative candidate_date canister artifact compatible

    # A day's canonical baseline stays pinned even if it is non-comparable.
    if [[ -f "$daily" && "$(cd "${daily%/*}" && pwd)" != "$report_dir_abs" ]]; then
        display_path "$daily"
        return
    fi
    # A reserved/failed run 01 is a missing daily baseline, not permission to
    # substitute an older day's report for a numbered same-day rerun.
    if [[ -n "$report_run" && "$report_run" != "01" ]]; then
        return 0
    fi

    [[ -d "$ROOT/docs/reports/recurring" ]] || return 0
    while IFS= read -r candidate; do
        relative="${candidate#"$ROOT/docs/reports/recurring/"}"
        [[ "$relative" =~ ^([0-9]{4})/([0-9]{2})/([0-9]{2})/wasm-footprint/[0-9]{2}/report\.md$ ]] || continue
        candidate_date="${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]}"
        [[ "$candidate_date" < "$audit_date" ]] || continue
        compatible=1
        for canister in "$@"; do
            artifact="$REPORT_SCOPE.$canister.$profile.$SQL_VARIANT.size-report.json"
            if ! baseline_artifact_matches "${candidate%/*}/artifacts/$artifact" "$artifact_scope_dir/$artifact"; then
                compatible=0
                break
            fi
        done
        if [[ "$compatible" == "1" ]]; then
            display_path "$candidate"
            return
        fi
    done < <(find "$ROOT/docs/reports/recurring" -path '*/wasm-footprint/[0-9][0-9]/report.md' -type f | LC_ALL=C sort -r)
}

write_summary_report() {
    local canisters=("$@")
    local report_path="$report_dir/report.md"
    local report_dir_abs
    local baseline_path
    local snapshot
    local all_baselines_available=1
    local all_current_sources_clean=1
    local rows=()
    local canister_list=""

    report_dir_abs="$(cd "$report_dir" && pwd)"
    baseline_path="$(select_baseline "$report_dir_abs" "${canisters[@]}")"
    baseline_path="${baseline_path:-N/A}"
    snapshot="$(jq -r '.provenance.source_revision' <<< "$batch_identity")"

    for canister_name in "${canisters[@]}"; do
        local size_report_path="$artifact_scope_dir/$REPORT_SCOPE.$canister_name.$profile.$SQL_VARIANT.size-report.json"
        local size_summary_path="$artifact_scope_dir/$REPORT_SCOPE.$canister_name.$profile.$SQL_VARIANT.size-summary.md"
        local baseline_artifact=""
        local status="PARTIAL"
        local previous_final="N/A"
        local current_final
        local previous_gz="N/A"
        local current_gz

        if [[ -z "$canister_list" ]]; then
            canister_list="$canister_name"
        else
            canister_list+=", $canister_name"
        fi

        current_final="$(jq -er '.artifacts.final_deployable_wasm.bytes' "$size_report_path")"
        current_gz="$(jq -er '.artifacts.final_deployable_wasm_gz.bytes' "$size_report_path")"
        if ! jq -e '.provenance.source_dirty == false' "$size_report_path" >/dev/null; then
            all_current_sources_clean=0
            status="PARTIAL"
        fi

        if [[ "$baseline_path" != "N/A" && "$all_current_sources_clean" == "1" ]]; then
            baseline_artifact="$ROOT/${baseline_path%/*}/artifacts/$REPORT_SCOPE.$canister_name.$profile.$SQL_VARIANT.size-report.json"
            if baseline_artifact_matches "$baseline_artifact" "$size_report_path"; then
                previous_final="$(jq -er '.artifacts.final_deployable_wasm.bytes' "$baseline_artifact")"
                previous_gz="$(jq -er '.artifacts.final_deployable_wasm_gz.bytes' "$baseline_artifact")"
                status="PASS"
            else
                all_baselines_available=0
            fi
        else
            all_baselines_available=0
        fi

        rows+=("$canister_name"$'\t'"$status"$'\t'"$previous_final"$'\t'"$current_final"$'\t'"$previous_gz"$'\t'"$current_gz"$'\t'"$(display_path "$size_summary_path")")
    done

    local comparability
    local baseline_status_row
    local pass_counts
    if [[ "$all_current_sources_clean" != "1" ]]; then
        comparability="non-comparable (current artifacts were built from a dirty source tree)"
        baseline_status_row="| Baseline delta availability | PARTIAL | current artifacts record dirty source state and cannot become baseline authority |"
        pass_counts="PASS=4, PARTIAL=1, FAIL=0"
    elif [[ "$baseline_path" == "N/A" ]]; then
        comparability="non-comparable (no eligible baseline)"
        baseline_status_row="| Baseline delta availability | PARTIAL | daily baseline is absent or no earlier comparable report is available |"
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
        printf -- '- source tree: `%s`\n' "$(jq -r '.provenance.source_tree' <<< "$batch_identity")"
        printf -- '- source dirty: `%s`\n' "$(jq -r '.provenance.source_dirty' <<< "$batch_identity")"
        printf -- '- lockfile SHA-256: `%s`\n' "$(jq -r '.provenance.lockfile_sha256' <<< "$batch_identity")"
        printf -- '- method tag/version: `WASM-4.0`\n'
        printf -- '- comparability status: `%s`\n\n' "$comparability"
        printf '## Checklist Results\n\n'
        printf '| Requirement | Status | Evidence |\n'
        printf '| --- | --- | --- |\n'
        printf '| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/` |\n'
        printf '| Twiggy top breakdown generated | PASS | per-canister top text artifacts written |\n'
        printf '| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |\n'
        printf '| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |\n'
        printf '%s\n\n' "$baseline_status_row"
        printf '%s\n\n' "$pass_counts"
        printf '## Per-Canister Size Snapshot\n\n'
        printf '| Canister | Baseline Status | Previous final `.wasm` | Current final `.wasm` | Previous final `.wasm.gz` | Current final `.wasm.gz` | Size Summary |\n'
        printf '| --- | --- | ---: | ---: | ---: | ---: | --- |\n'

        local row canister_name status previous_final current_final previous_gz current_gz size_summary_path
        for row in "${rows[@]}"; do
            IFS=$'\t' read -r canister_name status previous_final current_final previous_gz current_gz size_summary_path <<<"$row"
            printf '| `%s` | %s | %s | %s | %s | %s | `%s` |\n' \
                "$canister_name" "$status" "$previous_final" "$current_final" "$previous_gz" "$current_gz" "$size_summary_path"
        done

        printf '\n## Follow-Up Actions\n\n'
        if [[ "$all_current_sources_clean" != "1" ]]; then
            printf -- '- owner boundary: `wasm-audit provenance`; action: rebuild the complete matrix from one clean source identity before accepting a baseline or delta.\n'
        elif [[ "$baseline_path" == "N/A" ]]; then
            printf -- '- owner boundary: `wasm-audit`; action: preserve this capture; use a completed canonical run 01 for same-day comparisons and record missing baseline evidence explicitly.\n'
        elif [[ "$all_baselines_available" == "1" ]]; then
            printf -- '- No follow-up actions required for this run.\n'
        else
            printf -- '- owner boundary: `wasm-audit history`; action: preserve scoped current-schema baseline size artifacts so future consolidated summary runs stay comparable.\n'
        fi

        printf '\n## Verification Readout\n\n'
        printf -- '- captured Wasm/gzip hashes, byte counts, and batch provenance -> PASS\n'
        printf -- '- reused build artifacts (`--skip-build`): `%s`\n' "$skip_build"
        printf -- '- per-canister size-report JSON + Twiggy artifacts -> PASS\n'
    } > "$report_path"

    echo "[wasm-audit] Wrote summary: $report_path"
}

if ! command -v twiggy >/dev/null 2>&1; then
    echo "[wasm-audit] missing required tool: twiggy" >&2
    echo "[wasm-audit] install with: cargo install twiggy --locked" >&2
    exit 1
fi

# Reserve the run atomically before copying evidence. This also rejects a
# caller-supplied existing directory and concurrent selection of the same run.
mkdir -p -- "$(dirname "$report_dir")"
if ! mkdir -- "$report_dir"; then
    echo "[wasm-audit] cannot reserve a new report directory: $report_dir" >&2
    exit 1
fi
mkdir -- "$artifact_scope_dir"

# Canonical explicit paths carry the same date/run identity as automatic paths.
# Outside that hierarchy, --date supplies the comparison day.
resolved_report_dir="$(cd "$report_dir" && pwd)"
relative_report_dir="${resolved_report_dir#"$ROOT/docs/reports/recurring/"}"
if [[ "$relative_report_dir" =~ ^([0-9]{4})/([0-9]{2})/([0-9]{2})/wasm-footprint/([0-9]{2})$ ]]; then
    audit_date="${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]}"
    report_run="${BASH_REMATCH[4]}"
    report_scope_dir="${resolved_report_dir%/*}"
fi

# Attribution reads these private copies, never mutable shared build outputs.
capture_dir="$(mktemp -d)"
trap 'rm -f -- "$capture_dir/final.wasm" "$capture_dir/final.wasm.gz"; rmdir -- "$capture_dir"' EXIT

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
    local final_wasm="$artifact_dir/${canister_name}.${profile}${SIZE_REPORT_SUFFIX}.final-deployable.wasm"
    local final_gz="$final_wasm.gz"
    local report_stem="$REPORT_SCOPE"
    local size_report_copy="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.size-report.json"
    local size_summary_copy="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.size-summary.md"
    local twiggy_top_txt="$artifact_scope_dir/${report_stem}.${canister_name}.${profile}.${SQL_VARIANT}.twiggy-top.txt"
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

    for required in "$size_report_json" "$size_summary_md" "$final_wasm" "$final_gz"; do
        if [[ ! -f "$required" ]]; then
            echo "[wasm-audit] expected artifact missing: $required" >&2
            exit 1
        fi
    done

    cp "$size_report_json" "$size_report_copy"
    cp "$size_summary_md" "$size_summary_copy"
    cp "$final_wasm" "$capture_dir/final.wasm"
    cp "$final_gz" "$capture_dir/final.wasm.gz"
    validate_capture "$size_report_copy" "$capture_dir/final.wasm" "$capture_dir/final.wasm.gz" "$canister_name"

    write_twiggy_artifact "$twiggy_top_txt" twiggy top -n 40 "$capture_dir/final.wasm"
    write_twiggy_artifact "$twiggy_dominators_txt" twiggy dominators -r 160 "$capture_dir/final.wasm"
    write_twiggy_artifact "$twiggy_retained_csv" twiggy top --retained -n 40 -f csv "$capture_dir/final.wasm"
    write_twiggy_artifact "$twiggy_monos_txt" twiggy monos "$capture_dir/final.wasm"

    echo "[wasm-audit] Wrote artifacts for $canister_name"
}

for canister_name in "${canister_names[@]}"; do
    write_canister_artifacts "$canister_name"
done

write_summary_report "${canister_names[@]}"
