use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, HashSet},
    env,
    fmt::Write as _,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Instant,
};

use candid::CandidType;
use ic_testkit::pic::{
    StandaloneCanisterFixture, try_acquire_pic_serial_guard, try_ensure_pocket_ic_bin, try_pic,
};
use icydb::{
    Error, ErrorOrigin,
    db::{SqlQueryExecutionAttribution, sql::SqlQueryResult},
    diagnostic::{DiagnosticCode, ErrorClass},
};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildTarget, CanisterWasmProfile,
    install_fixture_canister_with_options, install_fixture_canister_with_options_and_progress,
    reset_icydb_fixtures,
};
use serde::{Deserialize, Serialize};

const DEFAULT_MATRIX_LIMIT: usize = 300;
const DEFAULT_RANDOM_CASE_COUNT: usize = 300;
const DEFAULT_TOP_N: usize = 20;
const DEFAULT_RANDOM_SEED: u64 = 0x1cdb_0182_0000_0001;
const SQL_PERF_MATRIX_WASM_PROFILE_ENV: &str = "ICYDB_SQL_PERF_MATRIX_WASM_PROFILE";
const SQL_PERF_MATRIX_INSTALL_PROGRESS_ENV: &str = "ICYDB_SQL_PERF_MATRIX_INSTALL_PROGRESS";
const SQL_PERF_SQLITE_OUTPUT_STEM_ENV: &str = "ICYDB_SQL_PERF_SQLITE_OUTPUT_STEM";
const SQL_PERF_SQLITE_KEYS_ENV: &str = "ICYDB_SQL_PERF_SQLITE_KEYS";
const SQL_PERF_SQLITE_STRICT_ENV: &str = "ICYDB_SQL_PERF_SQLITE_STRICT";
const SQL_PERF_SQLITE3_ENV: &str = "ICYDB_SQLITE3";
const SQL_PERF_SQLITE_TIMING_SAMPLES_ENV: &str = "ICYDB_SQL_PERF_SQLITE_TIMING_SAMPLES";
const DEFAULT_SQL_PERF_SQLITE_TIMING_SAMPLE_COUNT: usize = 0;
const DEFAULT_SQL_PERF_SQLITE_OUTPUT_STEM: &str =
    "/tmp/icydb-sqlite-comparison/sql_perf_audit_sqlite_comparison";
const SQL_PERF_SQLITE_REQUIRED_COMPATIBLE_KEYS: &[&str] = &[
    "user.select.pk.all.pk_asc.limit1",
    "user.select.narrow.age_range.age_asc.limit3",
    "user.select.narrow.lower_name_prefix.lower_name_asc.limit3",
    "user.aggregate.count_active",
    "user.aggregate.group_age_count",
    "account.select.narrow.active_true.handle_asc.limit3",
    "account.select.narrow.tier_gold_active.tier_handle_asc.limit3",
    "blob.select.metadata.bucket_range.bucket_label_asc.limit3",
    "blob.aggregate.count_bucket",
    "token.collection_stage_id.branch_set.page_only.limit50",
    "token.collection_stage_id.prefixed_stage_range.page_only.limit50",
    "token.collection_id.sparse_in.count",
];

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixSurface {
    Account,
    Blob,
    HeapUser,
    JournaledUser,
    Token,
    User,
}

impl MatrixSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Blob => "blob",
            Self::HeapUser => "heap_user",
            Self::JournaledUser => "journaled_user",
            Self::Token => "token",
            Self::User => "user",
        }
    }

    const fn table(self) -> &'static str {
        match self {
            Self::Account => "PerfAuditAccount",
            Self::Blob => "PerfAuditBlob",
            Self::HeapUser => "PerfAuditHeapUser",
            Self::JournaledUser => "PerfAuditJournaledUser",
            Self::Token => "PerfAuditToken",
            Self::User => "PerfAuditUser",
        }
    }

    const fn query_method(self) -> &'static str {
        match self {
            Self::Account => "query_account_with_perf",
            Self::Blob => "query_blob_with_perf",
            Self::HeapUser => "query_heap_user_with_perf",
            Self::JournaledUser => "query_journaled_user_with_perf",
            Self::Token => "query_token_with_perf",
            Self::User => "query_user_with_perf",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixSource {
    Deterministic,
    Random,
}

impl MatrixSource {
    const fn label(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Random => "random",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixMode {
    Deterministic,
    Random,
}

impl MatrixMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Random => "random",
        }
    }

    const fn title(self) -> &'static str {
        match self {
            Self::Deterministic => "SQL Perf Deterministic Matrix",
            Self::Random => "SQL Perf Random Matrix",
        }
    }

    const fn default_report_stem(self) -> &'static str {
        match self {
            Self::Deterministic => "sql_perf_deterministic_matrix",
            Self::Random => "sql_perf_random_matrix",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlFragment {
    key: &'static str,
    sql: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatrixScenario {
    key: String,
    source: MatrixSource,
    surface: MatrixSurface,
    family: String,
    sql: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct MatrixOutcome {
    result_kind: String,
    entity: String,
    row_count: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct MatrixLimitStopAfter {
    possible: bool,
    returned_limit: Option<usize>,
    lookahead: usize,
    stopped_after_matches: Option<u64>,
    stopped_after_index_entries: Option<u64>,
    disabled_reason: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct MatrixSample {
    key: String,
    source: String,
    surface: String,
    family: String,
    sql: String,
    #[serde(default)]
    route_family: String,
    #[serde(default)]
    route_outcome: String,
    #[serde(default)]
    route_reason: Option<String>,
    #[serde(default)]
    order_by_idx_hint: Option<String>,
    #[serde(default)]
    limit_stop_after: MatrixLimitStopAfter,
    #[serde(default)]
    result_signature: Option<String>,
    #[serde(default)]
    cursor_signature: Option<String>,
    compile_local_instructions: u64,
    compile_cache_key_local_instructions: u64,
    compile_cache_lookup_local_instructions: u64,
    compile_parse_local_instructions: u64,
    compile_parse_tokenize_local_instructions: u64,
    compile_parse_select_local_instructions: u64,
    compile_parse_expr_local_instructions: u64,
    compile_parse_predicate_local_instructions: u64,
    compile_aggregate_lane_check_local_instructions: u64,
    compile_prepare_local_instructions: u64,
    compile_lower_local_instructions: u64,
    compile_bind_local_instructions: u64,
    compile_cache_insert_local_instructions: u64,
    execute_local_instructions: u64,
    planner_local_instructions: u64,
    planner_schema_info_local_instructions: u64,
    planner_prepare_local_instructions: u64,
    planner_cache_key_local_instructions: u64,
    planner_cache_lookup_local_instructions: u64,
    planner_plan_build_local_instructions: u64,
    planner_cache_insert_local_instructions: u64,
    store_local_instructions: u64,
    executor_local_instructions: u64,
    grouped_stream_local_instructions: u64,
    grouped_fold_local_instructions: u64,
    grouped_finalize_local_instructions: u64,
    scalar_aggregate_base_row_local_instructions: u64,
    scalar_aggregate_reducer_fold_local_instructions: u64,
    scalar_aggregate_expression_evaluations: u64,
    scalar_aggregate_filter_evaluations: u64,
    scalar_aggregate_rows_ingested: u64,
    scalar_aggregate_terminal_count: u64,
    scalar_aggregate_unique_input_expr_count: u64,
    scalar_aggregate_unique_filter_expr_count: u64,
    scalar_aggregate_sink_mode: Option<String>,
    pure_covering_decode_local_instructions: u64,
    pure_covering_row_assembly_local_instructions: u64,
    hybrid_covering_path_hits: u64,
    hybrid_covering_index_field_accesses: u64,
    hybrid_covering_row_field_accesses: u64,
    direct_data_row_scan_local_instructions: u64,
    direct_data_row_key_stream_local_instructions: u64,
    direct_data_row_row_read_local_instructions: u64,
    direct_data_row_key_encode_local_instructions: u64,
    direct_data_row_store_get_local_instructions: u64,
    direct_data_row_order_window_local_instructions: u64,
    direct_data_row_page_window_local_instructions: u64,
    kernel_row_scan_local_instructions: u64,
    kernel_row_key_stream_local_instructions: u64,
    kernel_row_row_read_local_instructions: u64,
    kernel_row_order_window_local_instructions: u64,
    kernel_row_page_window_local_instructions: u64,
    kernel_row_retained_layout_hits: u64,
    kernel_row_retained_slot_values: u64,
    kernel_row_retained_octet_length_values: u64,
    data_store_get_calls: u64,
    index_store_get_calls: u64,
    index_store_range_scan_calls: u64,
    index_store_entry_reads: u64,
    output_blob_values: u64,
    output_blob_bytes: u64,
    output_blob_hex_bytes: u64,
    sql_compiled_command_hits: u64,
    sql_compiled_command_misses: u64,
    shared_query_plan_hits: u64,
    shared_query_plan_misses: u64,
    total_local_instructions: u64,
    outcome: MatrixOutcome,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MatrixFailure {
    key: String,
    source: String,
    surface: String,
    family: String,
    sql: String,
    #[serde(default = "failed_route_family")]
    route_family: String,
    #[serde(default = "failed_route_outcome")]
    route_outcome: String,
    #[serde(default = "failed_route_reason")]
    route_reason: String,
    code: u16,
    diagnostic_code: u16,
    diagnostic_label: String,
    class: String,
    origin: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MatrixReport {
    matrix_mode: String,
    #[serde(default)]
    canister_wasm_profile: String,
    generated_scenario_count: usize,
    executed_scenario_count: usize,
    failed_scenario_count: usize,
    matrix_limit: usize,
    scenario_key_filter: Option<String>,
    random_seed: Option<u64>,
    random_case_count: usize,
    samples: Vec<MatrixSample>,
    failures: Vec<MatrixFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SqliteAuditComparisonReport {
    sqlite_version: String,
    sqlite_path: String,
    canister_wasm_profile: String,
    generated_scenario_count: usize,
    compared_scenario_count: usize,
    common_success_count: usize,
    icydb_failure_count: usize,
    signature_mismatch_count: usize,
    sample_count: usize,
    scenario_key_filter: Option<String>,
    fairness_notes: Vec<String>,
    scenarios: Vec<SqliteAuditComparisonScenario>,
    failures: Vec<SqliteAuditComparisonFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SqliteAuditComparisonScenario {
    key: String,
    surface: String,
    family: String,
    sql: String,
    route_family: String,
    route_outcome: String,
    route_reason: Option<String>,
    limit_stop_after: MatrixLimitStopAfter,
    icydb_signature: String,
    sqlite_signature: String,
    signatures_match: bool,
    sqlite_explain_query_plan: Vec<String>,
    sqlite_plan_summary: SqlitePlanSummary,
    sqlite_plan_alignment: String,
    icydb_total_local_instructions: u64,
    icydb_execute_local_instructions: u64,
    icydb_data_store_get_calls: u64,
    icydb_index_store_range_scan_calls: u64,
    icydb_index_store_entry_reads: u64,
    sqlite_timing: SqliteTimingSummary,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SqliteAuditComparisonFailure {
    key: String,
    surface: String,
    family: String,
    sql: String,
    status: String,
    icydb_code: u16,
    icydb_diagnostic_code: u16,
    icydb_diagnostic_label: String,
    icydb_class: String,
    icydb_origin: String,
    sqlite_signature: String,
    sqlite_explain_query_plan: Vec<String>,
    sqlite_plan_summary: SqlitePlanSummary,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct SqlitePlanSummary {
    features: BTreeSet<String>,
    index_names: Vec<String>,
}

const SQLITE_PLAN_FEATURE_SCAN: &str = "scan";
const SQLITE_PLAN_FEATURE_SEARCH: &str = "search";
const SQLITE_PLAN_FEATURE_INDEX: &str = "index";
const SQLITE_PLAN_FEATURE_COVERING_INDEX: &str = "covering_index";
const SQLITE_PLAN_FEATURE_INTEGER_PRIMARY_KEY: &str = "integer_primary_key";
const SQLITE_PLAN_FEATURE_TEMP_ORDER: &str = "temp_order";
const SQLITE_PLAN_FEATURE_TEMP_GROUP: &str = "temp_group";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SqliteTimingSummary {
    #[serde(rename = "samples_ns")]
    samples: Vec<u128>,
    #[serde(rename = "median_ns")]
    median: u128,
    #[serde(rename = "min_ns")]
    min: u128,
    #[serde(rename = "max_ns")]
    max: u128,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct MetricDelta {
    before: Option<u64>,
    after: Option<u64>,
    delta: Option<i64>,
    delta_percent_bp: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixDeltaRow {
    key: String,
    before_status: String,
    after_status: String,
    status_class: String,
    total_local_instructions: MetricDelta,
    compile_local_instructions: MetricDelta,
    execute_local_instructions: MetricDelta,
    planner_local_instructions: MetricDelta,
    executor_local_instructions: MetricDelta,
    store_local_instructions: MetricDelta,
    data_store_get_calls: MetricDelta,
    index_store_range_scan_calls: MetricDelta,
    index_store_entry_reads: MetricDelta,
    rows_returned: MetricDelta,
    before_route_family: Option<String>,
    after_route_family: Option<String>,
    before_route_outcome: Option<String>,
    after_route_outcome: Option<String>,
    before_route_reason: Option<String>,
    after_route_reason: Option<String>,
    before_order_by_idx_hint: Option<String>,
    after_order_by_idx_hint: Option<String>,
    before_limit_stop_after: Option<MatrixLimitStopAfter>,
    after_limit_stop_after: Option<MatrixLimitStopAfter>,
    before_result_signature: Option<String>,
    after_result_signature: Option<String>,
    #[serde(flatten)]
    signature_changes: MatrixDeltaSignatureChanges,
    before_cursor_signature: Option<String>,
    after_cursor_signature: Option<String>,
    result_row_count_before: Option<usize>,
    result_row_count_after: Option<usize>,
    #[serde(flatten)]
    target_flags: MatrixDeltaTargetFlags,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixDeltaSignatureChanges {
    result_signature_changed: bool,
    cursor_signature_changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixDeltaTargetFlags {
    focused_target: bool,
    expected_to_improve: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct MatrixDeltaRouteAggregate {
    route_family: Option<String>,
    route_outcome: Option<String>,
    scenario_count: usize,
    total_delta: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixDeltaReport {
    baseline_path: String,
    current_path: String,
    baseline_canister_wasm_profile: String,
    current_canister_wasm_profile: String,
    baseline_scenario_count: usize,
    current_scenario_count: usize,
    union_scenario_count: usize,
    common_successful_scenario_count: usize,
    improved_scenario_count: usize,
    regressed_scenario_count: usize,
    neutral_scenario_count: usize,
    new_failure_count: usize,
    resolved_failure_count: usize,
    common_failure_count: usize,
    focused_target_count: usize,
    expected_improvement_count: usize,
    closeout_failures: Vec<String>,
    route_family_aggregates: Vec<MatrixDeltaRouteAggregate>,
    route_outcome_aggregates: Vec<MatrixDeltaRouteAggregate>,
    route_pair_aggregates: Vec<MatrixDeltaRouteAggregate>,
    rows: Vec<MatrixDeltaRow>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Lcg {
    state: u64,
}

impl Lcg {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    const fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("matrix option count should fit u64");
        usize::try_from(self.next() % len).expect("matrix option index should fit usize")
    }

    fn choose<'a, T>(&mut self, values: &'a [T]) -> &'a T {
        &values[self.index(values.len())]
    }
}

fn deterministic_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();
    let mut user_scenarios = select_matrix(
        MatrixSurface::User,
        &user_projections(),
        &user_predicates(),
        &user_orders(),
        &[1, 3, 10],
    );
    if !user_scenarios.is_empty() {
        scenarios.extend(user_scenarios.drain(..1));
    }
    scenarios.extend(token_branch_route_hotspot_matrix());
    scenarios.extend(user_scenarios);
    scenarios.extend(select_matrix(
        MatrixSurface::Account,
        &account_projections(),
        &account_predicates(),
        &account_orders(),
        &[1, 3, 10],
    ));
    scenarios.extend(select_matrix(
        MatrixSurface::Blob,
        &blob_projections(),
        &blob_predicates(),
        &blob_orders(),
        &[1, 3, 10],
    ));
    scenarios.extend(storage_backend_mirror_matrix());
    scenarios.extend(aggregate_and_metadata_matrix());

    scenarios
}

const TOKEN_TARGET_COLLECTION: &str = "01KV5N439P0000000000000000";
const TOKEN_BRANCH_STAGES: &str = "'Draft', 'Review'";
const TOKEN_BRANCH_STAGES_WITH_DUPLICATE: &str = "'Draft', 'Draft', 'Review'";
const TOKEN_BRANCH_STAGES_WIDE: &str =
    "'Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden'";
const TOKEN_BRANCH_STAGES_OVER_CAP: &str = "'Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07'";
const TOKEN_BRANCH_STAGES_OVER_CAP_EXCLUSIONS: &str = "'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07'";

fn token_branch_route_hotspot_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = vec![
        scenario(
            "token.collection_stage_id.branch_set.page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.covering_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.covering_page_only",
            token_branch_page_sql("id, collection_id, stage", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.full_entity.limit50",
            MatrixSurface::Token,
            "route.branch_set.full_entity",
            token_branch_page_sql("*", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.index_residual_covering.limit3",
            MatrixSurface::Token,
            "route.branch_set.index_residual_covering",
            token_branch_page_sql_with_extra_predicate(
                "id, stage",
                TOKEN_BRANCH_STAGES,
                "stage != 'Review'",
                3,
            ),
        ),
        scenario(
            "token.collection_stage_id.prefixed_stage_range.page_only.limit50",
            MatrixSurface::Token,
            "route.prefixed_range.page_only",
            token_prefixed_stage_range_page_sql("id", 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.count",
            MatrixSurface::Token,
            "route.branch_set.count",
            token_branch_count_sql(TOKEN_BRANCH_STAGES),
        ),
        scenario(
            "token.collection_stage_id.branch_set.duplicate_count",
            MatrixSurface::Token,
            "route.branch_set.duplicate_count",
            token_branch_count_sql(TOKEN_BRANCH_STAGES_WITH_DUPLICATE),
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.wide_page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES_WIDE, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.wide_noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES_WIDE, 50),
        ),
    ];
    scenarios.extend(token_branch_over_cap_hotspot_matrix());
    scenarios.extend([
        scenario(
            "token.collection_id.sparse_in.page_only.limit50",
            MatrixSurface::Token,
            "route.sparse_in.page_only",
            token_sparse_collection_in_page_sql(250, 50),
        ),
        scenario(
            "token.collection_id.sparse_in.count",
            MatrixSurface::Token,
            "route.sparse_in.count",
            token_sparse_collection_in_count_sql(250),
        ),
    ]);
    scenarios
}

fn token_branch_over_cap_hotspot_matrix() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "token.collection_stage_id.overcap_fallback.page_only.limit50",
            MatrixSurface::Token,
            "route.branch_over_cap.page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES_OVER_CAP, 50),
        ),
        scenario(
            "token.collection_stage_id.overcap_pruned.page_only.limit50",
            MatrixSurface::Token,
            "route.branch_over_cap_pruned.page_only",
            token_branch_page_sql_with_extra_predicate(
                "id",
                TOKEN_BRANCH_STAGES_OVER_CAP,
                &format!("stage NOT IN ({TOKEN_BRANCH_STAGES_OVER_CAP_EXCLUSIONS})"),
                50,
            ),
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_over_cap.noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES_OVER_CAP, 50),
        ),
    ]
}

fn token_branch_page_sql(projection: &str, stages: &str, limit: u32) -> String {
    format!(
        "SELECT {projection} FROM PerfAuditToken WHERE collection_id = '{TOKEN_TARGET_COLLECTION}' AND stage IN ({stages}) ORDER BY id ASC LIMIT {limit}"
    )
}

fn token_branch_page_sql_with_extra_predicate(
    projection: &str,
    stages: &str,
    extra_predicate: &str,
    limit: u32,
) -> String {
    format!(
        "SELECT {projection} FROM PerfAuditToken WHERE collection_id = '{TOKEN_TARGET_COLLECTION}' AND stage IN ({stages}) AND {extra_predicate} ORDER BY id ASC LIMIT {limit}"
    )
}

fn token_branch_count_sql(stages: &str) -> String {
    format!(
        "SELECT COUNT(*) FROM PerfAuditToken WHERE collection_id = '{TOKEN_TARGET_COLLECTION}' AND stage IN ({stages})"
    )
}

fn token_prefixed_stage_range_page_sql(projection: &str, limit: u32) -> String {
    format!(
        "SELECT {projection} FROM PerfAuditToken WHERE collection_id = '{TOKEN_TARGET_COLLECTION}' AND stage >= 'Draft' AND stage < 'Review' ORDER BY stage ASC, id ASC LIMIT {limit}"
    )
}

fn token_sparse_collection_in_filter(missing_count: usize) -> String {
    let mut collections = format!("'{TOKEN_TARGET_COLLECTION}'");
    for index in 0..missing_count {
        let _ = write!(collections, ", 'missing-collection-{index:03}'");
    }

    format!("collection_id IN ({collections})")
}

fn token_sparse_collection_in_page_sql(missing_count: usize, limit: u32) -> String {
    let filter = token_sparse_collection_in_filter(missing_count);

    format!("SELECT id FROM PerfAuditToken WHERE {filter} ORDER BY id ASC LIMIT {limit}")
}

fn token_sparse_collection_in_count_sql(missing_count: usize) -> String {
    let filter = token_sparse_collection_in_filter(missing_count);

    format!("SELECT COUNT(*) FROM PerfAuditToken WHERE {filter}")
}

fn select_matrix(
    surface: MatrixSurface,
    projections: &[SqlFragment],
    predicates: &[SqlFragment],
    orders: &[SqlFragment],
    limits: &[u32],
) -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();

    for projection in projections {
        for predicate in predicates {
            for order in orders {
                for limit in limits {
                    let key = format!(
                        "{}.select.{}.{}.{}.limit{}",
                        surface.label(),
                        projection.key,
                        predicate.key,
                        order.key,
                        limit
                    );
                    let family =
                        format!("select.{}.{}.{}", projection.key, predicate.key, order.key);
                    let sql = select_sql(
                        surface.table(),
                        projection.sql,
                        predicate.sql,
                        order.sql,
                        *limit,
                    );

                    scenarios.push(MatrixScenario {
                        key,
                        source: MatrixSource::Deterministic,
                        surface,
                        family,
                        sql,
                    });
                }
            }
        }
    }

    scenarios
}

fn select_sql(table: &str, projection: &str, predicate: &str, order: &str, limit: u32) -> String {
    let where_clause = if predicate.is_empty() {
        String::new()
    } else {
        format!(" WHERE {predicate}")
    };
    let order_clause = if order.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {order}")
    };

    format!("SELECT {projection} FROM {table}{where_clause}{order_clause} LIMIT {limit}")
}

fn user_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, name",
        },
        SqlFragment {
            key: "wide",
            sql: "id, name, age, age_nat, rank, active",
        },
        SqlFragment {
            key: "numeric_expr",
            sql: "id, age + rank AS total",
        },
        SqlFragment {
            key: "text_expr",
            sql: "id, LOWER(name) AS lower_name",
        },
    ]
}

fn user_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "pk_range",
            sql: "id >= 2",
        },
        SqlFragment {
            key: "age_range",
            sql: "age >= 24 AND age < 40",
        },
        SqlFragment {
            key: "name_prefix",
            sql: "name LIKE 'A%'",
        },
        SqlFragment {
            key: "lower_name_prefix",
            sql: "LOWER(name) LIKE 'a%'",
        },
        SqlFragment {
            key: "active_true",
            sql: "active = true",
        },
        SqlFragment {
            key: "age_in",
            sql: "age IN (24, 31, 43)",
        },
        SqlFragment {
            key: "field_compare",
            sql: "age > rank",
        },
    ]
}

fn user_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "pk_desc",
            sql: "id DESC",
        },
        SqlFragment {
            key: "age_asc",
            sql: "age ASC, id ASC",
        },
        SqlFragment {
            key: "age_desc",
            sql: "age DESC, id DESC",
        },
        SqlFragment {
            key: "name_asc",
            sql: "name ASC, id ASC",
        },
        SqlFragment {
            key: "lower_name_asc",
            sql: "LOWER(name) ASC, id ASC",
        },
        SqlFragment {
            key: "numeric_expr_asc",
            sql: "age + rank ASC, id ASC",
        },
    ]
}

fn account_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, handle",
        },
        SqlFragment {
            key: "wide",
            sql: "id, handle, tier, active, score",
        },
        SqlFragment {
            key: "text_expr",
            sql: "id, LOWER(handle) AS lower_handle",
        },
    ]
}

fn account_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "active_true",
            sql: "active = true",
        },
        SqlFragment {
            key: "tier_gold_active",
            sql: "tier = 'gold' AND active = true",
        },
        SqlFragment {
            key: "handle_prefix_active",
            sql: "handle LIKE 'a%' AND active = true",
        },
        SqlFragment {
            key: "lower_handle_prefix_active",
            sql: "LOWER(handle) LIKE 'a%' AND active = true",
        },
        SqlFragment {
            key: "score_range",
            sql: "score >= 20",
        },
    ]
}

fn account_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "handle_asc",
            sql: "handle ASC, id ASC",
        },
        SqlFragment {
            key: "handle_desc",
            sql: "handle DESC, id DESC",
        },
        SqlFragment {
            key: "lower_handle_asc",
            sql: "LOWER(handle) ASC, id ASC",
        },
        SqlFragment {
            key: "tier_handle_asc",
            sql: "tier ASC, handle ASC, id ASC",
        },
    ]
}

fn blob_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "metadata",
            sql: "id, label, bucket",
        },
        SqlFragment {
            key: "lengths",
            sql: "id, label, OCTET_LENGTH(thumbnail), OCTET_LENGTH(chunk)",
        },
        SqlFragment {
            key: "thumbnail",
            sql: "id, label, thumbnail",
        },
        SqlFragment {
            key: "payload",
            sql: "id, label, thumbnail, chunk",
        },
    ]
}

fn blob_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "bucket_eq",
            sql: "bucket = 10",
        },
        SqlFragment {
            key: "bucket_range",
            sql: "bucket >= 10 AND bucket < 40",
        },
        SqlFragment {
            key: "label_prefix",
            sql: "label LIKE 'blob-%'",
        },
    ]
}

fn blob_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "bucket_asc",
            sql: "bucket ASC, id ASC",
        },
        SqlFragment {
            key: "bucket_label_asc",
            sql: "bucket ASC, label ASC, id ASC",
        },
        SqlFragment {
            key: "label_asc",
            sql: "label ASC, id ASC",
        },
    ]
}

fn storage_backend_mirror_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();
    for surface in [MatrixSurface::HeapUser, MatrixSurface::JournaledUser] {
        scenarios.extend(select_matrix(
            surface,
            &storage_mirror_projections(),
            &storage_mirror_predicates(),
            &storage_mirror_orders(),
            &[1, 3, 10],
        ));
    }
    scenarios
}

fn storage_mirror_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, name",
        },
        SqlFragment {
            key: "wide",
            sql: "id, name, age",
        },
    ]
}

fn storage_mirror_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "pk_range",
            sql: "id >= 2",
        },
        SqlFragment {
            key: "age_range",
            sql: "age >= 24 AND age < 40",
        },
        SqlFragment {
            key: "name_range",
            sql: "name >= 'a'",
        },
    ]
}

fn storage_mirror_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "pk_desc",
            sql: "id DESC",
        },
        SqlFragment {
            key: "age_asc",
            sql: "age ASC, id ASC",
        },
        SqlFragment {
            key: "name_asc",
            sql: "name ASC, id ASC",
        },
    ]
}

fn aggregate_and_metadata_matrix() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "user.aggregate.count_all",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        scenario(
            "user.aggregate.count_active",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE active = true",
        ),
        scenario(
            "user.aggregate.count_age_in",
            MatrixSurface::User,
            "aggregate.count_in",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE age IN (24, 31, 43)",
        ),
        scenario(
            "user.aggregate.group_age_count",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
        ),
        scenario(
            "user.aggregate.group_active_avg_age",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT active, AVG(age) FROM PerfAuditUser GROUP BY active ORDER BY active ASC LIMIT 10",
        ),
        scenario(
            "user.aggregate.group_age_having_alias",
            MatrixSurface::User,
            "aggregate.grouped_having",
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
        ),
        scenario(
            "account.aggregate.group_tier_count",
            MatrixSurface::Account,
            "aggregate.grouped",
            "SELECT tier, COUNT(*) FROM PerfAuditAccount WHERE active = true GROUP BY tier ORDER BY tier ASC LIMIT 10",
        ),
        scenario(
            "account.aggregate.count_active_tier_in",
            MatrixSurface::Account,
            "aggregate.count_in",
            "SELECT COUNT(*) FROM PerfAuditAccount WHERE active = true AND tier IN ('gold', 'silver')",
        ),
        scenario(
            "blob.aggregate.count_bucket",
            MatrixSurface::Blob,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditBlob WHERE bucket = 10",
        ),
        scenario(
            "user.metadata.explain_pk_limit",
            MatrixSurface::User,
            "metadata.explain",
            "EXPLAIN SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        ),
        scenario(
            "user.metadata.describe",
            MatrixSurface::User,
            "metadata.describe",
            "DESCRIBE PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_columns",
            MatrixSurface::User,
            "metadata.show_columns",
            "SHOW COLUMNS PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_indexes",
            MatrixSurface::User,
            "metadata.show_indexes",
            "SHOW INDEXES FROM PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_entities",
            MatrixSurface::User,
            "metadata.show_entities",
            "SHOW ENTITIES",
        ),
    ]
}

fn scenario(
    key: impl Into<String>,
    surface: MatrixSurface,
    family: impl Into<String>,
    sql: impl Into<String>,
) -> MatrixScenario {
    MatrixScenario {
        key: key.into(),
        source: MatrixSource::Deterministic,
        surface,
        family: family.into(),
        sql: sql.into(),
    }
}

fn random_matrix(seed: u64, case_count: usize) -> Vec<MatrixScenario> {
    let mut rng = Lcg::new(seed);
    (0..case_count)
        .map(|index| random_scenario(&mut rng, seed, index))
        .collect()
}

fn random_scenario(rng: &mut Lcg, seed: u64, index: usize) -> MatrixScenario {
    let surface = *rng.choose(&[
        MatrixSurface::User,
        MatrixSurface::Account,
        MatrixSurface::Blob,
        MatrixSurface::Token,
    ]);
    let key = format!("random.{seed:016x}.{index:04}.{}", surface.label());

    match surface {
        MatrixSurface::Account => {
            let predicate = random_account_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &account_projections(),
                predicate,
                &account_orders(),
            )
        }
        MatrixSurface::Blob => {
            let predicate = random_blob_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &blob_projections(),
                predicate,
                &blob_orders(),
            )
        }
        MatrixSurface::HeapUser | MatrixSurface::JournaledUser => {
            let predicate = random_storage_mirror_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &storage_mirror_projections(),
                predicate,
                &storage_mirror_orders(),
            )
        }
        MatrixSurface::Token => random_token_route_hotspot_scenario(rng, key),
        MatrixSurface::User => {
            let predicate = random_user_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &user_projections(),
                predicate,
                &user_orders(),
            )
        }
    }
}

fn random_token_route_hotspot_scenario(rng: &mut Lcg, key: String) -> MatrixScenario {
    let token_scenarios = token_branch_route_hotspot_matrix();
    let mut scenario = rng.choose(&token_scenarios).clone();
    scenario.key = key;
    scenario.source = MatrixSource::Random;
    scenario.family = format!("random.{}", scenario.family);
    scenario
}

fn random_select_scenario(
    rng: &mut Lcg,
    key: String,
    surface: MatrixSurface,
    projections: &[SqlFragment],
    predicate: String,
    orders: &[SqlFragment],
) -> MatrixScenario {
    let projection = rng.choose(projections);
    let order = rng.choose(orders);
    let limit = *rng.choose(&[1, 2, 3, 5, 10]);
    let sql = select_sql(
        surface.table(),
        projection.sql,
        predicate.as_str(),
        order.sql,
        limit,
    );

    MatrixScenario {
        key,
        source: MatrixSource::Random,
        surface,
        family: format!("random.{}.{}", projection.key, order.key),
        sql,
    }
}

fn random_storage_mirror_predicate(rng: &mut Lcg) -> String {
    match rng.index(4) {
        0 => String::new(),
        1 => format!("id >= {}", rng.choose(&[1, 2, 3, 4])),
        2 => {
            let low = *rng.choose(&[18, 24, 30, 35]);
            let high = low + *rng.choose(&[5, 10, 20]);
            format!("age >= {low} AND age < {high}")
        }
        _ => "name >= 'a'".to_string(),
    }
}

fn random_user_predicate(rng: &mut Lcg) -> String {
    match rng.index(8) {
        0 => String::new(),
        1 => format!("id >= {}", rng.choose(&[1, 2, 3, 4])),
        2 => {
            let low = *rng.choose(&[18, 24, 30, 35]);
            let high = low + *rng.choose(&[5, 10, 20]);
            format!("age >= {low} AND age < {high}")
        }
        3 => format!("name LIKE '{}%'", rng.choose(&["A", "B", "C", "D"])),
        4 => format!("LOWER(name) LIKE '{}%'", rng.choose(&["a", "b", "c", "d"])),
        5 => format!("active = {}", rng.choose(&["true", "false"])),
        6 => format!(
            "age IN ({}, {}, {})",
            rng.choose(&[18, 24, 30]),
            rng.choose(&[31, 35, 40]),
            rng.choose(&[43, 45, 50])
        ),
        _ => "age > rank".to_string(),
    }
}

fn random_account_predicate(rng: &mut Lcg) -> String {
    match rng.index(6) {
        0 => String::new(),
        1 => "active = true".to_string(),
        2 => format!(
            "tier = '{}' AND active = true",
            rng.choose(&["free", "gold", "pro"])
        ),
        3 => format!(
            "handle LIKE '{}%' AND active = true",
            rng.choose(&["a", "b", "c"])
        ),
        4 => format!(
            "LOWER(handle) LIKE '{}%' AND active = true",
            rng.choose(&["a", "b", "c"])
        ),
        _ => format!("score >= {}", rng.choose(&[10, 20, 30, 40])),
    }
}

fn random_blob_predicate(rng: &mut Lcg) -> String {
    match rng.index(4) {
        0 => String::new(),
        1 => format!("bucket = {}", rng.choose(&[10, 20, 30, 40])),
        2 => {
            let low = *rng.choose(&[10, 20, 30]);
            let high = low + *rng.choose(&[10, 20]);
            format!("bucket >= {low} AND bucket < {high}")
        }
        _ => "label LIKE 'blob-%'".to_string(),
    }
}

fn generated_matrix(mode: MatrixMode) -> Vec<MatrixScenario> {
    match mode {
        MatrixMode::Deterministic => deterministic_matrix(),
        MatrixMode::Random => random_matrix(random_seed(), random_case_count()),
    }
}

fn filter_matrix_scenarios(
    scenarios: Vec<MatrixScenario>,
    scenario_key_filter: Option<&str>,
) -> Vec<MatrixScenario> {
    let Some(filter) = scenario_key_filter else {
        return scenarios;
    };
    let requested_keys = filter
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .collect::<Vec<_>>();
    assert!(
        !requested_keys.is_empty(),
        "ICYDB_SQL_PERF_MATRIX_KEYS should contain one or more comma-separated scenario keys",
    );

    let requested = requested_keys.iter().copied().collect::<HashSet<_>>();
    let mut found = HashSet::new();
    let selected = scenarios
        .into_iter()
        .filter(|scenario| {
            let keep = requested.contains(scenario.key.as_str());
            if keep {
                found.insert(scenario.key.clone());
            }
            keep
        })
        .collect::<Vec<_>>();
    let missing = requested_keys
        .into_iter()
        .filter(|key| !found.contains(*key))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "ICYDB_SQL_PERF_MATRIX_KEYS contained unknown scenario key(s): {}",
        missing.join(", "),
    );

    selected
}

fn matrix_limit(total: usize) -> usize {
    match env::var("ICYDB_SQL_PERF_MATRIX_LIMIT") {
        Ok(value) if value == "all" => total,
        Ok(value) => value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_LIMIT should be a positive integer or 'all'")
            .min(total),
        Err(_) => DEFAULT_MATRIX_LIMIT.min(total),
    }
}

fn matrix_mode() -> MatrixMode {
    if let Ok(value) = env::var("ICYDB_SQL_PERF_MATRIX_MODE") {
        return parse_matrix_mode(&value);
    }

    assert!(
        env::var_os("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES").is_none()
            && env::var_os("ICYDB_SQL_PERF_MATRIX_SEED").is_none(),
        "set ICYDB_SQL_PERF_MATRIX_MODE=random before using random matrix controls"
    );
    MatrixMode::Deterministic
}

fn parse_matrix_mode(value: &str) -> MatrixMode {
    match value {
        "deterministic" => MatrixMode::Deterministic,
        "random" => MatrixMode::Random,
        other => panic!(
            "ICYDB_SQL_PERF_MATRIX_MODE should be 'deterministic' or 'random', got '{other}'"
        ),
    }
}

fn random_case_count() -> usize {
    env::var("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES").map_or(DEFAULT_RANDOM_CASE_COUNT, |value| {
        value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES should be a positive integer")
    })
}

fn random_seed() -> u64 {
    env::var("ICYDB_SQL_PERF_MATRIX_SEED").map_or(DEFAULT_RANDOM_SEED, |value| {
        value
            .parse::<u64>()
            .expect("ICYDB_SQL_PERF_MATRIX_SEED should be an unsigned integer")
    })
}

fn top_n() -> usize {
    env::var("ICYDB_SQL_PERF_MATRIX_TOP").map_or(DEFAULT_TOP_N, |value| {
        value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_TOP should be a positive integer")
    })
}

fn matrix_scenario_key_filter() -> Option<String> {
    env::var("ICYDB_SQL_PERF_MATRIX_KEYS")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn matrix_canister_wasm_profile() -> CanisterWasmProfile {
    env::var(SQL_PERF_MATRIX_WASM_PROFILE_ENV).map_or(CanisterWasmProfile::Debug, |value| {
        CanisterWasmProfile::parse(&value).unwrap_or_else(|err| panic!("{err}"))
    })
}

fn matrix_canister_build_options() -> CanisterBuildOptions {
    CanisterBuildOptions {
        profile: matrix_canister_wasm_profile(),
        build_target: CanisterBuildTarget::Local,
        ..CanisterBuildOptions::default()
    }
}

fn sqlite_audit_comparison_scenarios() -> Vec<MatrixScenario> {
    let scenarios = deterministic_matrix();
    let Some(requested_keys) = sqlite_audit_comparison_keys() else {
        return scenarios
            .into_iter()
            .filter(sqlite_audit_scenario_is_compatible)
            .collect();
    };

    sqlite_audit_comparison_scenarios_for_keys(scenarios, &requested_keys)
}

fn sqlite_audit_comparison_scenarios_for_keys(
    scenarios: Vec<MatrixScenario>,
    requested_keys: &[String],
) -> Vec<MatrixScenario> {
    let generated = scenarios.len();
    let requested = requested_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut found = BTreeSet::new();
    let selected = scenarios
        .into_iter()
        .filter(|scenario| {
            let keep = requested.contains(scenario.key.as_str());
            if keep {
                found.insert(scenario.key.clone());
            }
            keep
        })
        .collect::<Vec<_>>();
    let missing = requested_keys
        .iter()
        .filter(|key| !found.contains(key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "{SQL_PERF_SQLITE_KEYS_ENV} contained unknown scenario key(s) for {generated} generated scenarios: {}",
        missing.join(", "),
    );

    selected
}

fn sqlite_audit_comparison_keys() -> Option<Vec<String>> {
    env::var(SQL_PERF_SQLITE_KEYS_ENV).ok().map(|value| {
        let keys = value
            .split(',')
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert!(
            !keys.is_empty(),
            "{SQL_PERF_SQLITE_KEYS_ENV} should contain one or more comma-separated scenario keys"
        );
        keys
    })
}

fn sqlite_audit_scenario_is_compatible(scenario: &MatrixScenario) -> bool {
    if !scenario.sql.starts_with("SELECT") {
        return false;
    }

    match scenario.surface {
        MatrixSurface::HeapUser | MatrixSurface::JournaledUser => false,
        MatrixSurface::Token => !scenario.key.contains(".full_entity."),
        MatrixSurface::Blob => {
            scenario.key.contains(".select.pk.")
                || scenario.key.contains(".select.metadata.")
                || scenario.key == "blob.aggregate.count_bucket"
        }
        MatrixSurface::Account => {
            !scenario.key.contains(".select.wide.")
                && !scenario.key.contains(".metadata.")
                && !scenario.sql.contains("OCTET_LENGTH(")
        }
        MatrixSurface::User => {
            !scenario.key.contains(".select.wide.")
                && scenario.key != "user.aggregate.group_active_avg_age"
                && !scenario.key.contains(".metadata.")
        }
    }
}

fn sqlite_timing_sample_count() -> usize {
    env::var(SQL_PERF_SQLITE_TIMING_SAMPLES_ENV).map_or(
        DEFAULT_SQL_PERF_SQLITE_TIMING_SAMPLE_COUNT,
        |value| {
            value
                .parse::<usize>()
                .expect("ICYDB_SQL_PERF_SQLITE_TIMING_SAMPLES should be a non-negative integer")
        },
    )
}

fn sqlite_strict_enabled() -> bool {
    env::var(SQL_PERF_SQLITE_STRICT_ENV)
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn sqlite3_path() -> PathBuf {
    env::var(SQL_PERF_SQLITE3_ENV).map_or_else(|_| PathBuf::from("sqlite3"), PathBuf::from)
}

fn sqlite_version(sqlite_path: &Path) -> Result<String, String> {
    let output = Command::new(sqlite_path)
        .arg("--version")
        .output()
        .map_err(|err| format!("failed to run `{}`: {err}", sqlite_path.display()))?;

    if !output.status.success() {
        return Err(format!(
            "`{}` --version failed with status {:?}: {}",
            sqlite_path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn sqlite_audit_db_path() -> PathBuf {
    env::temp_dir()
        .join("icydb-sqlite-comparison")
        .join(format!("sql-perf-audit-{}.sqlite3", std::process::id()))
}

fn reset_sqlite_audit_database(db_path: &Path) {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|err| {
            panic!(
                "failed to create SQLite audit comparison directory `{}`: {err}",
                parent.display()
            )
        });
    }

    for path in [
        db_path.to_path_buf(),
        db_path.with_extension("sqlite3-wal"),
        db_path.with_extension("sqlite3-shm"),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => panic!(
                "failed to remove stale SQLite audit comparison file `{}`: {err}",
                path.display()
            ),
        }
    }
}

fn setup_sqlite_audit_database(sqlite_path: &Path, db_path: &Path) -> Result<(), String> {
    reset_sqlite_audit_database(db_path);
    sqlite_output(
        sqlite_path,
        db_path,
        sqlite_audit_schema_and_seed().as_str(),
    )
    .map(|_| ())
}

fn sqlite_audit_schema_and_seed() -> String {
    let mut script = String::new();
    script.push_str(
        "PRAGMA journal_mode=WAL;\n\
         PRAGMA synchronous=NORMAL;\n\
         PRAGMA case_sensitive_like=ON;\n\
         CREATE TABLE PerfAuditUser (\n\
           id INTEGER PRIMARY KEY,\n\
           name TEXT NOT NULL,\n\
           age INTEGER NOT NULL,\n\
           age_nat INTEGER NOT NULL,\n\
           rank INTEGER NOT NULL,\n\
           active INTEGER NOT NULL CHECK(active IN (0, 1))\n\
         ) STRICT;\n\
         CREATE INDEX perf_audit_user_name ON PerfAuditUser(name);\n\
         CREATE INDEX perf_audit_user_age_id ON PerfAuditUser(age, id);\n\
         CREATE INDEX perf_audit_user_lower_name ON PerfAuditUser(LOWER(name));\n\
         CREATE TABLE PerfAuditAccount (\n\
           id INTEGER PRIMARY KEY,\n\
           handle TEXT NOT NULL,\n\
           tier TEXT NOT NULL,\n\
           active INTEGER NOT NULL CHECK(active IN (0, 1)),\n\
           score INTEGER NOT NULL\n\
         ) STRICT;\n\
         CREATE INDEX perf_audit_account_handle_active ON PerfAuditAccount(handle) WHERE active = 1;\n\
         CREATE INDEX perf_audit_account_lower_handle_active ON PerfAuditAccount(LOWER(handle)) WHERE active = 1;\n\
         CREATE INDEX perf_audit_account_tier_handle_active ON PerfAuditAccount(tier, handle) WHERE active = 1;\n\
         CREATE INDEX perf_audit_account_tier_lower_handle_active ON PerfAuditAccount(tier, LOWER(handle)) WHERE active = 1;\n\
         CREATE TABLE PerfAuditBlob (\n\
           id INTEGER PRIMARY KEY,\n\
           label TEXT NOT NULL,\n\
           bucket INTEGER NOT NULL,\n\
           thumbnail BLOB NOT NULL,\n\
           chunk BLOB NOT NULL\n\
         ) STRICT;\n\
         CREATE INDEX perf_audit_blob_bucket_label_id ON PerfAuditBlob(bucket, label, id);\n\
         CREATE INDEX perf_audit_blob_label ON PerfAuditBlob(label);\n\
         CREATE TABLE PerfAuditToken (\n\
           id TEXT PRIMARY KEY,\n\
           collection_id TEXT NOT NULL,\n\
           stage TEXT NOT NULL,\n\
           title TEXT NOT NULL\n\
         ) STRICT;\n\
         CREATE INDEX perf_audit_token_collection_stage_id ON PerfAuditToken(collection_id, stage, id);\n",
    );

    append_sqlite_perf_audit_user_rows(&mut script);
    append_sqlite_perf_audit_account_rows(&mut script);
    append_sqlite_perf_audit_blob_rows(&mut script);
    append_sqlite_perf_audit_token_rows(&mut script);
    script
}

fn append_sqlite_perf_audit_user_rows(script: &mut String) {
    for (id, name, age, age_nat, rank, active) in [
        (1, "Alice", 31, 31, 28, true),
        (2, "bob", 24, 24, 25, true),
        (3, "Charlie", 43, 43, 43, false),
        (4, "amber", 27, 26, 29, true),
        (5, "Andrew", 31, 30, 30, true),
        (6, "Zelda", 19, 19, 17, false),
    ] {
        writeln!(
            script,
            "INSERT INTO PerfAuditUser(id, name, age, age_nat, rank, active) VALUES ({id}, '{}', {age}, {age_nat}, {rank}, {});",
            sqlite_quote(name),
            i32::from(active),
        )
        .expect("write to string should succeed");
    }
}

fn append_sqlite_perf_audit_account_rows(script: &mut String) {
    for (id, handle, tier, active, score) in [
        (1, "Bravo", "gold", true, 91),
        (2, "alpha", "gold", true, 75),
        (3, "bravo", "silver", true, 78),
        (4, "Delta", "silver", false, 66),
        (5, "brick", "gold", true, 88),
        (6, "azure", "bronze", true, 63),
    ] {
        writeln!(
            script,
            "INSERT INTO PerfAuditAccount(id, handle, tier, active, score) VALUES ({id}, '{}', '{}', {}, {score});",
            sqlite_quote(handle),
            sqlite_quote(tier),
            i32::from(active),
        )
        .expect("write to string should succeed");
    }
}

fn append_sqlite_perf_audit_blob_rows(script: &mut String) {
    for (id, label, bucket, thumbnail_seed, thumbnail_len, chunk_seed, chunk_len) in [
        (1, "avatar-a", 10, 11, 1_024, 31, 16_384),
        (2, "avatar-b", 10, 12, 2_048, 32, 32_768),
        (3, "avatar-c", 10, 13, 4_096, 33, 65_536),
        (4, "archive-a", 20, 14, 1_024, 34, 16_384),
        (5, "archive-b", 20, 15, 2_048, 35, 32_768),
        (6, "archive-c", 30, 16, 4_096, 36, 65_536),
    ] {
        writeln!(
            script,
            "INSERT INTO PerfAuditBlob(id, label, bucket, thumbnail, chunk) VALUES ({id}, '{}', {bucket}, X'{}', X'{}');",
            sqlite_quote(label),
            sqlite_perf_blob_hex(thumbnail_seed, thumbnail_len),
            sqlite_perf_blob_hex(chunk_seed, chunk_len),
        )
        .expect("write to string should succeed");
    }
}

fn append_sqlite_perf_audit_token_rows(script: &mut String) {
    let mut append_token = |id: u128, collection_id: &str, stage: &str, title: &str| {
        writeln!(
            script,
            "INSERT INTO PerfAuditToken(id, collection_id, stage, title) VALUES ('{}', '{}', '{}', '{}');",
            icydb::types::Ulid::from_bytes(id.to_be_bytes()),
            sqlite_quote(collection_id),
            sqlite_quote(stage),
            sqlite_quote(title),
        )
        .expect("write to string should succeed");
    };

    for (id, collection_id, stage, title) in [
        (9_090, TOKEN_TARGET_COLLECTION, "Draft", "draft-090"),
        (9_095, TOKEN_TARGET_COLLECTION, "Review", "review-095"),
        (9_100, TOKEN_TARGET_COLLECTION, "Review", "review-100"),
        (9_105, TOKEN_TARGET_COLLECTION, "Draft", "draft-105"),
        (9_110, TOKEN_TARGET_COLLECTION, "Published", "published-110"),
        (
            9_115,
            "01KV5N439P1111111111111111",
            "Draft",
            "other-draft-115",
        ),
        (9_120, TOKEN_TARGET_COLLECTION, "Draft", "draft-120"),
        (9_125, TOKEN_TARGET_COLLECTION, "Review", "review-125"),
        (9_130, TOKEN_TARGET_COLLECTION, "Draft", "draft-130"),
        (9_135, TOKEN_TARGET_COLLECTION, "Review", "review-135"),
        (9_140, TOKEN_TARGET_COLLECTION, "Queued", "queued-140"),
        (
            9_145,
            "01KV5N439P1111111111111111",
            "Review",
            "other-review-145",
        ),
        (9_150, TOKEN_TARGET_COLLECTION, "Draft", "draft-150"),
        (9_155, TOKEN_TARGET_COLLECTION, "Review", "review-155"),
        (9_160, TOKEN_TARGET_COLLECTION, "Archived", "archived-160"),
        (
            9_165,
            "01KV5N439P1111111111111111",
            "Draft",
            "other-draft-165",
        ),
        (9_170, TOKEN_TARGET_COLLECTION, "Draft", "draft-170"),
        (9_175, TOKEN_TARGET_COLLECTION, "Review", "review-175"),
        (9_180, TOKEN_TARGET_COLLECTION, "Rejected", "rejected-180"),
        (
            9_185,
            "01KV5N439P1111111111111111",
            "Review",
            "other-review-185",
        ),
    ] {
        append_token(id, collection_id, stage, title);
    }

    for offset in 0..240u128 {
        let stage = match offset % 4 {
            0 => "Draft",
            1 => "Queued",
            2 => "Review",
            _ => "Published",
        };
        let title = format!("{}-pressure-{offset:03}", stage.to_ascii_lowercase());
        append_token(
            10_000 + offset,
            TOKEN_TARGET_COLLECTION,
            stage,
            title.as_str(),
        );
    }
}

fn sqlite_perf_blob_hex(seed: u8, len: usize) -> String {
    let mut out = String::with_capacity(len.saturating_mul(2));
    for value in (0u8..=250)
        .cycle()
        .take(len)
        .map(|offset| seed.wrapping_add(offset))
    {
        write!(out, "{value:02x}").expect("write to string should succeed");
    }
    out
}

fn sqlite_quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn sqlite_output(sqlite_path: &Path, db_path: &Path, sql: &str) -> Result<String, String> {
    let mut child = Command::new(sqlite_path)
        .arg("-batch")
        .arg("-noheader")
        .arg("-cmd")
        .arg(".mode tabs")
        .arg("-cmd")
        .arg(".nullvalue NULL")
        .arg(db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to run `{}`: {err}", sqlite_path.display()))?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| format!("failed to open stdin for `{}`", sqlite_path.display()))?
        .write_all(sql.as_bytes())
        .map_err(|err| format!("failed to write SQL to `{}`: {err}", sqlite_path.display()))?;
    let output = child
        .wait_with_output()
        .map_err(|err| format!("failed to wait for `{}`: {err}", sqlite_path.display()))?;

    if !output.status.success() {
        return Err(format!(
            "`{}` failed with status {:?}: {}",
            sqlite_path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .trim_end_matches(['\n', '\r'])
        .replace("\r\n", "\n"))
}

fn sqlite_query_signature(sqlite_path: &Path, db_path: &Path, sql: &str) -> Result<String, String> {
    sqlite_output(
        sqlite_path,
        db_path,
        format!("PRAGMA case_sensitive_like=ON;\n{sql};").as_str(),
    )
}

fn sqlite_explain_query_plan(
    sqlite_path: &Path,
    db_path: &Path,
    sql: &str,
) -> Result<Vec<String>, String> {
    let output = sqlite_output(
        sqlite_path,
        db_path,
        format!("PRAGMA case_sensitive_like=ON;\nEXPLAIN QUERY PLAN {sql};").as_str(),
    )?;

    Ok(output.lines().map(str::to_string).collect())
}

fn sqlite_plan_summary(rows: &[String]) -> SqlitePlanSummary {
    let mut summary = SqlitePlanSummary::default();
    for detail in rows.iter().map(|row| sqlite_plan_detail(row)) {
        if detail.starts_with("SCAN ") {
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_SCAN.to_string());
        }
        if detail.starts_with("SEARCH ") {
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_SEARCH.to_string());
        }
        if detail.contains("USING COVERING INDEX ") {
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_COVERING_INDEX.to_string());
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_INDEX.to_string());
        }
        if detail.contains("USING INTEGER PRIMARY KEY") {
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_INTEGER_PRIMARY_KEY.to_string());
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_INDEX.to_string());
        }
        if detail.contains("USING INDEX ") {
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_INDEX.to_string());
        }
        if detail.contains("USE TEMP B-TREE") && detail.contains("ORDER BY") {
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_TEMP_ORDER.to_string());
        }
        if detail.contains("USE TEMP B-TREE") && detail.contains("GROUP BY") {
            summary
                .features
                .insert(SQLITE_PLAN_FEATURE_TEMP_GROUP.to_string());
        }

        if let Some(index_name) = sqlite_plan_index_name(detail) {
            summary.index_names.push(index_name);
        }
    }

    summary.index_names.sort();
    summary.index_names.dedup();
    summary
}

fn sqlite_plan_has(summary: &SqlitePlanSummary, feature: &str) -> bool {
    summary.features.contains(feature)
}

fn sqlite_plan_detail(row: &str) -> &str {
    let mut detail = row.rsplit('\t').next().unwrap_or(row).trim();
    while let Some(stripped) = detail
        .strip_prefix("|--")
        .or_else(|| detail.strip_prefix("`--"))
    {
        detail = stripped.trim_start();
    }
    detail
}

fn sqlite_plan_index_name(detail: &str) -> Option<String> {
    for prefix in ["USING COVERING INDEX ", "USING INDEX "] {
        if let Some(suffix) = detail.split_once(prefix).map(|(_, suffix)| suffix) {
            return Some(
                suffix
                    .split([' ', '('])
                    .next()
                    .unwrap_or(suffix)
                    .to_string(),
            );
        }
    }

    detail
        .contains("USING INTEGER PRIMARY KEY")
        .then(|| "INTEGER_PRIMARY_KEY".to_string())
}

fn sqlite_plan_alignment(sample: &MatrixSample, summary: &SqlitePlanSummary) -> String {
    match sample.route_outcome.as_str() {
        "pushed" if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_TEMP_ORDER) => {
            "review_icydb_pushed_sqlite_temp_order".to_string()
        }
        "pushed"
            if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_INDEX)
                || !sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_SCAN) =>
        {
            "aligned_bounded_access".to_string()
        }
        "pushed" if sample.route_family == "primary_order" => "aligned_ordered_access".to_string(),
        "pushed" => "review_icydb_pushed_sqlite_scan".to_string(),
        "materialized" if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_TEMP_ORDER) => {
            "aligned_materialized_order".to_string()
        }
        "materialized"
            if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_INDEX)
                && !sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_TEMP_ORDER) =>
        {
            "sqlite_index_order_icydb_materialized".to_string()
        }
        "materialized" => "aligned_scan_or_materialized".to_string(),
        _ => "not_comparable".to_string(),
    }
}

fn sqlite_time_query(
    sqlite_path: &Path,
    db_path: &Path,
    sql: &str,
    expected_signature: &str,
    sample_count: usize,
) -> Result<SqliteTimingSummary, String> {
    if sample_count == 0 {
        return Ok(SqliteTimingSummary {
            samples: Vec::new(),
            median: 0,
            min: 0,
            max: 0,
        });
    }

    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let start = Instant::now();
        let signature = sqlite_query_signature(sqlite_path, db_path, sql)?;
        let elapsed = start.elapsed().as_nanos();
        if signature != expected_signature {
            return Err(format!(
                "SQLite result signature changed between timing samples for `{sql}`: expected `{expected_signature}`, got `{signature}`"
            ));
        }
        samples.push(elapsed);
    }
    Ok(SqliteTimingSummary::from_samples(samples))
}

fn sqlite_audit_comparison_for_scenario(
    fixture: &StandaloneCanisterFixture,
    sqlite_path: &Path,
    db_path: &Path,
    scenario: &MatrixScenario,
    timing_sample_count: usize,
) -> Result<SqliteAuditComparisonScenario, Box<SqliteAuditComparisonFailure>> {
    let sqlite_signature = sqlite_query_signature(sqlite_path, db_path, scenario.sql.as_str())
        .unwrap_or_else(|err| {
            panic!(
                "SQLite query failed for comparison scenario `{}`: {err}",
                scenario.key
            )
        });
    let sqlite_explain_query_plan =
        sqlite_explain_query_plan(sqlite_path, db_path, scenario.sql.as_str()).unwrap_or_else(
            |err| {
                panic!(
                    "SQLite EXPLAIN QUERY PLAN failed for comparison scenario `{}`: {err}",
                    scenario.key
                )
            },
        );
    let sqlite_plan_summary = sqlite_plan_summary(&sqlite_explain_query_plan);

    let perf = match query_surface_with_perf(fixture, scenario) {
        Ok(perf) => perf,
        Err(err) => {
            return Err(sqlite_audit_comparison_failure(
                scenario,
                err,
                sqlite_signature,
                sqlite_explain_query_plan,
                sqlite_plan_summary,
            ));
        }
    };
    let sample = matrix_sample_from_perf(scenario, &perf);
    let icydb_signature = sqlite_comparable_signature(&perf.result).unwrap_or_else(|| {
        panic!(
            "scenario `{}` did not produce a SQLite-comparable IcyDB result",
            scenario.key
        )
    });
    let sqlite_plan_alignment = sqlite_plan_alignment(&sample, &sqlite_plan_summary);
    let sqlite_timing = sqlite_time_query(
        sqlite_path,
        db_path,
        scenario.sql.as_str(),
        sqlite_signature.as_str(),
        timing_sample_count,
    )
    .unwrap_or_else(|err| {
        panic!(
            "SQLite timing failed for comparison scenario `{}`: {err}",
            scenario.key
        )
    });

    Ok(SqliteAuditComparisonScenario {
        key: scenario.key.clone(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        route_family: sample.route_family,
        route_outcome: sample.route_outcome,
        route_reason: sample.route_reason,
        limit_stop_after: sample.limit_stop_after,
        icydb_signature: icydb_signature.clone(),
        sqlite_signature: sqlite_signature.clone(),
        signatures_match: icydb_signature == sqlite_signature,
        sqlite_explain_query_plan,
        sqlite_plan_summary,
        sqlite_plan_alignment,
        icydb_total_local_instructions: sample.total_local_instructions,
        icydb_execute_local_instructions: sample.execute_local_instructions,
        icydb_data_store_get_calls: sample.data_store_get_calls,
        icydb_index_store_range_scan_calls: sample.index_store_range_scan_calls,
        icydb_index_store_entry_reads: sample.index_store_entry_reads,
        sqlite_timing,
    })
}

fn sqlite_audit_comparison_failure(
    scenario: &MatrixScenario,
    err: Error,
    sqlite_signature: String,
    sqlite_explain_query_plan: Vec<String>,
    sqlite_plan_summary: SqlitePlanSummary,
) -> Box<SqliteAuditComparisonFailure> {
    let diagnostic_code = err.diagnostic_code();
    Box::new(SqliteAuditComparisonFailure {
        key: scenario.key.clone(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        status: "icydb_failure".to_string(),
        icydb_code: err.code().raw(),
        icydb_diagnostic_code: diagnostic_code.error_code().raw(),
        icydb_diagnostic_label: diagnostic_label(diagnostic_code).to_string(),
        icydb_class: error_class_label(err.class()).to_string(),
        icydb_origin: format!("{:?}", err.origin()),
        sqlite_signature,
        sqlite_explain_query_plan,
        sqlite_plan_summary,
    })
}

impl SqliteTimingSummary {
    fn from_samples(mut samples: Vec<u128>) -> Self {
        samples.sort_unstable();
        let median = samples[samples.len() / 2];
        let min = samples[0];
        let max = samples[samples.len() - 1];

        Self {
            samples,
            median,
            min,
            max,
        }
    }
}

fn sqlite_comparable_signature(result: &SqlQueryResult) -> Option<String> {
    match result {
        SqlQueryResult::Count { row_count, .. } => Some(row_count.to_string()),
        SqlQueryResult::Projection(rows) => Some(rendered_rows_signature(&rows.rendered_rows())),
        SqlQueryResult::Grouped(rows) => Some(rendered_rows_signature(&rows.rows)),
        _ => None,
    }
}

fn rendered_rows_signature(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn sqlite_audit_comparison_fairness_notes() -> Vec<String> {
    vec![
        "SQLite runs through the local sqlite3 CLI and is not using Internet Computer stable memory.".to_string(),
        "IcyDB runs in the existing sql_perf audit canister and reports local instruction counters, not native wall-clock time.".to_string(),
        "SQLite timings include CLI process startup and are diagnostic outlier signals, not headline benchmark claims.".to_string(),
        "The SQLite fixture mirrors the main PerfAudit table data and indexes where SQLite can express the same shape.".to_string(),
        "Only overlapping SELECT/COUNT/GROUP BY scenarios are compared; metadata and IcyDB-only SQL surfaces stay out of this harness.".to_string(),
    ]
}

fn sqlite_audit_comparison_output_stem() -> PathBuf {
    env::var(SQL_PERF_SQLITE_OUTPUT_STEM_ENV).map_or_else(
        |_| PathBuf::from(DEFAULT_SQL_PERF_SQLITE_OUTPUT_STEM),
        PathBuf::from,
    )
}

fn write_sqlite_audit_comparison_reports(report: &SqliteAuditComparisonReport) {
    let output_stem = sqlite_audit_comparison_output_stem();
    if let Some(parent) = output_stem.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|err| {
            panic!(
                "failed to create SQLite audit comparison report directory `{}`: {err}",
                parent.display()
            )
        });
    }

    let json_path = output_stem.with_extension("json");
    let markdown_path = output_stem.with_extension("md");
    let json = serde_json::to_string_pretty(report)
        .expect("SQLite audit comparison report should serialize");
    fs::write(&json_path, json).unwrap_or_else(|err| {
        panic!(
            "failed to write SQLite audit comparison JSON `{}`: {err}",
            json_path.display()
        )
    });
    fs::write(&markdown_path, sqlite_audit_comparison_markdown(report)).unwrap_or_else(|err| {
        panic!(
            "failed to write SQLite audit comparison Markdown `{}`: {err}",
            markdown_path.display()
        )
    });
}

fn print_sqlite_audit_comparison_report(report: &SqliteAuditComparisonReport) {
    if report.compared_scenario_count <= 50 {
        println!("{}", sqlite_audit_comparison_markdown(report));
        return;
    }

    let output_stem = sqlite_audit_comparison_output_stem();
    println!(
        "SQLite audit comparison: compared={}, common_success={}, icydb_failures={}, signature_mismatches={}, artifacts={}{{.json,.md}}",
        report.compared_scenario_count,
        report.common_success_count,
        report.icydb_failure_count,
        report.signature_mismatch_count,
        output_stem.display(),
    );
}

fn sqlite_audit_comparison_markdown(report: &SqliteAuditComparisonReport) -> String {
    let mut out = String::new();
    writeln!(out, "# SQL Perf Audit SQLite Comparison").expect("write to string should succeed");
    writeln!(out).expect("write to string should succeed");
    writeln!(out, "- SQLite version: {}", report.sqlite_version)
        .expect("write to string should succeed");
    writeln!(out, "- SQLite path: {}", report.sqlite_path).expect("write to string should succeed");
    writeln!(
        out,
        "- Canister wasm profile: {}",
        report.canister_wasm_profile
    )
    .expect("write to string should succeed");
    writeln!(
        out,
        "- Compared scenarios: {} of {} generated",
        report.compared_scenario_count, report.generated_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(out, "- Common successes: {}", report.common_success_count)
        .expect("write to string should succeed");
    writeln!(out, "- IcyDB failures: {}", report.icydb_failure_count)
        .expect("write to string should succeed");
    writeln!(
        out,
        "- Signature mismatches: {}",
        report.signature_mismatch_count
    )
    .expect("write to string should succeed");
    writeln!(out, "- SQLite timing samples: {}", report.sample_count)
        .expect("write to string should succeed");
    writeln!(out).expect("write to string should succeed");
    writeln!(out, "## Fairness Notes").expect("write to string should succeed");
    for note in &report.fairness_notes {
        writeln!(out, "- {note}").expect("write to string should succeed");
    }
    writeln!(out).expect("write to string should succeed");
    append_sqlite_success_table(&mut out, &report.scenarios);
    append_sqlite_failure_table(&mut out, &report.failures);
    append_sqlite_explain_plans(&mut out, &report.scenarios, &report.failures);
    out
}

fn append_sqlite_success_table(output: &mut String, scenarios: &[SqliteAuditComparisonScenario]) {
    writeln!(output, "## Scenarios").expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Route | Outcome | Match | SQLite Plan | Alignment | IcyDB Total | IcyDB Execute | Gets | Ranges | Entries | SQLite Median ns |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "| --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    .expect("write to string should succeed");
    for scenario in scenarios {
        writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            scenario.key,
            scenario.surface,
            scenario.route_family,
            scenario.route_outcome,
            scenario.signatures_match,
            sqlite_plan_summary_cell(&scenario.sqlite_plan_summary),
            scenario.sqlite_plan_alignment,
            scenario.icydb_total_local_instructions,
            scenario.icydb_execute_local_instructions,
            scenario.icydb_data_store_get_calls,
            scenario.icydb_index_store_range_scan_calls,
            scenario.icydb_index_store_entry_reads,
            sqlite_timing_median_cell(&scenario.sqlite_timing),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_sqlite_failure_table(output: &mut String, failures: &[SqliteAuditComparisonFailure]) {
    if failures.is_empty() {
        return;
    }

    writeln!(output, "## IcyDB Failures").expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Diagnostic | Class | Origin | SQLite Plan | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "| --- | --- | --- | --- | --- | --- | --- |")
        .expect("write to string should succeed");
    for failure in failures {
        writeln!(
            output,
            "| {} | {} | {} ({}) | {} | {} | {} | `{}` |",
            failure.key,
            failure.surface,
            failure.icydb_diagnostic_label,
            failure.icydb_diagnostic_code,
            failure.icydb_class,
            failure.icydb_origin,
            sqlite_plan_summary_cell(&failure.sqlite_plan_summary),
            failure.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_sqlite_explain_plans(
    output: &mut String,
    scenarios: &[SqliteAuditComparisonScenario],
    failures: &[SqliteAuditComparisonFailure],
) {
    writeln!(output, "## SQLite EXPLAIN QUERY PLAN").expect("write to string should succeed");
    for scenario in scenarios {
        writeln!(output).expect("write to string should succeed");
        writeln!(output, "### {}", scenario.key).expect("write to string should succeed");
        for row in &scenario.sqlite_explain_query_plan {
            writeln!(output, "- `{}`", row.replace('`', "\\`"))
                .expect("write to string should succeed");
        }
    }
    for failure in failures {
        writeln!(output).expect("write to string should succeed");
        writeln!(output, "### {} [icydb_failure]", failure.key)
            .expect("write to string should succeed");
        for row in &failure.sqlite_explain_query_plan {
            writeln!(output, "- `{}`", row.replace('`', "\\`"))
                .expect("write to string should succeed");
        }
    }
}

fn sqlite_plan_summary_cell(summary: &SqlitePlanSummary) -> String {
    let mut parts = Vec::new();
    if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_SCAN) {
        parts.push("scan".to_string());
    }
    if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_SEARCH) {
        parts.push("search".to_string());
    }
    if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_COVERING_INDEX) {
        parts.push("covering-index".to_string());
    } else if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_INDEX) {
        parts.push("index".to_string());
    }
    if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_TEMP_ORDER) {
        parts.push("temp-order".to_string());
    }
    if sqlite_plan_has(summary, SQLITE_PLAN_FEATURE_TEMP_GROUP) {
        parts.push("temp-group".to_string());
    }
    if !summary.index_names.is_empty() {
        parts.push(format!("indexes={}", summary.index_names.join("+")));
    }

    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(", ")
    }
}

fn sqlite_timing_median_cell(timing: &SqliteTimingSummary) -> String {
    if timing.samples.is_empty() {
        "n/a".to_string()
    } else {
        timing.median.to_string()
    }
}

fn install_sql_perf_canister_fixture() -> StandaloneCanisterFixture {
    let options = matrix_canister_build_options();
    eprintln!(
        "sql_perf_matrix: canister wasm profile {}",
        options.profile.as_str(),
    );
    if matrix_install_progress_enabled() {
        return install_fixture_canister_with_options_and_progress(
            "sql_perf",
            options,
            "sql_perf_matrix",
        );
    }

    install_fixture_canister_with_options("sql_perf", options)
}

fn matrix_install_progress_enabled() -> bool {
    env::var(SQL_PERF_MATRIX_INSTALL_PROGRESS_ENV)
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<SqlQueryPerfResult, Error> {
    fixture
        .query_call(scenario.surface.query_method(), (scenario.sql.clone(),))
        .unwrap_or_else(|err| panic!("{} should decode: {err}", scenario.surface.query_method()))
}

fn summarize_perf_outcome(result: &SqlQueryResult) -> MatrixOutcome {
    match result {
        SqlQueryResult::Count { entity, row_count } => MatrixOutcome {
            result_kind: "count".to_string(),
            entity: entity.clone(),
            row_count: usize::try_from(*row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Projection(rows) => MatrixOutcome {
            result_kind: "projection".to_string(),
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Grouped(rows) => MatrixOutcome {
            result_kind: "grouped".to_string(),
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Explain { entity, .. } => MatrixOutcome {
            result_kind: "explain".to_string(),
            entity: entity.clone(),
            row_count: 1,
        },
        SqlQueryResult::Describe(entity) => MatrixOutcome {
            result_kind: "describe".to_string(),
            entity: entity.entity_name().to_string(),
            row_count: entity.fields().len(),
        },
        SqlQueryResult::ShowIndexes { entity, indexes } => MatrixOutcome {
            result_kind: "show_indexes".to_string(),
            entity: entity.clone(),
            row_count: indexes.len(),
        },
        SqlQueryResult::ShowColumns { entity, columns } => MatrixOutcome {
            result_kind: "show_columns".to_string(),
            entity: entity.clone(),
            row_count: columns.len(),
        },
        SqlQueryResult::ShowEntities { entities, .. } => MatrixOutcome {
            result_kind: "show_entities".to_string(),
            entity: String::new(),
            row_count: entities.len(),
        },
        SqlQueryResult::ShowStores { stores, .. } => MatrixOutcome {
            result_kind: "show_stores".to_string(),
            entity: String::new(),
            row_count: stores.len(),
        },
        SqlQueryResult::ShowMemory { memory } => MatrixOutcome {
            result_kind: "show_memory".to_string(),
            entity: String::new(),
            row_count: memory.len(),
        },
        SqlQueryResult::Ddl { entity, .. } => MatrixOutcome {
            result_kind: "icydb_ddl".to_string(),
            entity: entity.clone(),
            row_count: 1,
        },
    }
}

fn sample_scenario(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<MatrixSample, Box<MatrixFailure>> {
    let perf = query_surface_with_perf(fixture, scenario)
        .map_err(|err| Box::new(matrix_failure_from_error(scenario, err)))?;

    Ok(matrix_sample_from_perf(scenario, &perf))
}

fn matrix_sample_from_perf(scenario: &MatrixScenario, perf: &SqlQueryPerfResult) -> MatrixSample {
    let attribution = &perf.attribution;
    let mut sample = MatrixSample {
        key: scenario.key.clone(),
        source: scenario.source.label().to_string(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        outcome: summarize_perf_outcome(&perf.result),
        ..MatrixSample::default()
    };
    fill_matrix_compile_sample(&mut sample, attribution);
    fill_matrix_execution_sample(&mut sample, attribution);
    fill_matrix_grouped_sample(&mut sample, attribution);
    fill_matrix_scalar_aggregate_sample(&mut sample, attribution);
    fill_matrix_projection_path_sample(&mut sample, attribution);
    fill_matrix_store_output_cache_sample(&mut sample, attribution);

    let route = route_classification_for_sample(&sample);
    sample.route_family = route.family.to_string();
    sample.route_outcome = route.outcome.to_string();
    sample.route_reason = route.reason.map(str::to_string);
    sample.order_by_idx_hint = sql_order_by_idx_hint(&sample.sql);
    sample.limit_stop_after = limit_stop_after_for_sample(&sample);
    sample.result_signature = Some(result_signature(&perf.result));
    sample.cursor_signature = cursor_signature(&perf.result);

    sample
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RouteClassification {
    family: &'static str,
    outcome: &'static str,
    reason: Option<&'static str>,
}

impl RouteClassification {
    const fn new(
        family: &'static str,
        outcome: &'static str,
        reason: Option<&'static str>,
    ) -> Self {
        Self {
            family,
            outcome,
            reason,
        }
    }
}

fn route_classification_for_sample(sample: &MatrixSample) -> RouteClassification {
    if !sample.sql.starts_with("SELECT") || !sample.sql.contains(" LIMIT ") {
        return RouteClassification::new(
            "not_ordered_or_not_paginated",
            "unchanged_or_not_applicable",
            Some("not_a_paginated_select"),
        );
    }
    if !sample.sql.contains(" ORDER BY ") {
        return RouteClassification::new(
            "not_ordered_or_not_paginated",
            "unchanged_or_not_applicable",
            Some("no_order_by"),
        );
    }
    if sample.surface == MatrixSurface::HeapUser.label()
        || sample.surface == MatrixSurface::JournaledUser.label()
    {
        return classify_storage_mirror_route(sample);
    }
    if sample.sql.contains("ORDER BY id ASC") || sample.sql.contains("ORDER BY id DESC") {
        return classify_primary_order_route(sample, "primary_order_candidate");
    }
    if sample.sql.contains("collection_id =")
        && (sample.sql.contains("ORDER BY stage ASC, id ASC")
            || sample.sql.contains("ORDER BY stage DESC, id DESC"))
    {
        return classify_index_order_route(
            sample,
            "equality_prefix_ordered_suffix",
            "equality_prefix_ordered_suffix_candidate",
            "equality_prefix_ordered_suffix_limit_stop_proven",
        );
    }
    if sample.sql.contains(" GROUP BY ") {
        return RouteClassification::new(
            "materialized_order",
            "materialized",
            Some("grouped_aggregate_materialized"),
        );
    }
    if sample.sql.contains("ORDER BY age ")
        || sample.sql.contains("ORDER BY name ")
        || sample.sql.contains("ORDER BY handle ")
        || sample.sql.contains("ORDER BY bucket ")
        || sample.sql.contains("ORDER BY label ")
        || sample.sql.contains("ORDER BY tier ")
        || sample.sql.contains("ORDER BY LOWER(")
    {
        return classify_secondary_order_route(sample);
    }

    RouteClassification::new(
        "unsupported_access_kind",
        "unsupported",
        Some("order_expression_not_classified"),
    )
}

fn classify_storage_mirror_route(sample: &MatrixSample) -> RouteClassification {
    if sample.sql.contains("ORDER BY id ASC") || sample.sql.contains("ORDER BY id DESC") {
        return classify_primary_order_route(sample, "storage_mirror_primary_order_candidate");
    }

    RouteClassification::new(
        "materialized_order",
        "materialized",
        Some("storage_mirror_has_primary_index_only"),
    )
}

fn classify_primary_order_route(
    sample: &MatrixSample,
    candidate_reason: &'static str,
) -> RouteClassification {
    if primary_order_has_materialized_window(sample) {
        return RouteClassification::new(
            "materialized_order",
            "materialized",
            Some("requires_materialized_sort"),
        );
    }
    if primary_order_requires_candidate_scan(sample) {
        return RouteClassification::new(
            "residual_filter_ordered_scan",
            "residual_unbounded",
            Some("residual_filter_requires_candidate_scan"),
        );
    }
    if primary_order_limit_stop_is_proven(sample) {
        return RouteClassification::new(
            "primary_order",
            "pushed",
            Some("primary_order_limit_stop_proven"),
        );
    }

    RouteClassification::new(
        "primary_order",
        "eligible_but_not_pushed",
        Some(candidate_reason),
    )
}

const fn primary_order_has_materialized_window(sample: &MatrixSample) -> bool {
    order_window_was_materialized(sample)
}

fn primary_order_requires_candidate_scan(sample: &MatrixSample) -> bool {
    let Some((predicate_key, _order_key)) = select_predicate_and_order_keys(&sample.family) else {
        return false;
    };
    !(predicate_key == "all" || predicate_key.starts_with("pk"))
}

fn primary_order_limit_stop_is_proven(sample: &MatrixSample) -> bool {
    let Some(bound) = ordered_limit_read_bound(sample) else {
        return false;
    };
    if primary_order_requires_candidate_scan(sample) || order_window_was_materialized(sample) {
        return false;
    }

    sample.data_store_get_calls <= bound
}

fn ordered_limit_read_bound(sample: &MatrixSample) -> Option<u64> {
    let limit = sql_clause_usize_value(&sample.sql, " LIMIT ")?;
    let offset = sql_clause_usize_value(&sample.sql, " OFFSET ").unwrap_or(0);
    let bound = limit.saturating_add(offset).saturating_add(1);
    Some(u64::try_from(bound).unwrap_or(u64::MAX))
}

fn sql_clause_usize_value(sql: &str, marker: &str) -> Option<usize> {
    let tail = sql.split_once(marker)?.1.trim_start();
    let end = tail
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(tail.len());
    if end == 0 {
        return None;
    }
    tail[..end].parse().ok()
}

fn sql_order_by_idx_hint(sql: &str) -> Option<String> {
    let clause = sql_order_by_clause(sql)?;
    let terms = split_sql_top_level_commas(clause)
        .into_iter()
        .map(normalize_sql_order_term)
        .filter(|term| !term.is_empty())
        .collect::<Vec<_>>();
    if terms.is_empty() {
        return None;
    }

    Some(terms.join(", "))
}

fn sql_order_by_clause(sql: &str) -> Option<&str> {
    let tail = sql.split_once(" ORDER BY ")?.1.trim_start();
    let end = [" LIMIT ", " OFFSET "]
        .into_iter()
        .filter_map(|marker| tail.find(marker))
        .min()
        .unwrap_or(tail.len());
    let clause = tail[..end].trim();
    (!clause.is_empty()).then_some(clause)
}

fn split_sql_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut in_string = false;

    for (index, character) in input.char_indices() {
        match character {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth = depth.saturating_add(1),
            ')' if !in_string => depth = depth.saturating_sub(1),
            ',' if !in_string && depth == 0 => {
                parts.push(input[start..index].trim());
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(input[start..].trim());
    parts
}

fn normalize_sql_order_term(term: &str) -> String {
    term.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn limit_stop_after_for_sample(sample: &MatrixSample) -> MatrixLimitStopAfter {
    let returned_limit = sql_clause_usize_value(&sample.sql, " LIMIT ");
    let possible = sample.route_outcome == "pushed";
    MatrixLimitStopAfter {
        possible,
        returned_limit,
        lookahead: returned_limit.map_or(0, |limit| usize::from(limit > 0)),
        stopped_after_matches: possible
            .then(|| u64::try_from(sample.outcome.row_count).unwrap_or(u64::MAX)),
        stopped_after_index_entries: possible.then_some(sample.index_store_entry_reads),
        disabled_reason: (!possible).then(|| limit_stop_after_disabled_reason(sample)),
    }
}

fn limit_stop_after_disabled_reason(sample: &MatrixSample) -> String {
    if !sample.sql.contains(" LIMIT ") {
        return "no_limit".to_string();
    }
    if !sample.sql.contains(" ORDER BY ") {
        return "no_order_by".to_string();
    }

    sample
        .route_reason
        .clone()
        .unwrap_or_else(|| "not_pushed".to_string())
}

fn classify_secondary_order_route(sample: &MatrixSample) -> RouteClassification {
    let Some((predicate_key, order_key)) = select_predicate_and_order_keys(&sample.family) else {
        return RouteClassification::new(
            "secondary_order",
            "eligible_but_not_pushed",
            Some("secondary_order_candidate"),
        );
    };
    if secondary_order_has_index_suffix_gap(sample, order_key) {
        return RouteClassification::new(
            "secondary_order",
            "missing_tie_breaker",
            Some("index_order_suffix_gap"),
        );
    }
    if order_key.starts_with("numeric_expr") {
        return RouteClassification::new(
            "unsupported_access_kind",
            "unsupported",
            Some("order_expression_not_classified"),
        );
    }
    if predicate_order_is_obviously_incompatible(predicate_key, order_key) {
        return RouteClassification::new(
            "incompatible_filter_first_order",
            "materialized",
            Some("filter_order_mismatch"),
        );
    }
    if predicate_key == "field_compare" {
        return RouteClassification::new(
            "residual_filter_ordered_scan",
            "residual_unbounded",
            Some("residual_filter_requires_candidate_scan"),
        );
    }
    if secondary_order_requires_candidate_scan(predicate_key) {
        return RouteClassification::new(
            "residual_filter_ordered_scan",
            "residual_unbounded",
            Some("residual_filter_requires_candidate_scan"),
        );
    }

    classify_index_order_route(
        sample,
        "secondary_order",
        "secondary_order_candidate",
        "secondary_order_limit_stop_proven",
    )
}

fn classify_index_order_route(
    sample: &MatrixSample,
    family: &'static str,
    candidate_reason: &'static str,
    pushed_reason: &'static str,
) -> RouteClassification {
    if order_window_was_materialized(sample) {
        return RouteClassification::new(
            "materialized_order",
            "materialized",
            Some("requires_materialized_sort"),
        );
    }
    if index_order_limit_stop_is_proven(sample) {
        return RouteClassification::new(family, "pushed", Some(pushed_reason));
    }

    RouteClassification::new(family, "eligible_but_not_pushed", Some(candidate_reason))
}

fn secondary_order_requires_candidate_scan(predicate_key: &str) -> bool {
    predicate_key == "field_compare"
        || predicate_key.ends_with("_active")
        || predicate_key.contains("_active_")
}

fn secondary_order_has_index_suffix_gap(sample: &MatrixSample, order_key: &str) -> bool {
    // PerfAuditBlob's ordered metadata index is `(bucket, label, id)`. The
    // matrix also emits `ORDER BY bucket, id` cases to expose the next-order
    // frontier, but those cannot use the declared index order because `label`
    // is the intervening suffix key.
    sample.surface == MatrixSurface::Blob.label() && order_key == "bucket_asc"
}

fn index_order_limit_stop_is_proven(sample: &MatrixSample) -> bool {
    let Some(bound) = ordered_limit_read_bound(sample) else {
        return false;
    };
    if order_window_was_materialized(sample) {
        return false;
    }

    sample.data_store_get_calls <= bound && sample.index_store_entry_reads <= bound
}

const fn order_window_was_materialized(sample: &MatrixSample) -> bool {
    sample.direct_data_row_order_window_local_instructions != 0
        || sample.kernel_row_order_window_local_instructions != 0
}

fn select_predicate_and_order_keys(family: &str) -> Option<(&str, &str)> {
    let mut parts = family.split('.');
    if parts.next()? != "select" {
        return None;
    }
    let _projection_key = parts.next()?;
    let predicate_key = parts.next()?;
    let order_key = parts.next()?;
    Some((predicate_key, order_key))
}

fn predicate_order_is_obviously_incompatible(predicate_key: &str, order_key: &str) -> bool {
    if predicate_key == "all" || order_key.starts_with("pk_") {
        return false;
    }
    if predicate_key.starts_with("age") && order_key.starts_with("age") {
        return false;
    }
    if predicate_key.starts_with("name") && order_key.starts_with("name") {
        return false;
    }
    if predicate_key.starts_with("lower_name") && order_key.starts_with("lower_name") {
        return false;
    }
    if predicate_key.starts_with("handle") && order_key.starts_with("handle") {
        return false;
    }
    if predicate_key.starts_with("lower_handle") && order_key.starts_with("lower_handle") {
        return false;
    }
    if predicate_key.starts_with("bucket") && order_key.starts_with("bucket") {
        return false;
    }
    if predicate_key.starts_with("label") && order_key.starts_with("label") {
        return false;
    }
    predicate_key != "active_true" || !order_key.starts_with("tier")
}

fn result_signature(result: &SqlQueryResult) -> String {
    match result {
        SqlQueryResult::Count { entity, row_count } => {
            format!("count|{entity}|{row_count}")
        }
        SqlQueryResult::Projection(rows) => {
            let rendered_rows = rows
                .rendered_rows()
                .into_iter()
                .map(|row| row.join("\u{1f}"))
                .collect::<Vec<_>>()
                .join("\u{1e}");
            format!(
                "projection|{}|{}|{}|{}",
                rows.entity,
                rows.columns.join("\u{1f}"),
                rows.row_count,
                rendered_rows,
            )
        }
        SqlQueryResult::Grouped(rows) => {
            let rendered_rows = rows
                .rows
                .iter()
                .map(|row| row.join("\u{1f}"))
                .collect::<Vec<_>>()
                .join("\u{1e}");
            format!(
                "grouped|{}|{}|{}|{}",
                rows.entity,
                rows.columns.join("\u{1f}"),
                rows.row_count,
                rendered_rows,
            )
        }
        _ => result.render_lines().join("\n"),
    }
}

fn cursor_signature(result: &SqlQueryResult) -> Option<String> {
    match result {
        SqlQueryResult::Grouped(rows) => rows.next_cursor.clone(),
        _ => None,
    }
}

const fn fill_matrix_compile_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let compile = attribution.compile;

    sample.compile_local_instructions = attribution.compile_local_instructions;
    sample.compile_cache_key_local_instructions = compile.cache_key_local_instructions;
    sample.compile_cache_lookup_local_instructions = compile.cache_lookup_local_instructions;
    sample.compile_parse_local_instructions = compile.parse_local_instructions;
    sample.compile_parse_tokenize_local_instructions = compile.parse_tokenize_local_instructions;
    sample.compile_parse_select_local_instructions = compile.parse_select_local_instructions;
    sample.compile_parse_expr_local_instructions = compile.parse_expr_local_instructions;
    sample.compile_parse_predicate_local_instructions = compile.parse_predicate_local_instructions;
    sample.compile_aggregate_lane_check_local_instructions =
        compile.aggregate_lane_check_local_instructions;
    sample.compile_prepare_local_instructions = compile.prepare_local_instructions;
    sample.compile_lower_local_instructions = compile.lower_local_instructions;
    sample.compile_bind_local_instructions = compile.bind_local_instructions;
    sample.compile_cache_insert_local_instructions = compile.cache_insert_local_instructions;
}

const fn fill_matrix_execution_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let execution = attribution.execution;

    sample.execute_local_instructions = attribution.execute_local_instructions;
    sample.planner_local_instructions = execution.planner_local_instructions;
    sample.planner_schema_info_local_instructions =
        execution.planner_schema_info_local_instructions;
    sample.planner_prepare_local_instructions = execution.planner_prepare_local_instructions;
    sample.planner_cache_key_local_instructions = execution.planner_cache_key_local_instructions;
    sample.planner_cache_lookup_local_instructions =
        execution.planner_cache_lookup_local_instructions;
    sample.planner_plan_build_local_instructions = execution.planner_plan_build_local_instructions;
    sample.planner_cache_insert_local_instructions =
        execution.planner_cache_insert_local_instructions;
    sample.store_local_instructions = execution.store_local_instructions;
    sample.executor_local_instructions = execution.executor_local_instructions;
    sample.total_local_instructions = attribution.total_local_instructions;
}

const fn fill_matrix_grouped_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let Some(grouped) = attribution.grouped else {
        return;
    };

    sample.grouped_stream_local_instructions = grouped.stream_local_instructions;
    sample.grouped_fold_local_instructions = grouped.fold_local_instructions;
    sample.grouped_finalize_local_instructions = grouped.finalize_local_instructions;
}

fn fill_matrix_scalar_aggregate_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let Some(aggregate) = &attribution.scalar_aggregate else {
        return;
    };

    sample.scalar_aggregate_base_row_local_instructions = aggregate.base_row_local_instructions;
    sample.scalar_aggregate_reducer_fold_local_instructions =
        aggregate.reducer_fold_local_instructions;
    sample.scalar_aggregate_expression_evaluations = aggregate.expression_evaluations;
    sample.scalar_aggregate_filter_evaluations = aggregate.filter_evaluations;
    sample.scalar_aggregate_rows_ingested = aggregate.rows_ingested;
    sample.scalar_aggregate_terminal_count = aggregate.terminal_count;
    sample.scalar_aggregate_unique_input_expr_count = aggregate.unique_input_expr_count;
    sample.scalar_aggregate_unique_filter_expr_count = aggregate.unique_filter_expr_count;
    sample
        .scalar_aggregate_sink_mode
        .clone_from(&aggregate.sink_mode);
}

const fn fill_matrix_projection_path_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    if let Some(pure_covering) = attribution.pure_covering {
        sample.pure_covering_decode_local_instructions = pure_covering.decode_local_instructions;
        sample.pure_covering_row_assembly_local_instructions =
            pure_covering.row_assembly_local_instructions;
    }

    if let Some(hybrid) = attribution.hybrid_covering {
        sample.hybrid_covering_path_hits = hybrid.path_hits;
        sample.hybrid_covering_index_field_accesses = hybrid.index_field_accesses;
        sample.hybrid_covering_row_field_accesses = hybrid.row_field_accesses;
    }

    if let Some(direct) = attribution.direct_data_row {
        sample.direct_data_row_scan_local_instructions = direct.scan_local_instructions;
        sample.direct_data_row_key_stream_local_instructions = direct.key_stream_local_instructions;
        sample.direct_data_row_row_read_local_instructions = direct.row_read_local_instructions;
        sample.direct_data_row_key_encode_local_instructions = direct.key_encode_local_instructions;
        sample.direct_data_row_store_get_local_instructions = direct.store_get_local_instructions;
        sample.direct_data_row_order_window_local_instructions =
            direct.order_window_local_instructions;
        sample.direct_data_row_page_window_local_instructions =
            direct.page_window_local_instructions;
    }

    if let Some(kernel) = attribution.kernel_row {
        sample.kernel_row_scan_local_instructions = kernel.scan_local_instructions;
        sample.kernel_row_key_stream_local_instructions = kernel.key_stream_local_instructions;
        sample.kernel_row_row_read_local_instructions = kernel.row_read_local_instructions;
        sample.kernel_row_order_window_local_instructions = kernel.order_window_local_instructions;
        sample.kernel_row_page_window_local_instructions = kernel.page_window_local_instructions;
        sample.kernel_row_retained_layout_hits = kernel.retained_layout_hits;
        sample.kernel_row_retained_slot_values = kernel.retained_slot_values;
        sample.kernel_row_retained_octet_length_values = kernel.retained_octet_length_values;
    }
}

const fn fill_matrix_store_output_cache_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    sample.data_store_get_calls = attribution.store_get_calls;
    sample.index_store_get_calls = attribution.index_store_get_calls;
    sample.index_store_range_scan_calls = attribution.index_store_range_scan_calls;
    sample.index_store_entry_reads = attribution.index_store_entry_reads;
    sample.output_blob_values = attribution.output_blob.projected_values;
    sample.output_blob_bytes = attribution.output_blob.projected_bytes;
    sample.output_blob_hex_bytes = attribution.output_blob.rendered_hex_bytes;
    sample.sql_compiled_command_hits = attribution.cache.sql_compiled_command_hits;
    sample.sql_compiled_command_misses = attribution.cache.sql_compiled_command_misses;
    sample.shared_query_plan_hits = attribution.cache.shared_query_plan_hits;
    sample.shared_query_plan_misses = attribution.cache.shared_query_plan_misses;
}

fn matrix_failure_from_error(scenario: &MatrixScenario, err: Error) -> MatrixFailure {
    let diagnostic_code = err.diagnostic_code();
    MatrixFailure {
        key: scenario.key.clone(),
        source: scenario.source.label().to_string(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        route_family: failed_route_family(),
        route_outcome: failed_route_outcome(),
        route_reason: failed_route_reason(),
        code: err.code().raw(),
        diagnostic_code: diagnostic_code.error_code().raw(),
        diagnostic_label: diagnostic_label(diagnostic_code).to_string(),
        class: error_class_label(err.class()).to_string(),
        origin: format!("{:?}", err.origin()),
    }
}

fn failed_route_family() -> String {
    "failed_or_not_executed".to_string()
}

fn failed_route_outcome() -> String {
    "failed".to_string()
}

fn failed_route_reason() -> String {
    "scenario_failed".to_string()
}

const fn diagnostic_label(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "QueryValidate",
        DiagnosticCode::QueryIntent => "QueryIntent",
        DiagnosticCode::QueryPlan => "QueryPlan",
        DiagnosticCode::QueryAccessRequirement => "QueryAccessRequirement",
        DiagnosticCode::QueryUnorderedPagination => "QueryUnorderedPagination",
        DiagnosticCode::QueryInvalidContinuationCursor => "QueryInvalidContinuationCursor",
        DiagnosticCode::QueryNotFound => "QueryNotFound",
        DiagnosticCode::QueryNotUnique => "QueryNotUnique",
        DiagnosticCode::QueryNumericOverflow => "QueryNumericOverflow",
        DiagnosticCode::QueryNumericNotRepresentable => "QueryNumericNotRepresentable",
        DiagnosticCode::QueryUnknownAggregateTargetField => "QueryUnknownAggregateTargetField",
        DiagnosticCode::QueryUnsupportedProjection => "QueryUnsupportedProjection",
        DiagnosticCode::QueryResultShapeMismatch => "QueryResultShapeMismatch",
        DiagnosticCode::QueryReadAdmission => "QueryReadAdmission",
        DiagnosticCode::QueryUnsupportedSqlFeature => "QueryUnsupportedSqlFeature",
        DiagnosticCode::QuerySqlSurfaceMismatch => "QuerySqlSurfaceMismatch",
        DiagnosticCode::QuerySqlWriteBoundary => "QuerySqlWriteBoundary",
        DiagnosticCode::SchemaDdlAdmission => "SchemaDdlAdmission",
        DiagnosticCode::StoreNotFound => "StoreNotFound",
        DiagnosticCode::StoreCorruption => "StoreCorruption",
        DiagnosticCode::StoreInvariantViolation => "StoreInvariantViolation",
        DiagnosticCode::RuntimeCorruption => "RuntimeCorruption",
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => "RuntimeIncompatiblePersistedFormat",
        DiagnosticCode::RuntimeInvariantViolation => "RuntimeInvariantViolation",
        DiagnosticCode::RuntimeConflict => "RuntimeConflict",
        DiagnosticCode::RuntimeNotFound => "RuntimeNotFound",
        DiagnosticCode::RuntimeUnsupported => "RuntimeUnsupported",
        DiagnosticCode::RuntimeInternal => "RuntimeInternal",
    }
}

const fn error_class_label(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Query => "Query",
        ErrorClass::Corruption => "Corruption",
        ErrorClass::IncompatiblePersistedFormat => "IncompatiblePersistedFormat",
        ErrorClass::NotFound => "NotFound",
        ErrorClass::Internal => "Internal",
        ErrorClass::Conflict => "Conflict",
        ErrorClass::Unsupported => "Unsupported",
        ErrorClass::InvariantViolation => "InvariantViolation",
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("integration crate should live two levels below workspace root")
        .to_path_buf()
}

fn report_stem(mode: MatrixMode) -> PathBuf {
    env::var("ICYDB_SQL_PERF_MATRIX_OUT").map_or_else(
        |_| {
            workspace_root()
                .join("artifacts/perf-audit")
                .join(mode.default_report_stem())
        },
        PathBuf::from,
    )
}

fn write_matrix_reports(report: &MatrixReport) {
    let stem = report_stem(matrix_mode_from_report(report));
    if let Some(parent) = stem.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|err| panic!("matrix report directory should be created: {err}"));
    }

    let json_path = stem.with_extension("json");
    let md_path = stem.with_extension("md");
    let json = serde_json::to_string_pretty(report).expect("matrix report should serialize");
    fs::write(&json_path, json)
        .unwrap_or_else(|err| panic!("matrix JSON report should write: {err}"));
    fs::write(&md_path, matrix_markdown(report))
        .unwrap_or_else(|err| panic!("matrix Markdown report should write: {err}"));

    println!("matrix JSON: {}", json_path.display());
    println!("matrix Markdown: {}", md_path.display());
}

fn read_matrix_report(path: &Path) -> MatrixReport {
    let json = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("matrix JSON report should read {}: {err}", path.display()));
    serde_json::from_str(&json)
        .unwrap_or_else(|err| panic!("matrix JSON report should parse {}: {err}", path.display()))
}

fn write_matrix_delta_reports(delta: &MatrixDeltaReport, stem: &Path) {
    if let Some(parent) = stem.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|err| panic!("matrix delta directory should be created: {err}"));
    }

    let json_path = stem.with_extension("json");
    let md_path = stem.with_extension("md");
    let json = serde_json::to_string_pretty(delta).expect("matrix delta report should serialize");
    fs::write(&json_path, json)
        .unwrap_or_else(|err| panic!("matrix delta JSON report should write: {err}"));
    fs::write(&md_path, matrix_delta_markdown(delta))
        .unwrap_or_else(|err| panic!("matrix delta Markdown report should write: {err}"));

    println!("matrix delta JSON: {}", json_path.display());
    println!("matrix delta Markdown: {}", md_path.display());
}

fn matrix_delta_report(
    baseline_path: &Path,
    baseline: &MatrixReport,
    current_path: &Path,
    current: &MatrixReport,
    focused_targets: &BTreeSet<String>,
    expected_improvements: &BTreeSet<String>,
) -> MatrixDeltaReport {
    let baseline_successes = sample_map(baseline);
    let current_successes = sample_map(current);
    let baseline_failures = failure_map(baseline);
    let current_failures = failure_map(current);
    let mut keys = BTreeSet::new();
    keys.extend(baseline_successes.keys().map(|key| (*key).to_string()));
    keys.extend(current_successes.keys().map(|key| (*key).to_string()));
    keys.extend(baseline_failures.keys().map(|key| (*key).to_string()));
    keys.extend(current_failures.keys().map(|key| (*key).to_string()));

    let mut rows = keys
        .iter()
        .map(|key| {
            let before = report_entry(
                baseline_successes.get(key.as_str()).copied(),
                baseline_failures.get(key.as_str()).copied(),
            );
            let after = report_entry(
                current_successes.get(key.as_str()).copied(),
                current_failures.get(key.as_str()).copied(),
            );
            matrix_delta_row(
                key,
                before,
                after,
                focused_targets.contains(key),
                expected_improvements.contains(key),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.key.cmp(&right.key));

    let mut closeout_failures = matrix_delta_closeout_failures(&rows);
    append_canister_profile_closeout_failures(&mut closeout_failures, baseline, current);
    append_focused_target_closeout_failures(&mut closeout_failures, &rows, focused_targets);
    append_expected_improvement_closeout_failures(
        &mut closeout_failures,
        &rows,
        expected_improvements,
    );

    MatrixDeltaReport {
        baseline_path: baseline_path.display().to_string(),
        current_path: current_path.display().to_string(),
        baseline_canister_wasm_profile: baseline.canister_wasm_profile.clone(),
        current_canister_wasm_profile: current.canister_wasm_profile.clone(),
        baseline_scenario_count: baseline.samples.len() + baseline.failures.len(),
        current_scenario_count: current.samples.len() + current.failures.len(),
        union_scenario_count: rows.len(),
        common_successful_scenario_count: rows
            .iter()
            .filter(|row| row.status_class == "common_success")
            .count(),
        improved_scenario_count: rows
            .iter()
            .filter(|row| row.status_class == "common_success")
            .filter(|row| {
                row.total_local_instructions
                    .delta
                    .is_some_and(|delta| delta < 0)
            })
            .count(),
        regressed_scenario_count: rows
            .iter()
            .filter(|row| row.status_class == "common_success")
            .filter(|row| {
                row.total_local_instructions
                    .delta
                    .is_some_and(|delta| delta > 0)
            })
            .count(),
        neutral_scenario_count: rows
            .iter()
            .filter(|row| row.status_class == "common_success")
            .filter(|row| row.total_local_instructions.delta == Some(0))
            .count(),
        new_failure_count: rows
            .iter()
            .filter(|row| row.status_class == "new_failure")
            .count(),
        resolved_failure_count: rows
            .iter()
            .filter(|row| row.status_class == "resolved_failure")
            .count(),
        common_failure_count: rows
            .iter()
            .filter(|row| row.status_class == "common_failure")
            .count(),
        focused_target_count: focused_targets.len(),
        expected_improvement_count: expected_improvements.len(),
        closeout_failures,
        route_family_aggregates: route_delta_aggregates(&rows, RouteAggregateKind::Family),
        route_outcome_aggregates: route_delta_aggregates(&rows, RouteAggregateKind::Outcome),
        route_pair_aggregates: route_delta_aggregates(&rows, RouteAggregateKind::Pair),
        rows,
    }
}

fn append_canister_profile_closeout_failures(
    closeout_failures: &mut Vec<String>,
    baseline: &MatrixReport,
    current: &MatrixReport,
) {
    if baseline.canister_wasm_profile.is_empty() {
        closeout_failures.push("baseline report lacks canister_wasm_profile metadata".to_string());
    }
    if current.canister_wasm_profile.is_empty() {
        closeout_failures.push("current report lacks canister_wasm_profile metadata".to_string());
    }
    if !baseline.canister_wasm_profile.is_empty()
        && !current.canister_wasm_profile.is_empty()
        && baseline.canister_wasm_profile != current.canister_wasm_profile
    {
        closeout_failures.push(format!(
            "canister wasm profile mismatch: baseline `{}`, current `{}`",
            baseline.canister_wasm_profile, current.canister_wasm_profile,
        ));
    }
}

fn append_focused_target_closeout_failures(
    closeout_failures: &mut Vec<String>,
    rows: &[MatrixDeltaRow],
    focused_targets: &BTreeSet<String>,
) {
    for key in focused_targets {
        match rows.iter().find(|row| &row.key == key) {
            Some(row) if row.before_status == "missing" || row.after_status == "missing" => {
                closeout_failures
                    .push(format!("focused target `{key}` lacks before or after data"));
            }
            Some(_) => {}
            None => {
                closeout_failures.push(format!(
                    "focused target `{key}` is absent from both reports"
                ));
            }
        }
    }
}

fn append_expected_improvement_closeout_failures(
    closeout_failures: &mut Vec<String>,
    rows: &[MatrixDeltaRow],
    expected_improvements: &BTreeSet<String>,
) {
    for key in expected_improvements {
        match rows.iter().find(|row| &row.key == key) {
            Some(row) if row.status_class != "common_success" => {
                closeout_failures.push(format!(
                    "expected improvement target `{key}` is not a common successful scenario"
                ));
            }
            Some(row)
                if row
                    .total_local_instructions
                    .delta
                    .is_none_or(|delta| delta >= 0) =>
            {
                closeout_failures.push(format!(
                    "expected improvement target `{key}` did not reduce total instructions"
                ));
            }
            Some(_) => {}
            None => {
                closeout_failures.push(format!(
                    "expected improvement target `{key}` is absent from both reports"
                ));
            }
        }
    }
}

fn sample_map(report: &MatrixReport) -> BTreeMap<&str, &MatrixSample> {
    report
        .samples
        .iter()
        .map(|sample| (sample.key.as_str(), sample))
        .collect()
}

fn failure_map(report: &MatrixReport) -> BTreeMap<&str, &MatrixFailure> {
    report
        .failures
        .iter()
        .map(|failure| (failure.key.as_str(), failure))
        .collect()
}

#[derive(Clone, Copy)]
enum ReportEntry<'a> {
    Success(&'a MatrixSample),
    Failure(&'a MatrixFailure),
    Missing,
}

const fn report_entry<'a>(
    success: Option<&'a MatrixSample>,
    failure: Option<&'a MatrixFailure>,
) -> ReportEntry<'a> {
    if let Some(sample) = success {
        ReportEntry::Success(sample)
    } else if let Some(failure) = failure {
        ReportEntry::Failure(failure)
    } else {
        ReportEntry::Missing
    }
}

fn matrix_delta_row(
    key: &str,
    before: ReportEntry<'_>,
    after: ReportEntry<'_>,
    focused_target: bool,
    expected_to_improve: bool,
) -> MatrixDeltaRow {
    let before_sample = entry_sample(before);
    let after_sample = entry_sample(after);
    let before_route = entry_route(before);
    let after_route = entry_route(after);
    let before_result_signature = before_sample.and_then(|sample| sample.result_signature.clone());
    let after_result_signature = after_sample.and_then(|sample| sample.result_signature.clone());
    let before_cursor_signature = before_sample.and_then(|sample| sample.cursor_signature.clone());
    let after_cursor_signature = after_sample.and_then(|sample| sample.cursor_signature.clone());
    let before_order_by_idx_hint =
        before_sample.and_then(|sample| sample.order_by_idx_hint.clone());
    let after_order_by_idx_hint = after_sample.and_then(|sample| sample.order_by_idx_hint.clone());
    let before_limit_stop_after = before_sample.map(|sample| sample.limit_stop_after.clone());
    let after_limit_stop_after = after_sample.map(|sample| sample.limit_stop_after.clone());
    let (before_route_family, before_route_outcome, before_route_reason) = before_route
        .map_or((None, None, None), |(family, outcome, reason)| {
            (Some(family), Some(outcome), reason)
        });
    let (after_route_family, after_route_outcome, after_route_reason) = after_route
        .map_or((None, None, None), |(family, outcome, reason)| {
            (Some(family), Some(outcome), reason)
        });

    MatrixDeltaRow {
        key: key.to_string(),
        before_status: entry_status(before).to_string(),
        after_status: entry_status(after).to_string(),
        status_class: status_class(before, after).to_string(),
        total_local_instructions: metric_delta(
            before_sample.map(|sample| sample.total_local_instructions),
            after_sample.map(|sample| sample.total_local_instructions),
        ),
        compile_local_instructions: metric_delta(
            before_sample.map(|sample| sample.compile_local_instructions),
            after_sample.map(|sample| sample.compile_local_instructions),
        ),
        execute_local_instructions: metric_delta(
            before_sample.map(|sample| sample.execute_local_instructions),
            after_sample.map(|sample| sample.execute_local_instructions),
        ),
        planner_local_instructions: metric_delta(
            before_sample.map(|sample| sample.planner_local_instructions),
            after_sample.map(|sample| sample.planner_local_instructions),
        ),
        executor_local_instructions: metric_delta(
            before_sample.map(|sample| sample.executor_local_instructions),
            after_sample.map(|sample| sample.executor_local_instructions),
        ),
        store_local_instructions: metric_delta(
            before_sample.map(|sample| sample.store_local_instructions),
            after_sample.map(|sample| sample.store_local_instructions),
        ),
        data_store_get_calls: metric_delta(
            before_sample.map(|sample| sample.data_store_get_calls),
            after_sample.map(|sample| sample.data_store_get_calls),
        ),
        index_store_range_scan_calls: metric_delta(
            before_sample.map(|sample| sample.index_store_range_scan_calls),
            after_sample.map(|sample| sample.index_store_range_scan_calls),
        ),
        index_store_entry_reads: metric_delta(
            before_sample.map(|sample| sample.index_store_entry_reads),
            after_sample.map(|sample| sample.index_store_entry_reads),
        ),
        rows_returned: metric_delta(
            before_sample.and_then(|sample| u64::try_from(sample.outcome.row_count).ok()),
            after_sample.and_then(|sample| u64::try_from(sample.outcome.row_count).ok()),
        ),
        before_route_family,
        after_route_family,
        before_route_outcome,
        after_route_outcome,
        before_route_reason,
        after_route_reason,
        before_order_by_idx_hint,
        after_order_by_idx_hint,
        before_limit_stop_after,
        after_limit_stop_after,
        signature_changes: MatrixDeltaSignatureChanges {
            result_signature_changed: signatures_changed(
                before_result_signature.as_ref(),
                after_result_signature.as_ref(),
            ),
            cursor_signature_changed: signatures_changed(
                before_cursor_signature.as_ref(),
                after_cursor_signature.as_ref(),
            ),
        },
        before_result_signature,
        after_result_signature,
        before_cursor_signature,
        after_cursor_signature,
        result_row_count_before: before_sample.map(|sample| sample.outcome.row_count),
        result_row_count_after: after_sample.map(|sample| sample.outcome.row_count),
        target_flags: MatrixDeltaTargetFlags {
            focused_target,
            expected_to_improve,
        },
    }
}

const fn entry_sample(entry: ReportEntry<'_>) -> Option<&MatrixSample> {
    match entry {
        ReportEntry::Success(sample) => Some(sample),
        ReportEntry::Failure(_) | ReportEntry::Missing => None,
    }
}

fn entry_route(entry: ReportEntry<'_>) -> Option<(String, String, Option<String>)> {
    match entry {
        ReportEntry::Success(sample) => {
            let route = route_for_sample(sample);
            Some((route.0, route.1, route.2))
        }
        ReportEntry::Failure(failure) => Some((
            non_empty_or_default(&failure.route_family, failed_route_family),
            non_empty_or_default(&failure.route_outcome, failed_route_outcome),
            Some(non_empty_or_default(
                &failure.route_reason,
                failed_route_reason,
            )),
        )),
        ReportEntry::Missing => None,
    }
}

fn route_for_sample(sample: &MatrixSample) -> (String, String, Option<String>) {
    if !sample.route_family.is_empty() && !sample.route_outcome.is_empty() {
        return (
            sample.route_family.clone(),
            sample.route_outcome.clone(),
            sample.route_reason.clone(),
        );
    }

    let route = route_classification_for_sample(sample);
    (
        route.family.to_string(),
        route.outcome.to_string(),
        route.reason.map(str::to_string),
    )
}

fn non_empty_or_default(value: &str, default: fn() -> String) -> String {
    if value.is_empty() {
        default()
    } else {
        value.to_string()
    }
}

const fn entry_status(entry: ReportEntry<'_>) -> &'static str {
    match entry {
        ReportEntry::Success(_) => "success",
        ReportEntry::Failure(_) => "failure",
        ReportEntry::Missing => "missing",
    }
}

const fn status_class(before: ReportEntry<'_>, after: ReportEntry<'_>) -> &'static str {
    match (before, after) {
        (ReportEntry::Success(_), ReportEntry::Success(_)) => "common_success",
        (ReportEntry::Success(_), ReportEntry::Failure(_)) => "new_failure",
        (ReportEntry::Failure(_), ReportEntry::Success(_)) => "resolved_failure",
        (ReportEntry::Failure(_), ReportEntry::Failure(_)) => "common_failure",
        (ReportEntry::Success(_), ReportEntry::Missing) => "before_only_success",
        (ReportEntry::Missing, ReportEntry::Success(_)) => "after_only_success",
        _ => "skipped_or_missing",
    }
}

fn metric_delta(before: Option<u64>, after: Option<u64>) -> MetricDelta {
    let delta = before.zip(after).map(|(before, after)| {
        i64::try_from(after).unwrap_or(i64::MAX) - i64::try_from(before).unwrap_or(i64::MAX)
    });
    let delta_percent_bp = before
        .zip(after)
        .and_then(|(before, after)| percent_delta_bp(before, after));

    MetricDelta {
        before,
        after,
        delta,
        delta_percent_bp,
    }
}

fn percent_delta_bp(before: u64, after: u64) -> Option<i64> {
    if before == 0 {
        return None;
    }

    let before = i128::from(before);
    let after = i128::from(after);
    i64::try_from((after - before).saturating_mul(10_000) / before).ok()
}

fn signatures_changed(before: Option<&String>, after: Option<&String>) -> bool {
    matches!((before, after), (Some(before), Some(after)) if before != after)
}

fn matrix_delta_closeout_failures(rows: &[MatrixDeltaRow]) -> Vec<String> {
    let mut failures = Vec::new();
    for row in rows {
        if row.status_class == "common_success" {
            if row.total_local_instructions.before.is_none()
                || row.total_local_instructions.after.is_none()
            {
                failures.push(format!(
                    "common-success scenario `{}` lacks instruction totals",
                    row.key
                ));
            }
            if row.before_route_family.is_none()
                || row.after_route_family.is_none()
                || row.before_route_outcome.is_none()
                || row.after_route_outcome.is_none()
            {
                failures.push(format!(
                    "common-success scenario `{}` lacks route family/outcome",
                    row.key
                ));
            }
            if row.before_result_signature.is_none() || row.after_result_signature.is_none() {
                failures.push(format!(
                    "common-success scenario `{}` lacks result signatures",
                    row.key
                ));
            }
            if row.signature_changes.result_signature_changed {
                failures.push(format!(
                    "common-success scenario `{}` changed result signature without rationale",
                    row.key
                ));
            }
            if row.signature_changes.cursor_signature_changed {
                failures.push(format!(
                    "common-success scenario `{}` changed cursor signature without rationale",
                    row.key
                ));
            }
            if row
                .total_local_instructions
                .delta
                .is_some_and(|delta| delta >= 100_000)
                && row
                    .total_local_instructions
                    .delta_percent_bp
                    .is_some_and(|percent| percent >= 1_000)
            {
                failures.push(format!(
                    "common-success scenario `{}` regressed >=10% and >=100k instructions without rationale",
                    row.key
                ));
            }
        }
        if row.status_class == "new_failure"
            && (row.after_route_family.is_none() || row.after_route_outcome.is_none())
        {
            failures.push(format!(
                "new failure `{}` lacks route classification",
                row.key
            ));
        }
    }

    failures
}

#[derive(Clone, Copy)]
enum RouteAggregateKind {
    Family,
    Outcome,
    Pair,
}

fn route_delta_aggregates(
    rows: &[MatrixDeltaRow],
    kind: RouteAggregateKind,
) -> Vec<MatrixDeltaRouteAggregate> {
    let mut aggregates =
        BTreeMap::<(Option<String>, Option<String>), MatrixDeltaRouteAggregate>::new();
    for row in rows
        .iter()
        .filter(|row| row.status_class == "common_success")
    {
        let family = row
            .after_route_family
            .clone()
            .or_else(|| row.before_route_family.clone());
        let outcome = row
            .after_route_outcome
            .clone()
            .or_else(|| row.before_route_outcome.clone());
        let key = match kind {
            RouteAggregateKind::Family => (family, None),
            RouteAggregateKind::Outcome => (None, outcome),
            RouteAggregateKind::Pair => (family, outcome),
        };
        let aggregate =
            aggregates
                .entry(key.clone())
                .or_insert_with(|| MatrixDeltaRouteAggregate {
                    route_family: key.0.clone(),
                    route_outcome: key.1.clone(),
                    scenario_count: 0,
                    total_delta: 0,
                });
        aggregate.scenario_count += 1;
        aggregate.total_delta += row.total_local_instructions.delta.unwrap_or_default();
    }

    let mut values = aggregates.into_values().collect::<Vec<_>>();
    values.sort_by_key(|aggregate| Reverse(aggregate.total_delta.abs()));
    values
}

fn matrix_delta_markdown(delta: &MatrixDeltaReport) -> String {
    let mut output = String::new();
    append_delta_heading(&mut output, delta);
    append_delta_closeout_failures(&mut output, delta);
    append_delta_aggregate_tables(&mut output, delta);
    append_delta_ranked_tables(&mut output, delta);
    append_delta_focus_and_change_tables(&mut output, delta);

    output
}

fn append_delta_heading(output: &mut String, delta: &MatrixDeltaReport) {
    writeln!(output, "# SQL Perf Matrix Delta").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(output, "- baseline: `{}`", delta.baseline_path)
        .expect("write to string should succeed");
    writeln!(output, "- current: `{}`", delta.current_path)
        .expect("write to string should succeed");
    writeln!(
        output,
        "- canister wasm profile: baseline `{}`, current `{}`",
        delta.baseline_canister_wasm_profile, delta.current_canister_wasm_profile,
    )
    .expect("write to string should succeed");
    writeln!(output, "- union scenarios: {}", delta.union_scenario_count)
        .expect("write to string should succeed");
    writeln!(
        output,
        "- common successful scenarios: {}",
        delta.common_successful_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- improved scenarios: {}",
        delta.improved_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- regressed scenarios: {}",
        delta.regressed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- neutral scenarios: {}",
        delta.neutral_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(output, "- new failures: {}", delta.new_failure_count)
        .expect("write to string should succeed");
    writeln!(
        output,
        "- resolved failures: {}",
        delta.resolved_failure_count
    )
    .expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
}

fn append_delta_aggregate_tables(output: &mut String, delta: &MatrixDeltaReport) {
    append_delta_route_aggregate_table(
        output,
        "Route Family Delta",
        &delta.route_family_aggregates,
    );
    append_delta_route_aggregate_table(
        output,
        "Route Outcome Delta",
        &delta.route_outcome_aggregates,
    );
    append_delta_route_aggregate_table(
        output,
        "Route Family/Outcome Delta",
        &delta.route_pair_aggregates,
    );
}

fn append_delta_ranked_tables(output: &mut String, delta: &MatrixDeltaReport) {
    append_delta_rows_table(
        output,
        "Top 50 Improvements By Absolute Instructions",
        ranked_delta_rows(delta, DeltaRank::ImprovementAbsolute),
    );
    append_delta_rows_table(
        output,
        "Top 50 Regressions By Absolute Instructions",
        ranked_delta_rows(delta, DeltaRank::RegressionAbsolute),
    );
    append_delta_rows_table(
        output,
        "Top 50 Improvements By Percent",
        ranked_delta_rows(delta, DeltaRank::ImprovementPercent),
    );
    append_delta_rows_table(
        output,
        "Top 50 Regressions By Percent",
        ranked_delta_rows(delta, DeltaRank::RegressionPercent),
    );
}

fn append_delta_focus_and_change_tables(output: &mut String, delta: &MatrixDeltaReport) {
    append_delta_rows_table(
        output,
        "Focused Target Scenarios",
        delta
            .rows
            .iter()
            .filter(|row| row.target_flags.focused_target)
            .collect::<Vec<_>>(),
    );
    append_delta_rows_table(
        output,
        "Route Fact Changes",
        delta
            .rows
            .iter()
            .filter(|row| {
                row.before_route_family != row.after_route_family
                    || row.before_route_outcome != row.after_route_outcome
                    || row.before_route_reason != row.after_route_reason
                    || row.before_order_by_idx_hint != row.after_order_by_idx_hint
                    || row.before_limit_stop_after != row.after_limit_stop_after
            })
            .collect::<Vec<_>>(),
    );
    append_delta_rows_table(
        output,
        "Result Or Status Changes",
        delta
            .rows
            .iter()
            .filter(|row| {
                row.signature_changes.result_signature_changed
                    || row.signature_changes.cursor_signature_changed
                    || row.result_row_count_before != row.result_row_count_after
                    || row.before_status != row.after_status
            })
            .collect::<Vec<_>>(),
    );
}

fn append_delta_closeout_failures(output: &mut String, delta: &MatrixDeltaReport) {
    if delta.closeout_failures.is_empty() {
        writeln!(output, "## Closeout Gate").expect("write to string should succeed");
        writeln!(output).expect("write to string should succeed");
        writeln!(output, "- PASS").expect("write to string should succeed");
        writeln!(output).expect("write to string should succeed");
        return;
    }

    writeln!(output, "## Closeout Gate").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(output, "- FAIL").expect("write to string should succeed");
    for failure in &delta.closeout_failures {
        writeln!(output, "- {failure}").expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_delta_route_aggregate_table(
    output: &mut String,
    title: &str,
    aggregates: &[MatrixDeltaRouteAggregate],
) {
    if aggregates.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Route Family | Route Outcome | Scenarios | Total Delta |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|").expect("write to string should succeed");
    for aggregate in aggregates.iter().take(50) {
        writeln!(
            output,
            "| {} | {} | {} | {} |",
            aggregate.route_family.as_deref().unwrap_or(""),
            aggregate.route_outcome.as_deref().unwrap_or(""),
            aggregate.scenario_count,
            signed_i64(aggregate.total_delta),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

#[derive(Clone, Copy)]
enum DeltaRank {
    ImprovementAbsolute,
    RegressionAbsolute,
    ImprovementPercent,
    RegressionPercent,
}

fn ranked_delta_rows(delta: &MatrixDeltaReport, rank: DeltaRank) -> Vec<&MatrixDeltaRow> {
    let mut rows = delta
        .rows
        .iter()
        .filter(|row| row.status_class == "common_success")
        .filter(|row| row.total_local_instructions.delta != Some(0))
        .collect::<Vec<_>>();
    match rank {
        DeltaRank::ImprovementAbsolute => {
            rows.retain(|row| {
                row.total_local_instructions
                    .delta
                    .is_some_and(|delta| delta < 0)
            });
            rows.sort_by_key(|row| row.total_local_instructions.delta.unwrap_or_default());
        }
        DeltaRank::RegressionAbsolute => {
            rows.retain(|row| {
                row.total_local_instructions
                    .delta
                    .is_some_and(|delta| delta > 0)
            });
            rows.sort_by_key(|row| Reverse(row.total_local_instructions.delta.unwrap_or_default()));
        }
        DeltaRank::ImprovementPercent => {
            rows.retain(|row| {
                row.total_local_instructions
                    .before
                    .is_some_and(|before| before >= 100_000)
                    && row
                        .total_local_instructions
                        .delta
                        .is_some_and(|delta| delta < 0)
            });
            rows.sort_by_key(|row| {
                row.total_local_instructions
                    .delta_percent_bp
                    .unwrap_or_default()
            });
        }
        DeltaRank::RegressionPercent => {
            rows.retain(|row| {
                row.total_local_instructions
                    .before
                    .is_some_and(|before| before >= 100_000)
                    && row
                        .total_local_instructions
                        .delta
                        .is_some_and(|delta| delta > 0)
            });
            rows.sort_by_key(|row| {
                Reverse(
                    row.total_local_instructions
                        .delta_percent_bp
                        .unwrap_or_default(),
                )
            });
        }
    }
    rows.truncate(50);
    rows
}

fn append_delta_rows_table(output: &mut String, title: &str, rows: Vec<&MatrixDeltaRow>) {
    if rows.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Status | Total Delta | Total %bp | Compile Delta | Execute Delta | Store Delta | Executor Delta | data_store.get Delta | index ranges Delta | index entries Delta | Rows Delta | Route Family | Route Outcome | Order Hint | Limit Stop | Result Changed | Cursor Changed |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|---|"
    )
    .expect("write to string should succeed");
    for row in rows {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} -> {} | {} -> {} | {} | {} | {} | {} |",
            row.key,
            row.status_class,
            metric_delta_text(&row.total_local_instructions),
            row.total_local_instructions
                .delta_percent_bp
                .map_or_else(|| "n/a".to_string(), signed_i64),
            metric_delta_text(&row.compile_local_instructions),
            metric_delta_text(&row.execute_local_instructions),
            metric_delta_text(&row.store_local_instructions),
            metric_delta_text(&row.executor_local_instructions),
            metric_delta_text(&row.data_store_get_calls),
            metric_delta_text(&row.index_store_range_scan_calls),
            metric_delta_text(&row.index_store_entry_reads),
            metric_delta_text(&row.rows_returned),
            row.before_route_family.as_deref().unwrap_or(""),
            row.after_route_family.as_deref().unwrap_or(""),
            row.before_route_outcome.as_deref().unwrap_or(""),
            row.after_route_outcome.as_deref().unwrap_or(""),
            option_string_transition(
                row.before_order_by_idx_hint.as_deref(),
                row.after_order_by_idx_hint.as_deref(),
            ),
            limit_stop_after_transition(
                row.before_limit_stop_after.as_ref(),
                row.after_limit_stop_after.as_ref(),
            ),
            row.signature_changes.result_signature_changed,
            row.signature_changes.cursor_signature_changed,
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn metric_delta_text(metric: &MetricDelta) -> String {
    metric.delta.map_or_else(|| "n/a".to_string(), signed_i64)
}

fn option_string_transition(before: Option<&str>, after: Option<&str>) -> String {
    format!("{} -> {}", before.unwrap_or(""), after.unwrap_or(""))
}

fn limit_stop_after_transition(
    before: Option<&MatrixLimitStopAfter>,
    after: Option<&MatrixLimitStopAfter>,
) -> String {
    format!(
        "{} -> {}",
        limit_stop_after_text(before),
        limit_stop_after_text(after)
    )
}

fn limit_stop_after_text(value: Option<&MatrixLimitStopAfter>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if value.possible {
        return format!(
            "possible(limit={},lookahead={},matches={},index_entries={})",
            value
                .returned_limit
                .map_or_else(|| "n/a".to_string(), |limit| limit.to_string()),
            value.lookahead,
            value
                .stopped_after_matches
                .map_or_else(|| "n/a".to_string(), |count| count.to_string()),
            value
                .stopped_after_index_entries
                .map_or_else(|| "n/a".to_string(), |count| count.to_string()),
        );
    }

    format!(
        "disabled({})",
        value.disabled_reason.as_deref().unwrap_or("unknown")
    )
}

fn signed_i64(value: i64) -> String {
    if value >= 0 {
        format!("+{value}")
    } else {
        value.to_string()
    }
}

fn matrix_markdown(report: &MatrixReport) -> String {
    let mut output = String::new();
    let mode = matrix_mode_from_report(report);
    writeln!(output, "# {}", mode.title()).expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(output, "- matrix mode: {}", report.matrix_mode)
        .expect("write to string should succeed");
    if !report.canister_wasm_profile.is_empty() {
        writeln!(
            output,
            "- canister wasm profile: {}",
            report.canister_wasm_profile,
        )
        .expect("write to string should succeed");
    }
    writeln!(
        output,
        "- generated scenarios: {}",
        report.generated_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- executed scenarios: {}",
        report.executed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- failed scenarios: {}",
        report.failed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(output, "- matrix limit: {}", report.matrix_limit)
        .expect("write to string should succeed");
    if let Some(filter) = &report.scenario_key_filter {
        writeln!(output, "- scenario key filter: {filter}")
            .expect("write to string should succeed");
    }
    if let Some(seed) = report.random_seed {
        writeln!(output, "- random seed: {seed}").expect("write to string should succeed");
        writeln!(output, "- random cases: {}", report.random_case_count)
            .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");

    append_instruction_hotspot_tables(&mut output, &report.samples);
    append_storage_backend_comparison_table(&mut output, &report.samples);
    append_route_classification_summary(&mut output, report);
    append_failure_table(&mut output, &report.failures);

    output
}

#[derive(Default)]
struct MatrixRouteSummary {
    scenario_count: usize,
    total_local_instructions: u64,
    data_store_get_calls: u64,
    index_store_range_scan_calls: u64,
    index_store_entry_reads: u64,
    rows_returned: usize,
}

fn append_route_classification_summary(output: &mut String, report: &MatrixReport) {
    let mut summaries = BTreeMap::<(String, String, String), MatrixRouteSummary>::new();
    for sample in &report.samples {
        let (family, outcome, reason) = route_for_sample(sample);
        let key = (family, outcome, reason.unwrap_or_default());
        let summary = summaries.entry(key).or_default();
        summary.scenario_count += 1;
        summary.total_local_instructions = summary
            .total_local_instructions
            .saturating_add(sample.total_local_instructions);
        summary.data_store_get_calls = summary
            .data_store_get_calls
            .saturating_add(sample.data_store_get_calls);
        summary.index_store_range_scan_calls = summary
            .index_store_range_scan_calls
            .saturating_add(sample.index_store_range_scan_calls);
        summary.index_store_entry_reads = summary
            .index_store_entry_reads
            .saturating_add(sample.index_store_entry_reads);
        summary.rows_returned = summary
            .rows_returned
            .saturating_add(sample.outcome.row_count);
    }
    for failure in &report.failures {
        let key = (
            non_empty_or_default(&failure.route_family, failed_route_family),
            non_empty_or_default(&failure.route_outcome, failed_route_outcome),
            non_empty_or_default(&failure.route_reason, failed_route_reason),
        );
        summaries.entry(key).or_default().scenario_count += 1;
    }
    if summaries.is_empty() {
        return;
    }

    writeln!(output, "## Route Classification Summary").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Route Family | Route Outcome | Reason | Scenarios | Total Instructions | data_store.get | index ranges | index entries | Rows |",
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---|---:|---:|---:|---:|---:|---:|")
        .expect("write to string should succeed");
    for ((family, outcome, reason), summary) in summaries {
        writeln!(
            output,
            "| {family} | {outcome} | {reason} | {} | {} | {} | {} | {} | {} |",
            summary.scenario_count,
            summary.total_local_instructions,
            summary.data_store_get_calls,
            summary.index_store_range_scan_calls,
            summary.index_store_entry_reads,
            summary.rows_returned,
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_instruction_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_ranked_table(
        output,
        "Top Total Instructions",
        ranked_by(samples, |sample| sample.total_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Compile Instructions",
        ranked_by(samples, |sample| sample.compile_local_instructions),
    );
    append_compile_phase_table(
        output,
        ranked_by(samples, |sample| sample.compile_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Execute Instructions",
        ranked_by(samples, |sample| sample.execute_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Store Instructions",
        ranked_by(samples, |sample| sample.store_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Executor Instructions",
        ranked_by(samples, |sample| sample.executor_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Data Store Gets",
        ranked_by(samples, |sample| sample.data_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Index Store Gets",
        ranked_by(samples, |sample| sample.index_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Index Store Range Scans",
        ranked_by(samples, |sample| sample.index_store_range_scan_calls),
    );
    append_ranked_table(
        output,
        "Top Index Store Entry Reads",
        ranked_by(samples, |sample| sample.index_store_entry_reads),
    );
    append_blob_output_table(
        output,
        "Top Blob Output Bytes",
        ranked_by(samples, |sample| sample.output_blob_bytes),
    );
    append_pure_covering_hotspot_tables(output, samples);
    append_hybrid_covering_hotspot_tables(output, samples);
    append_direct_data_row_hotspot_tables(output, samples);
    append_kernel_row_hotspot_tables(output, samples);
    append_main_fixture_hotspot_tables(output, samples);
}

fn append_pure_covering_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_pure_covering_table(
        output,
        "Top Pure Covering Decode Instructions",
        ranked_by(samples, |sample| {
            sample.pure_covering_decode_local_instructions
        }),
        |sample| sample.pure_covering_decode_local_instructions,
    );
    append_pure_covering_table(
        output,
        "Top Pure Covering Row Assembly Instructions",
        ranked_by(samples, |sample| {
            sample.pure_covering_row_assembly_local_instructions
        }),
        |sample| sample.pure_covering_row_assembly_local_instructions,
    );
}

fn append_hybrid_covering_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_hybrid_covering_table(
        output,
        "Top Hybrid Covering Row Field Accesses",
        ranked_by(samples, |sample| sample.hybrid_covering_row_field_accesses),
        |sample| sample.hybrid_covering_row_field_accesses,
    );
    append_hybrid_covering_table(
        output,
        "Top Hybrid Covering Index Field Accesses",
        ranked_by(samples, |sample| {
            sample.hybrid_covering_index_field_accesses
        }),
        |sample| sample.hybrid_covering_index_field_accesses,
    );
}

fn append_direct_data_row_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_direct_data_row_table(
        output,
        "Top Direct Data-Row Scan Instructions",
        ranked_by(samples, |sample| {
            sample.direct_data_row_scan_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Direct Data-Row Row-Read Instructions",
        ranked_by(samples, |sample| {
            sample.direct_data_row_row_read_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Direct Data-Row Order-Window Instructions",
        ranked_by(samples, |sample| {
            sample.direct_data_row_order_window_local_instructions
        }),
    );
}

fn append_kernel_row_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_kernel_row_table(
        output,
        "Top Kernel Row Scan Instructions",
        ranked_by(samples, |sample| sample.kernel_row_scan_local_instructions),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Row-Read Instructions",
        ranked_by(samples, |sample| {
            sample.kernel_row_row_read_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Order-Window Instructions",
        ranked_by(samples, |sample| {
            sample.kernel_row_order_window_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Retained Layout Hits",
        ranked_by(samples, |sample| sample.kernel_row_retained_layout_hits),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Retained Slot Values",
        ranked_by(samples, |sample| sample.kernel_row_retained_slot_values),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Retained Length Values",
        ranked_by(samples, |sample| {
            sample.kernel_row_retained_octet_length_values
        }),
    );
}

fn append_main_fixture_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    if !samples
        .iter()
        .any(|sample| !sample_is_storage_mirror(sample))
    {
        return;
    }

    append_ranked_table(
        output,
        "Top Main Fixture Total Instructions",
        ranked_main_fixture_by(samples, |sample| sample.total_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Data Store Gets",
        ranked_main_fixture_by(samples, |sample| sample.data_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Index Store Gets",
        ranked_main_fixture_by(samples, |sample| sample.index_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Index Store Range Scans",
        ranked_main_fixture_by(samples, |sample| sample.index_store_range_scan_calls),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Index Store Entry Reads",
        ranked_main_fixture_by(samples, |sample| sample.index_store_entry_reads),
    );
    append_main_fixture_covering_hotspot_tables(output, samples);
    append_main_fixture_execution_hotspot_tables(output, samples);
}

fn append_main_fixture_covering_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_pure_covering_table(
        output,
        "Top Main Fixture Pure Covering Decode Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.pure_covering_decode_local_instructions
        }),
        |sample| sample.pure_covering_decode_local_instructions,
    );
    append_pure_covering_table(
        output,
        "Top Main Fixture Pure Covering Row Assembly Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.pure_covering_row_assembly_local_instructions
        }),
        |sample| sample.pure_covering_row_assembly_local_instructions,
    );
    append_hybrid_covering_table(
        output,
        "Top Main Fixture Hybrid Covering Row Field Accesses",
        ranked_main_fixture_by(samples, |sample| sample.hybrid_covering_row_field_accesses),
        |sample| sample.hybrid_covering_row_field_accesses,
    );
    append_hybrid_covering_table(
        output,
        "Top Main Fixture Hybrid Covering Index Field Accesses",
        ranked_main_fixture_by(samples, |sample| {
            sample.hybrid_covering_index_field_accesses
        }),
        |sample| sample.hybrid_covering_index_field_accesses,
    );
}

fn append_main_fixture_execution_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_direct_data_row_table(
        output,
        "Top Main Fixture Direct Data-Row Scan Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.direct_data_row_scan_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Main Fixture Direct Data-Row Row-Read Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.direct_data_row_row_read_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Main Fixture Direct Data-Row Order-Window Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.direct_data_row_order_window_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Scan Instructions",
        ranked_main_fixture_by(samples, |sample| sample.kernel_row_scan_local_instructions),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Row-Read Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.kernel_row_row_read_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Order-Window Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.kernel_row_order_window_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Retained Layout Hits",
        ranked_main_fixture_by(samples, |sample| sample.kernel_row_retained_layout_hits),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Retained Slot Values",
        ranked_main_fixture_by(samples, |sample| sample.kernel_row_retained_slot_values),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Retained Length Values",
        ranked_main_fixture_by(samples, |sample| {
            sample.kernel_row_retained_octet_length_values
        }),
    );
}

fn matrix_mode_from_report(report: &MatrixReport) -> MatrixMode {
    match report.matrix_mode.as_str() {
        "deterministic" => MatrixMode::Deterministic,
        "random" => MatrixMode::Random,
        other => panic!("matrix report mode should be known, got '{other}'"),
    }
}

fn ranked_by<F>(samples: &[MatrixSample], key: F) -> Vec<&MatrixSample>
where
    F: Fn(&MatrixSample) -> u64,
{
    let mut ranked = samples.iter().collect::<Vec<_>>();
    ranked.sort_by_key(|sample| Reverse(key(sample)));
    ranked.truncate(top_n());
    ranked
}

fn ranked_main_fixture_by<F>(samples: &[MatrixSample], key: F) -> Vec<&MatrixSample>
where
    F: Fn(&MatrixSample) -> u64,
{
    let mut ranked = samples
        .iter()
        .filter(|sample| !sample_is_storage_mirror(sample))
        .collect::<Vec<_>>();
    ranked.sort_by_key(|sample| Reverse(key(sample)));
    ranked.truncate(top_n());
    ranked
}

fn sample_is_storage_mirror(sample: &MatrixSample) -> bool {
    sample.surface == MatrixSurface::HeapUser.label()
        || sample.surface == MatrixSurface::JournaledUser.label()
}

fn append_ranked_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Total | Compile | Execute | Planner | Store | Executor | data_store.get | index_store.get | index_store.ranges | index_store.entries | Rows | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.total_local_instructions,
            sample.compile_local_instructions,
            sample.execute_local_instructions,
            sample.planner_local_instructions,
            sample.store_local_instructions,
            sample.executor_local_instructions,
            sample.data_store_get_calls,
            sample.index_store_get_calls,
            sample.index_store_range_scan_calls,
            sample.index_store_entry_reads,
            sample.outcome.row_count,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_blob_output_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.output_blob_bytes > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Blob Values | Blob Bytes | Blob Hex Bytes | Total | Rows | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.output_blob_values,
            sample.output_blob_bytes,
            sample.output_blob_hex_bytes,
            sample.total_local_instructions,
            sample.outcome.row_count,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_compile_phase_table(output: &mut String, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.compile_local_instructions > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## Top Compile Phase Instructions").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Compile | Cache Key | Cache Lookup | Parse | Tokenize | Select | Expr | Predicate | Aggregate Check | Prepare | Lower | Bind | Cache Insert | Total | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.compile_local_instructions,
            sample.compile_cache_key_local_instructions,
            sample.compile_cache_lookup_local_instructions,
            sample.compile_parse_local_instructions,
            sample.compile_parse_tokenize_local_instructions,
            sample.compile_parse_select_local_instructions,
            sample.compile_parse_expr_local_instructions,
            sample.compile_parse_predicate_local_instructions,
            sample.compile_aggregate_lane_check_local_instructions,
            sample.compile_prepare_local_instructions,
            sample.compile_lower_local_instructions,
            sample.compile_bind_local_instructions,
            sample.compile_cache_insert_local_instructions,
            sample.total_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_pure_covering_table<F>(
    output: &mut String,
    title: &str,
    samples: Vec<&MatrixSample>,
    metric: F,
) where
    F: Fn(&MatrixSample) -> u64,
{
    let samples = samples
        .into_iter()
        .filter(|sample| metric(sample) > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Decode | Row Assembly | Total | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---|").expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.pure_covering_decode_local_instructions,
            sample.pure_covering_row_assembly_local_instructions,
            sample.total_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_hybrid_covering_table<F>(
    output: &mut String,
    title: &str,
    samples: Vec<&MatrixSample>,
    metric: F,
) where
    F: Fn(&MatrixSample) -> u64,
{
    let samples = samples
        .into_iter()
        .filter(|sample| metric(sample) > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Path Hits | Index Fields | Row Fields | Data Store Get | Total | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.hybrid_covering_path_hits,
            sample.hybrid_covering_index_field_accesses,
            sample.hybrid_covering_row_field_accesses,
            sample.data_store_get_calls,
            sample.total_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_direct_data_row_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.direct_data_row_scan_local_instructions > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Scan | Key Stream | Row Read | Key Encode | Data Store Get | Order Window | Page Window | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.direct_data_row_scan_local_instructions,
            sample.direct_data_row_key_stream_local_instructions,
            sample.direct_data_row_row_read_local_instructions,
            sample.direct_data_row_key_encode_local_instructions,
            sample.direct_data_row_store_get_local_instructions,
            sample.direct_data_row_order_window_local_instructions,
            sample.direct_data_row_page_window_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_kernel_row_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.kernel_row_scan_local_instructions > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Scan | Key Stream | Row Read | Order Window | Page Window | Retained Layouts | Retained Values | Length Values | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.kernel_row_scan_local_instructions,
            sample.kernel_row_key_stream_local_instructions,
            sample.kernel_row_row_read_local_instructions,
            sample.kernel_row_order_window_local_instructions,
            sample.kernel_row_page_window_local_instructions,
            sample.kernel_row_retained_layout_hits,
            sample.kernel_row_retained_slot_values,
            sample.kernel_row_retained_octet_length_values,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_storage_backend_comparison_table(output: &mut String, samples: &[MatrixSample]) {
    let heap_samples = storage_samples_by_suffix(samples, MatrixSurface::HeapUser, "heap_user.");
    let journaled_samples =
        storage_samples_by_suffix(samples, MatrixSurface::JournaledUser, "journaled_user.");

    let mut rows = heap_samples
        .iter()
        .filter_map(|(suffix, heap)| {
            let heap = *heap;
            let journaled = *journaled_samples.get(suffix)?;

            Some((suffix.as_str(), heap, journaled))
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return;
    }

    rows.sort_by_key(|(_, heap, journaled)| {
        Reverse(absolute_delta(
            journaled.total_local_instructions,
            heap.total_local_instructions,
        ))
    });
    rows.truncate(top_n());

    writeln!(output, "## Heap vs Journaled Unindexed Storage Mirror")
        .expect("write to string should succeed");
    writeln!(
        output,
        "Mirror entities expose only the primary-key index; field predicate/order scenarios are intentional unindexed scan baselines."
    )
        .expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Heap Total | Journaled Total | Journaled Delta | Journaled Ratio | Heap Store | Journaled Store | SQL |",
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---:|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for (suffix, heap, journaled) in rows {
        writeln!(
            output,
            "| `{suffix}` | {} | {} | {} | {} | {} | {} | `{}` |",
            heap.total_local_instructions,
            journaled.total_local_instructions,
            signed_delta(
                journaled.total_local_instructions,
                heap.total_local_instructions
            ),
            ratio_text(
                journaled.total_local_instructions,
                heap.total_local_instructions
            ),
            heap.store_local_instructions,
            journaled.store_local_instructions,
            journaled.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn storage_samples_by_suffix<'a>(
    samples: &'a [MatrixSample],
    surface: MatrixSurface,
    prefix: &str,
) -> BTreeMap<String, &'a MatrixSample> {
    samples
        .iter()
        .filter(|sample| sample.surface == surface.label())
        .filter_map(|sample| {
            sample
                .key
                .strip_prefix(prefix)
                .map(|suffix| (suffix.to_string(), sample))
        })
        .collect()
}

const fn absolute_delta(value: u64, baseline: u64) -> u64 {
    value.abs_diff(baseline)
}

fn signed_delta(value: u64, baseline: u64) -> String {
    if value >= baseline {
        format!("+{}", value - baseline)
    } else {
        format!("-{}", baseline - value)
    }
}

fn ratio_text(value: u64, baseline: u64) -> String {
    if baseline == 0 {
        return "n/a".to_string();
    }

    let scaled = value.saturating_mul(100) / baseline;
    format!("{}.{:02}x", scaled / 100, scaled % 100)
}

fn append_failure_table(output: &mut String, failures: &[MatrixFailure]) {
    if failures.is_empty() {
        return;
    }

    writeln!(output, "## Failed Generated Scenarios").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Code | Diagnostic | Class | Origin | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---|---|---|---|").expect("write to string should succeed");
    for failure in failures.iter().take(top_n()) {
        writeln!(
            output,
            "| `{}` | {} | {} | {} ({}) | {} | {} | `{}` |",
            failure.key,
            failure.surface,
            failure.code,
            failure.diagnostic_label,
            failure.diagnostic_code,
            failure.class,
            failure.origin,
            failure.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

#[test]
fn sql_perf_matrix_failures_use_stable_diagnostic_labels() {
    let scenario = scenario(
        "user.failure.query_plan",
        MatrixSurface::User,
        "failure.query_plan",
        "SELECT id FROM PerfAuditUser ORDER BY unsupported_expression",
    );
    let failure = matrix_failure_from_error(
        &scenario,
        Error::from_code(DiagnosticCode::QueryPlan, ErrorOrigin::Query),
    );

    assert_eq!(failure.code, 3);
    assert_eq!(failure.diagnostic_code, 3);
    assert_eq!(failure.diagnostic_label, "QueryPlan");
    assert_eq!(failure.class, "Query");
    assert_eq!(failure.origin, "Query");
}

fn print_matrix_summary(report: &MatrixReport) {
    println!("{}", matrix_markdown(report));
}

fn matrix_delta_path_env(name: &str) -> PathBuf {
    env::var(name).map_or_else(
        |_| panic!("{name} should point at one matrix JSON report"),
        PathBuf::from,
    )
}

fn matrix_delta_output_stem() -> PathBuf {
    env::var("ICYDB_SQL_PERF_MATRIX_DELTA_OUT").map_or_else(
        |_| workspace_root().join("artifacts/perf-audit/sql_perf_matrix_delta"),
        PathBuf::from,
    )
}

fn matrix_delta_key_set_env(name: &str) -> BTreeSet<String> {
    env::var(name).map_or_else(
        |_| BTreeSet::new(),
        |value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|key| !key.is_empty())
                .map(str::to_string)
                .collect()
        },
    )
}

#[test]
fn sql_perf_generated_matrix_has_stable_shape() {
    let deterministic = deterministic_matrix();
    assert!(
        deterministic.len() >= 1_000,
        "deterministic matrix should be broad enough to hunt hotspots; got {}",
        deterministic.len(),
    );
    assert_eq!(
        deterministic.first().map(|scenario| scenario.key.as_str()),
        Some("user.select.pk.all.pk_asc.limit1"),
    );

    let mut keys = HashSet::new();
    for scenario in &deterministic {
        assert!(
            keys.insert(scenario.key.as_str()),
            "duplicate generated scenario key '{}'",
            scenario.key,
        );
        assert!(
            scenario.sql.starts_with("SELECT")
                || scenario.sql.starts_with("EXPLAIN")
                || scenario.sql.starts_with("DESCRIBE")
                || scenario.sql.starts_with("SHOW"),
            "generated scenario '{}' should use supported SQL syntax",
            scenario.key,
        );
    }
}

#[test]
fn sql_perf_generated_matrix_includes_branch_route_hotspots() {
    let deterministic = deterministic_matrix();
    let scenarios_by_key = deterministic
        .iter()
        .enumerate()
        .map(|(position, scenario)| (scenario.key.as_str(), (position, scenario)))
        .collect::<BTreeMap<_, _>>();
    let expected_keys = [
        "token.collection_stage_id.branch_set.page_only.limit50",
        "token.collection_stage_id.branch_set.covering_page_only.limit50",
        "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
        "token.collection_stage_id.branch_set.full_entity.limit50",
        "token.collection_stage_id.branch_set.index_residual_covering.limit3",
        "token.collection_stage_id.prefixed_stage_range.page_only.limit50",
        "token.collection_stage_id.branch_set.count",
        "token.collection_stage_id.branch_set.duplicate_count",
        "token.collection_stage_id.branch_set.wide_page_only.limit50",
        "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
        "token.collection_stage_id.overcap_fallback.page_only.limit50",
        "token.collection_stage_id.overcap_pruned.page_only.limit50",
        "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
    ];

    for key in expected_keys {
        let (position, scenario) = scenarios_by_key
            .get(key)
            .unwrap_or_else(|| panic!("deterministic matrix should include route hotspot {key}"));
        assert!(
            *position < DEFAULT_MATRIX_LIMIT,
            "route hotspot {key} should run inside the default matrix window; position={position}"
        );
        assert_eq!(scenario.surface, MatrixSurface::Token);
        assert!(
            scenario.family.starts_with("route."),
            "route hotspot {key} should be grouped under route families"
        );
        assert!(
            scenario.sql.contains("FROM PerfAuditToken"),
            "route hotspot {key} should target the token fixture"
        );
        assert!(
            scenario
                .sql
                .contains("collection_id = '01KV5N439P0000000000000000'"),
            "route hotspot {key} should keep the fixed collection prefix"
        );
    }

    assert_branch_route_hotspot_sql_shapes(&scenarios_by_key);
    assert_sparse_collection_in_route_hotspots(&scenarios_by_key);
}

#[test]
fn sql_perf_sqlite_comparison_default_subset_is_broad_and_compatible() {
    let deterministic = deterministic_matrix();
    let scenarios_by_key = deterministic
        .iter()
        .map(|scenario| (scenario.key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let selected = deterministic
        .iter()
        .filter(|scenario| sqlite_audit_scenario_is_compatible(scenario))
        .collect::<Vec<_>>();
    let selected_keys = selected
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<BTreeSet<_>>();

    assert!(
        selected.len() >= 1_000,
        "SQLite audit comparison should cover the broad compatible matrix subset; got {}",
        selected.len(),
    );
    for key in SQL_PERF_SQLITE_REQUIRED_COMPATIBLE_KEYS {
        assert!(
            scenarios_by_key.contains_key(key),
            "SQLite audit comparison required key should exist in deterministic matrix: {key}"
        );
        assert!(
            selected_keys.contains(key),
            "SQLite audit comparison compatible subset should include required key: {key}"
        );
    }
    assert!(
        selected
            .iter()
            .any(|scenario| scenario.surface == MatrixSurface::User)
    );
    assert!(
        selected
            .iter()
            .any(|scenario| scenario.surface == MatrixSurface::Account)
    );
    assert!(
        selected
            .iter()
            .any(|scenario| scenario.surface == MatrixSurface::Blob)
    );
    assert!(
        selected
            .iter()
            .any(|scenario| scenario.surface == MatrixSurface::Token)
    );
    for incompatible in [
        "user.select.wide.all.pk_asc.limit1",
        "blob.select.payload.all.pk_asc.limit1",
        "blob.select.lengths.all.pk_asc.limit1",
        "token.collection_stage_id.branch_set.full_entity.limit50",
        "user.metadata.describe",
        "heap_user.select.pk.all.pk_asc.limit1",
    ] {
        assert!(
            !selected_keys.contains(incompatible),
            "SQLite audit comparison compatible subset should exclude incompatible key: {incompatible}"
        );
    }
}

#[test]
fn sql_perf_sqlite_comparison_schema_seeds_audit_tables() {
    let schema = sqlite_audit_schema_and_seed();

    for required in [
        "CREATE TABLE PerfAuditUser",
        "CREATE TABLE PerfAuditAccount",
        "CREATE TABLE PerfAuditBlob",
        "CREATE TABLE PerfAuditToken",
        "CREATE INDEX perf_audit_token_collection_stage_id",
        "INSERT INTO PerfAuditUser",
        "INSERT INTO PerfAuditAccount",
        "INSERT INTO PerfAuditBlob",
        "INSERT INTO PerfAuditToken",
        TOKEN_TARGET_COLLECTION,
        "draft-pressure-000",
        "published-pressure-239",
    ] {
        assert!(
            schema.contains(required),
            "SQLite audit comparison schema should contain `{required}`"
        );
    }
}

#[test]
fn sql_perf_sqlite_plan_summary_classifies_index_and_temp_sort() {
    let rows = vec![
        "4\t0\t0\t`--SEARCH PerfAuditUser USING COVERING INDEX perf_audit_user_age_id (age>?)"
            .to_string(),
        "18\t0\t0\t|--USE TEMP B-TREE FOR RIGHT PART OF ORDER BY".to_string(),
    ];
    let summary = sqlite_plan_summary(&rows);

    assert!(!sqlite_plan_has(&summary, SQLITE_PLAN_FEATURE_SCAN));
    assert!(sqlite_plan_has(&summary, SQLITE_PLAN_FEATURE_SEARCH));
    assert!(sqlite_plan_has(&summary, SQLITE_PLAN_FEATURE_INDEX));
    assert!(sqlite_plan_has(
        &summary,
        SQLITE_PLAN_FEATURE_COVERING_INDEX
    ));
    assert!(sqlite_plan_has(&summary, SQLITE_PLAN_FEATURE_TEMP_ORDER));
    assert_eq!(summary.index_names, vec!["perf_audit_user_age_id"]);

    let sample = MatrixSample {
        route_outcome: "pushed".to_string(),
        ..MatrixSample::default()
    };
    assert_eq!(
        sqlite_plan_alignment(&sample, &summary),
        "review_icydb_pushed_sqlite_temp_order"
    );
}

fn assert_branch_route_hotspot_sql_shapes(
    scenarios_by_key: &BTreeMap<&str, (usize, &MatrixScenario)>,
) {
    let branch = scenarios_by_key
        .get("token.collection_stage_id.branch_set.page_only.limit50")
        .expect("branch-set route hotspot should exist")
        .1;
    assert!(
        branch.sql.contains("stage IN ('Draft', 'Review')"),
        "branch-set route hotspot should use the small exact stage set"
    );
    assert!(
        branch.sql.contains("ORDER BY id ASC LIMIT 50"),
        "branch-set route hotspot should preserve the primary-key page order"
    );

    let prefixed_range = scenarios_by_key
        .get("token.collection_stage_id.prefixed_stage_range.page_only.limit50")
        .expect("prefixed range route hotspot should exist")
        .1;
    assert!(
        prefixed_range
            .sql
            .contains("stage >= 'Draft' AND stage < 'Review'"),
        "prefixed range hotspot should exercise one equality prefix plus one range component"
    );
    assert!(
        prefixed_range
            .sql
            .contains("ORDER BY stage ASC, id ASC LIMIT 50"),
        "prefixed range hotspot should preserve index-order pagination"
    );

    let wide_branch = scenarios_by_key
        .get("token.collection_stage_id.branch_set.wide_page_only.limit50")
        .expect("wide branch-set route hotspot should exist")
        .1;
    assert!(
        wide_branch.sql.contains(
            "stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden')"
        ),
        "wide branch-set hotspot should cover the admitted nine-branch route"
    );

    let over_cap = scenarios_by_key
        .get("token.collection_stage_id.overcap_fallback.page_only.limit50")
        .expect("over-cap route hotspot should exist")
        .1;
    assert!(
        over_cap.sql.contains(
            "stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07')"
        ),
        "over-cap route hotspot should exceed the branch-set admission cap"
    );

    let over_cap_pruned = scenarios_by_key
        .get("token.collection_stage_id.overcap_pruned.page_only.limit50")
        .expect("post-exclusion over-cap route hotspot should exist")
        .1;
    assert!(
        over_cap_pruned.sql.contains(
            "stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07')"
        ),
        "post-exclusion over-cap route hotspot should start from the same over-cap stage list"
    );
    assert!(
        over_cap_pruned.sql.contains(
            "stage NOT IN ('Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07')"
        ),
        "post-exclusion over-cap route hotspot should explicitly reduce the branch set under the cap"
    );
}

fn assert_sparse_collection_in_route_hotspots(
    scenarios_by_key: &BTreeMap<&str, (usize, &MatrixScenario)>,
) {
    let (sparse_position, sparse_in) = scenarios_by_key
        .get("token.collection_id.sparse_in.page_only.limit50")
        .expect("sparse collection IN route hotspot should exist")
        .to_owned();
    assert!(
        sparse_position < DEFAULT_MATRIX_LIMIT,
        "sparse collection IN hotspot should run inside the default matrix window; position={sparse_position}"
    );
    assert!(
        sparse_in.sql.contains("collection_id IN"),
        "sparse collection IN hotspot should exercise the index multi-lookup route"
    );
    assert!(
        sparse_in.sql.contains("missing-collection-249"),
        "sparse collection IN hotspot should include 250 missing prefixes"
    );
    assert!(
        sparse_in.sql.contains("ORDER BY id ASC LIMIT 50"),
        "sparse collection IN hotspot should preserve the primary-key page order"
    );

    let (sparse_count_position, sparse_count) = scenarios_by_key
        .get("token.collection_id.sparse_in.count")
        .expect("sparse collection IN count hotspot should exist")
        .to_owned();
    assert!(
        sparse_count_position < DEFAULT_MATRIX_LIMIT,
        "sparse collection IN count hotspot should run inside the default matrix window; position={sparse_count_position}"
    );
    assert!(
        sparse_count.sql.contains("SELECT COUNT(*)"),
        "sparse collection IN count hotspot should exercise count terminal routing"
    );
    assert!(
        sparse_count.sql.contains("missing-collection-249"),
        "sparse collection IN count hotspot should include 250 missing prefixes"
    );
}

#[test]
fn sql_perf_matrix_exact_key_filter_selects_known_scenarios() {
    let deterministic = deterministic_matrix();
    let selected = filter_matrix_scenarios(
        deterministic,
        Some(
            "user.select.pk.all.pk_asc.limit1,\
             journaled_user.select.wide.name_range.age_asc.limit10",
        ),
    );
    let keys = selected
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        vec![
            "user.select.pk.all.pk_asc.limit1",
            "journaled_user.select.wide.name_range.age_asc.limit10",
        ],
    );
}

#[test]
fn sql_perf_matrix_classifies_bounded_primary_order_limit_as_pushed() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        "select.pk.all.pk_asc",
    );
    sample.data_store_get_calls = 1;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "primary_order",
            "pushed",
            Some("primary_order_limit_stop_proven"),
        ),
    );
}

#[test]
fn sql_perf_matrix_limit_stop_after_reports_pushed_bound() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        "select.pk.all.pk_asc",
    );
    sample.data_store_get_calls = 1;
    let route = route_classification_for_sample(&sample);
    sample.route_family = route.family.to_string();
    sample.route_outcome = route.outcome.to_string();
    sample.route_reason = route.reason.map(str::to_string);

    assert_eq!(
        limit_stop_after_for_sample(&sample),
        MatrixLimitStopAfter {
            possible: true,
            returned_limit: Some(1),
            lookahead: 1,
            stopped_after_matches: Some(1),
            stopped_after_index_entries: Some(0),
            disabled_reason: None,
        },
    );
}

#[test]
fn sql_perf_matrix_classifies_offset_primary_order_limit_as_pushed_when_bounded() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditUser ORDER BY id DESC LIMIT 3 OFFSET 2",
        "select.pk.all.pk_desc",
    );
    sample.data_store_get_calls = 6;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "primary_order",
            "pushed",
            Some("primary_order_limit_stop_proven"),
        ),
    );
}

#[test]
fn sql_perf_matrix_keeps_primary_order_candidate_unpushed_without_bounded_evidence() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        "select.pk.all.pk_asc",
    );
    sample.data_store_get_calls = 512;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "primary_order",
            "eligible_but_not_pushed",
            Some("primary_order_candidate"),
        ),
    );

    sample.data_store_get_calls = 1;
    sample.direct_data_row_order_window_local_instructions = 10;
    let materialized_route = route_classification_for_sample(&sample);

    assert_eq!(
        materialized_route,
        RouteClassification::new(
            "materialized_order",
            "materialized",
            Some("requires_materialized_sort"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_primary_order_residual_candidate_scan() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditUser WHERE active = true ORDER BY id DESC LIMIT 3",
        "select.pk.active_true.pk_desc",
    );
    sample.data_store_get_calls = 5;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "residual_filter_ordered_scan",
            "residual_unbounded",
            Some("residual_filter_requires_candidate_scan"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_bounded_secondary_order_limit_as_pushed() {
    let mut sample = route_classification_sample(
        "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
        "select.pk.all.age_asc",
    );
    sample.data_store_get_calls = 3;
    sample.index_store_entry_reads = 4;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "secondary_order",
            "pushed",
            Some("secondary_order_limit_stop_proven"),
        ),
    );
}

#[test]
fn sql_perf_matrix_keeps_secondary_order_candidate_unpushed_without_index_bound() {
    let mut sample = route_classification_sample(
        "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
        "select.pk.all.age_asc",
    );
    sample.data_store_get_calls = 3;
    sample.index_store_entry_reads = 512;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "secondary_order",
            "eligible_but_not_pushed",
            Some("secondary_order_candidate"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_materialized_secondary_order_window() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditAccount ORDER BY handle ASC, id ASC LIMIT 1",
        "select.pk.all.handle_asc",
    );
    sample.surface = MatrixSurface::Account.label().to_string();
    sample.data_store_get_calls = 6;
    sample.kernel_row_order_window_local_instructions = 1_893;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "materialized_order",
            "materialized",
            Some("requires_materialized_sort"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_secondary_order_residual_candidate_scan() {
    let mut sample = route_classification_sample(
        "SELECT id, handle FROM PerfAuditAccount WHERE LOWER(handle) LIKE 'a%' AND active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        "select.narrow.lower_handle_prefix_active.lower_handle_asc",
    );
    sample.surface = MatrixSurface::Account.label().to_string();
    sample.data_store_get_calls = 9;
    sample.index_store_range_scan_calls = 3;
    sample.index_store_entry_reads = 9;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "residual_filter_ordered_scan",
            "residual_unbounded",
            Some("residual_filter_requires_candidate_scan"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_unindexed_order_expression_as_unsupported() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditUser ORDER BY age + rank ASC, id ASC LIMIT 1",
        "select.pk.all.numeric_expr_asc",
    );
    sample.data_store_get_calls = 6;
    sample.kernel_row_order_window_local_instructions = 2_048;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "unsupported_access_kind",
            "unsupported",
            Some("order_expression_not_classified"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_grouped_aggregate_as_materialized() {
    let mut sample = route_classification_sample(
        "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
        "aggregate.grouped",
    );
    sample.outcome.result_kind = "grouped".to_string();
    sample.outcome.row_count = 6;
    sample.grouped_fold_local_instructions = 8_192;
    sample.data_store_get_calls = 6;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "materialized_order",
            "materialized",
            Some("grouped_aggregate_materialized"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_blob_bucket_id_order_as_missing_index_suffix() {
    let mut sample = route_classification_sample(
        "SELECT id, label FROM PerfAuditBlob WHERE bucket >= 10 AND bucket < 40 ORDER BY bucket ASC, id ASC LIMIT 3",
        "select.narrow.bucket_range.bucket_asc",
    );
    sample.surface = MatrixSurface::Blob.label().to_string();
    sample.data_store_get_calls = 3;
    sample.index_store_entry_reads = 4;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "secondary_order",
            "missing_tie_breaker",
            Some("index_order_suffix_gap"),
        ),
    );
}

#[test]
fn sql_perf_matrix_limit_stop_after_reports_index_suffix_gap_reason() {
    let mut sample = route_classification_sample(
        "SELECT id, label FROM PerfAuditBlob WHERE bucket >= 10 AND bucket < 40 ORDER BY bucket ASC, id ASC LIMIT 3",
        "select.narrow.bucket_range.bucket_asc",
    );
    sample.surface = MatrixSurface::Blob.label().to_string();
    let route = route_classification_for_sample(&sample);
    sample.route_family = route.family.to_string();
    sample.route_outcome = route.outcome.to_string();
    sample.route_reason = route.reason.map(str::to_string);

    assert_eq!(
        limit_stop_after_for_sample(&sample),
        MatrixLimitStopAfter {
            possible: false,
            returned_limit: Some(3),
            lookahead: 1,
            stopped_after_matches: None,
            stopped_after_index_entries: None,
            disabled_reason: Some("index_order_suffix_gap".to_string()),
        },
    );
}

#[test]
fn sql_perf_matrix_keeps_blob_bucket_label_id_order_pushable() {
    let mut sample = route_classification_sample(
        "SELECT id, label FROM PerfAuditBlob WHERE bucket >= 10 AND bucket < 40 ORDER BY bucket ASC, label ASC, id ASC LIMIT 3",
        "select.narrow.bucket_range.bucket_label_asc",
    );
    sample.surface = MatrixSurface::Blob.label().to_string();
    sample.data_store_get_calls = 3;
    sample.index_store_entry_reads = 4;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "secondary_order",
            "pushed",
            Some("secondary_order_limit_stop_proven"),
        ),
    );
}

#[test]
fn sql_perf_matrix_classifies_bounded_equality_prefix_suffix_order_as_pushed() {
    let mut sample = route_classification_sample(
        "SELECT id FROM PerfAuditToken WHERE collection_id = '01KV5N439P0000000000000000' ORDER BY stage ASC, id ASC LIMIT 50",
        "route.prefixed_range.page_only",
    );
    sample.surface = MatrixSurface::Token.label().to_string();
    sample.data_store_get_calls = 0;
    sample.index_store_range_scan_calls = 1;
    sample.index_store_entry_reads = 50;

    let route = route_classification_for_sample(&sample);

    assert_eq!(
        route,
        RouteClassification::new(
            "equality_prefix_ordered_suffix",
            "pushed",
            Some("equality_prefix_ordered_suffix_limit_stop_proven"),
        ),
    );
}

#[test]
fn sql_perf_matrix_extracts_order_by_hint_from_matrix_sql() {
    assert_eq!(
        sql_order_by_idx_hint("SELECT id FROM PerfAuditToken ORDER BY stage ASC, id ASC LIMIT 50",)
            .as_deref(),
        Some("stage ASC, id ASC"),
    );
    assert_eq!(
        sql_order_by_idx_hint(
            "SELECT name FROM PerfAuditUser ORDER BY ROUND(age / 3, 2) DESC, name ASC LIMIT 2",
        )
        .as_deref(),
        Some("ROUND(age / 3, 2) DESC, name ASC"),
    );
    assert_eq!(
        sql_order_by_idx_hint("SELECT id FROM PerfAuditUser LIMIT 1"),
        None,
    );
}

fn matrix_delta_report_test_fixture() -> (MatrixReport, MatrixReport) {
    let mut before_sample = report_matrix_sample(
        "user.select.pk.all.pk_asc.limit1",
        "user",
        1_000,
        100,
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
    );
    before_sample.result_signature = Some("projection|PerfAuditUser|id|1|1".to_string());
    let mut after_sample = before_sample.clone();
    after_sample.total_local_instructions = 900;
    after_sample.execute_local_instructions = 899;
    after_sample.data_store_get_calls = 0;
    after_sample.route_outcome = "pushed".to_string();
    after_sample.route_reason = Some("primary_order_limit_stop_proven".to_string());
    after_sample.limit_stop_after = limit_stop_after_for_sample(&after_sample);
    after_sample
        .result_signature
        .clone_from(&before_sample.result_signature);
    let before_new_failure_sample = report_matrix_sample(
        "user.failure.new",
        "user",
        1_100,
        100,
        "SELECT id FROM PerfAuditUser ORDER BY unsupported_expression",
    );
    let after_resolved_failure_sample = report_matrix_sample(
        "user.failure.resolved",
        "user",
        1_050,
        100,
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
    );

    let before_failure = MatrixFailure {
        key: "user.failure.resolved".to_string(),
        source: MatrixSource::Deterministic.label().to_string(),
        surface: MatrixSurface::User.label().to_string(),
        family: "failure.query_plan".to_string(),
        sql: "SELECT id FROM PerfAuditUser ORDER BY unsupported_expression".to_string(),
        route_family: failed_route_family(),
        route_outcome: failed_route_outcome(),
        route_reason: failed_route_reason(),
        code: 3,
        diagnostic_code: 3,
        diagnostic_label: "QueryPlan".to_string(),
        class: "Query".to_string(),
        origin: "Query".to_string(),
    };
    let after_failure = MatrixFailure {
        key: "user.failure.new".to_string(),
        ..before_failure.clone()
    };

    let before = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: 3,
        executed_scenario_count: 2,
        failed_scenario_count: 1,
        matrix_limit: 3,
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples: vec![before_sample, before_new_failure_sample],
        failures: vec![before_failure],
    };
    let current = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: 3,
        executed_scenario_count: 2,
        failed_scenario_count: 1,
        matrix_limit: 3,
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples: vec![after_sample, after_resolved_failure_sample],
        failures: vec![after_failure],
    };

    (before, current)
}

#[test]
fn sql_perf_matrix_delta_reports_union_status_routes_and_signatures() {
    let (before, current) = matrix_delta_report_test_fixture();
    let focused = BTreeSet::from(["user.select.pk.all.pk_asc.limit1".to_string()]);
    let expected = focused.clone();
    let delta = matrix_delta_report(
        Path::new("/tmp/before.json"),
        &before,
        Path::new("/tmp/after.json"),
        &current,
        &focused,
        &expected,
    );

    assert_eq!(delta.union_scenario_count, 3);
    assert_eq!(delta.common_successful_scenario_count, 1);
    assert_eq!(delta.improved_scenario_count, 1);
    assert_eq!(delta.new_failure_count, 1);
    assert_eq!(delta.resolved_failure_count, 1);
    assert!(
        delta.closeout_failures.is_empty(),
        "expected clean delta closeout, got {:?}",
        delta.closeout_failures,
    );

    let row = delta
        .rows
        .iter()
        .find(|row| row.key == "user.select.pk.all.pk_asc.limit1")
        .expect("delta row should exist for common success");
    assert_eq!(row.status_class, "common_success");
    assert_eq!(row.total_local_instructions.delta, Some(-100));
    assert_eq!(row.data_store_get_calls.delta, Some(-1));
    assert_eq!(row.before_route_family.as_deref(), Some("primary_order"));
    assert_eq!(
        row.before_route_outcome.as_deref(),
        Some("eligible_but_not_pushed")
    );
    assert_eq!(row.after_route_outcome.as_deref(), Some("pushed"));
    assert_eq!(row.before_order_by_idx_hint.as_deref(), Some("id ASC"));
    assert_eq!(row.after_order_by_idx_hint.as_deref(), Some("id ASC"));
    assert_eq!(
        row.before_limit_stop_after
            .as_ref()
            .map(|limit| limit.possible),
        Some(false),
    );
    assert_eq!(
        row.after_limit_stop_after
            .as_ref()
            .map(|limit| limit.possible),
        Some(true),
    );
    assert!(!row.signature_changes.result_signature_changed);

    let markdown = matrix_delta_markdown(&delta);
    assert!(
        markdown.contains("Top 50 Improvements By Absolute Instructions"),
        "delta markdown should include improvement table"
    );
    assert!(
        markdown.contains("Route Family Delta"),
        "delta markdown should include route-family aggregate"
    );
    assert!(
        markdown.contains("id ASC -> id ASC"),
        "delta markdown should include order-by hint transitions"
    );
    assert!(
        markdown.contains(
            "disabled(test_sample) -> possible(limit=1,lookahead=1,matches=1,index_entries=0)"
        ),
        "delta markdown should include limit-stop transitions"
    );
}

#[test]
fn sql_perf_matrix_delta_gate_rejects_absent_focused_targets() {
    let (before, current) = matrix_delta_report_test_fixture();
    let focused = BTreeSet::from(["user.select.pk.missing.pk_asc.limit1".to_string()]);
    let delta = matrix_delta_report(
        Path::new("/tmp/before.json"),
        &before,
        Path::new("/tmp/after.json"),
        &current,
        &focused,
        &BTreeSet::new(),
    );

    assert!(
        delta.closeout_failures.contains(
            &"focused target `user.select.pk.missing.pk_asc.limit1` is absent from both reports"
                .to_string()
        ),
        "missing focused target should fail closeout, got {:?}",
        delta.closeout_failures,
    );
}

#[test]
fn sql_perf_matrix_delta_gate_rejects_unimproved_expected_targets() {
    let (mut before, mut current) = matrix_delta_report_test_fixture();
    before.samples[0].total_local_instructions = 1_000;
    current.samples[0].total_local_instructions = 1_000;
    let expected = BTreeSet::from(["user.select.pk.all.pk_asc.limit1".to_string()]);
    let delta = matrix_delta_report(
        Path::new("/tmp/before.json"),
        &before,
        Path::new("/tmp/after.json"),
        &current,
        &BTreeSet::new(),
        &expected,
    );

    assert!(
        delta.closeout_failures.contains(
            &"expected improvement target `user.select.pk.all.pk_asc.limit1` did not reduce total instructions"
                .to_string(),
        ),
        "unimproved expected target should fail closeout, got {:?}",
        delta.closeout_failures,
    );
}

#[test]
fn sql_perf_matrix_delta_gate_rejects_canister_wasm_profile_mismatch() {
    let (before, mut current) = matrix_delta_report_test_fixture();
    current.canister_wasm_profile = "wasm-release".to_string();
    let delta = matrix_delta_report(
        Path::new("/tmp/before.json"),
        &before,
        Path::new("/tmp/after.json"),
        &current,
        &BTreeSet::new(),
        &BTreeSet::new(),
    );

    assert!(
        delta.closeout_failures.contains(
            &"canister wasm profile mismatch: baseline `test`, current `wasm-release`".to_string(),
        ),
        "profile mismatch should fail closeout, got {:?}",
        delta.closeout_failures,
    );
}

#[test]
fn sql_perf_matrix_markdown_reports_route_classification_summary() {
    let mut sample = report_matrix_sample(
        "user.select.pk.all.pk_asc.limit1",
        "user",
        100,
        10,
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
    );
    sample.route_outcome = "pushed".to_string();
    sample.route_reason = Some("primary_order_limit_stop_proven".to_string());

    let failure = MatrixFailure {
        key: "user.failure".to_string(),
        source: MatrixSource::Deterministic.label().to_string(),
        surface: MatrixSurface::User.label().to_string(),
        family: "failure.query_plan".to_string(),
        sql: "SELECT id FROM PerfAuditUser ORDER BY unsupported_expression".to_string(),
        route_family: failed_route_family(),
        route_outcome: failed_route_outcome(),
        route_reason: failed_route_reason(),
        code: 3,
        diagnostic_code: 3,
        diagnostic_label: "QueryPlan".to_string(),
        class: "Query".to_string(),
        origin: "Query".to_string(),
    };
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: 2,
        executed_scenario_count: 1,
        failed_scenario_count: 1,
        matrix_limit: 2,
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples: vec![sample],
        failures: vec![failure],
    };

    let markdown = matrix_markdown(&report);

    assert!(
        markdown.contains("- canister wasm profile: test"),
        "matrix markdown should include the fixture wasm profile",
    );
    assert!(
        markdown.contains("## Route Classification Summary"),
        "matrix markdown should expose route classification coverage",
    );
    assert!(
        markdown.contains(
            "| primary_order | pushed | primary_order_limit_stop_proven | 1 | 100 | 1 | 0 | 0 | 1 |"
        ),
        "route summary should include successful pushed-route counters",
    );
    assert!(
        markdown.contains(
            "| failed_or_not_executed | failed | scenario_failed | 1 | 0 | 0 | 0 | 0 | 0 |"
        ),
        "route summary should include failed scenarios in the taxonomy",
    );
}

#[test]
fn sql_perf_random_matrix_has_seeded_stable_shape() {
    let random = random_matrix(DEFAULT_RANDOM_SEED, 20);
    assert_eq!(random.len(), 20);
    assert_eq!(
        random.first().map(|scenario| scenario.key.as_str()),
        Some("random.1cdb018200000001.0000.blob"),
    );

    let mut keys = HashSet::new();
    for scenario in &random {
        assert_eq!(scenario.source, MatrixSource::Random);
        assert!(
            keys.insert(scenario.key.as_str()),
            "duplicate random scenario key '{}'",
            scenario.key,
        );
        assert!(
            scenario.sql.starts_with("SELECT"),
            "random scenario '{}' should use supported SELECT syntax",
            scenario.key,
        );
    }
    assert!(
        random
            .iter()
            .any(|scenario| scenario.surface == MatrixSurface::Token),
        "seeded random matrix should include token IN/branch route pressure"
    );
}

#[test]
fn sql_perf_matrix_storage_backend_comparison_pairs_all_storage_mirrors() {
    let samples = vec![
        storage_matrix_sample("heap_user.select.pk.all.pk_asc.limit1", "heap_user", 80, 10),
        storage_matrix_sample(
            "journaled_user.select.pk.all.pk_asc.limit1",
            "journaled_user",
            70,
            12,
        ),
    ];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);

    assert!(
        markdown.contains("Heap vs Journaled Unindexed Storage Mirror"),
        "storage mirror report should include the comparison table",
    );
    assert!(
        markdown.contains("intentional unindexed scan baselines"),
        "storage mirror report should label field predicate/order cases as unindexed baselines",
    );
    assert!(
        markdown.contains("Heap Total"),
        "storage mirror report should include heap totals",
    );
    assert!(
        markdown.contains("| `select.pk.all.pk_asc.limit1` | 80 | 70 | -10 | 0.87x | 10 | 12 |"),
        "storage mirror report should pair heap and journaled by scenario suffix",
    );
}

#[test]
fn sql_perf_matrix_main_fixture_hotspots_exclude_storage_mirror_baselines() {
    let samples = vec![
        storage_matrix_sample(
            "heap_user.select.pk.all.pk_asc.limit1",
            "heap_user",
            800,
            100,
        ),
        storage_matrix_sample(
            "journaled_user.select.pk.all.pk_asc.limit1",
            "journaled_user",
            700,
            120,
        ),
        {
            let mut sample = main_fixture_sample_with_kernel_scan(
                "user.select.pk.all.pk_asc.limit1",
                "user",
                90,
                5,
                "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            );
            sample.kernel_row_retained_layout_hits = 1;
            sample.kernel_row_retained_slot_values = 3;
            sample.kernel_row_retained_octet_length_values = 1;
            sample
        },
    ];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let main_fixture_total_section = markdown
        .split("## Top Main Fixture Total Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("main fixture total hotspot section should render");

    assert!(
        main_fixture_total_section.contains("user.select.pk.all.pk_asc.limit1"),
        "main fixture hotspot section should keep ordinary fixture scenarios",
    );
    assert!(
        !main_fixture_total_section.contains("heap_user"),
        "main fixture hotspot section should exclude heap storage mirror baselines",
    );
    assert!(
        !main_fixture_total_section.contains("journaled_user"),
        "main fixture hotspot section should exclude journaled storage mirror baselines",
    );

    let main_fixture_kernel_section =
        matrix_markdown_section(&markdown, "Top Main Fixture Kernel Row Scan Instructions");
    assert!(
        !main_fixture_kernel_section.contains("heap_user"),
        "main fixture kernel-row hotspot section should exclude heap storage mirror baselines",
    );
    assert!(
        !main_fixture_kernel_section.contains("journaled_user"),
        "main fixture kernel-row hotspot section should exclude journaled storage mirror baselines",
    );

    assert_main_fixture_kernel_retained_hotspot_sections(&markdown);
}

fn assert_main_fixture_kernel_retained_hotspot_sections(markdown: &str) {
    const SCENARIO_KEY: &str = "user.select.pk.all.pk_asc.limit1";
    const SCENARIO_ROW: &str =
        "| `user.select.pk.all.pk_asc.limit1` | user | 90 | 0 | 5 | 0 | 0 | 1 | 3 | 1 |";

    let main_fixture_kernel_section =
        matrix_markdown_section(markdown, "Top Main Fixture Kernel Row Scan Instructions");
    assert!(
        main_fixture_kernel_section.contains(SCENARIO_KEY),
        "main fixture kernel-row hotspot section should keep ordinary fixture scenarios",
    );
    assert!(
        main_fixture_kernel_section.contains("Retained Values"),
        "main fixture kernel-row hotspot section should expose retained-slot footprint columns",
    );
    assert!(
        main_fixture_kernel_section.contains("Length Values"),
        "main fixture kernel-row hotspot section should expose byte-length retained-slot columns",
    );

    let main_fixture_retained_section =
        matrix_markdown_section(markdown, "Top Main Fixture Kernel Row Retained Slot Values");
    assert!(
        main_fixture_retained_section.contains(SCENARIO_KEY),
        "main fixture retained-slot hotspot section should rank ordinary fixture scenarios",
    );
    assert!(
        main_fixture_retained_section.contains(SCENARIO_ROW),
        "main fixture retained-slot hotspot section should expose retained layout/value counts",
    );

    let main_fixture_length_section = matrix_markdown_section(
        markdown,
        "Top Main Fixture Kernel Row Retained Length Values",
    );
    assert!(
        main_fixture_length_section.contains(SCENARIO_KEY),
        "main fixture retained byte-length hotspot section should rank ordinary fixture scenarios",
    );
    assert!(
        main_fixture_length_section.contains(SCENARIO_ROW),
        "main fixture retained byte-length hotspot section should expose retained length counts",
    );
}

fn matrix_markdown_section<'a>(markdown: &'a str, title: &str) -> &'a str {
    markdown
        .split(&format!("## {title}"))
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .unwrap_or_else(|| panic!("matrix markdown section should render: {title}"))
}

#[test]
fn sql_perf_matrix_reports_compile_phase_hotspots() {
    let mut sample = report_matrix_sample(
        "token.collection_stage_id.overcap_fallback.page_only.limit50",
        "token",
        240,
        10,
        "SELECT id FROM PerfAuditToken WHERE collection_id = '01KV5N439P0000000000000000' AND stage IN ('Draft', 'Review', 'Hold') ORDER BY id ASC LIMIT 50",
    );
    sample.compile_local_instructions = 120;
    sample.compile_cache_key_local_instructions = 7;
    sample.compile_cache_lookup_local_instructions = 5;
    sample.compile_parse_local_instructions = 30;
    sample.compile_parse_tokenize_local_instructions = 11;
    sample.compile_parse_select_local_instructions = 8;
    sample.compile_parse_expr_local_instructions = 4;
    sample.compile_parse_predicate_local_instructions = 7;
    sample.compile_aggregate_lane_check_local_instructions = 3;
    sample.compile_prepare_local_instructions = 13;
    sample.compile_lower_local_instructions = 17;
    sample.compile_bind_local_instructions = 19;
    sample.compile_cache_insert_local_instructions = 26;

    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: 1,
        executed_scenario_count: 1,
        failed_scenario_count: 0,
        matrix_limit: 1,
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples: vec![sample],
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let compile_phase_section = markdown
        .split("## Top Compile Phase Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("compile phase hotspot section should render");

    assert!(
        compile_phase_section
            .contains("token.collection_stage_id.overcap_fallback.page_only.limit50"),
        "compile phase section should include scenarios with compile attribution",
    );
    assert!(
        compile_phase_section.contains("| `token.collection_stage_id.overcap_fallback.page_only.limit50` | token | 120 | 7 | 5 | 30 | 11 | 8 | 4 | 7 | 3 | 13 | 17 | 19 | 26 | 240 |"),
        "compile phase section should expose cache, parse, prepare, lower, bind, and insert costs",
    );
}

#[test]
fn sql_perf_matrix_reports_pure_covering_hotspots() {
    let samples = vec![main_fixture_sample_with_pure_covering(
        "user.select.pk.id_only.pk_asc.limit1",
        "user",
        120,
        75,
        35,
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
    )];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let pure_covering_decode_section = markdown
        .split("## Top Pure Covering Decode Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("pure covering decode hotspot section should render");

    assert!(
        pure_covering_decode_section.contains("user.select.pk.id_only.pk_asc.limit1"),
        "pure covering decode section should include scenarios with decode attribution",
    );
    assert!(
        pure_covering_decode_section
            .contains("| `user.select.pk.id_only.pk_asc.limit1` | user | 75 | 35 | 120 |"),
        "pure covering decode section should expose decode, row assembly, and total costs",
    );

    let main_fixture_row_assembly_section = markdown
        .split("## Top Main Fixture Pure Covering Row Assembly Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("main fixture pure covering row assembly section should render");

    assert!(
        main_fixture_row_assembly_section.contains("user.select.pk.id_only.pk_asc.limit1"),
        "main fixture pure covering section should include ordinary fixture scenarios",
    );
}

#[test]
fn sql_perf_matrix_reports_hybrid_covering_hotspots() {
    let samples = vec![main_fixture_sample_with_hybrid_covering(
        "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
        "token",
        240,
        1,
        0,
        50,
        "SELECT id, title FROM PerfAuditToken ORDER BY id ASC LIMIT 50",
    )];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let hybrid_row_section = markdown
        .split("## Top Hybrid Covering Row Field Accesses")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("hybrid covering row-field hotspot section should render");

    assert!(
        hybrid_row_section
            .contains("token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50"),
        "hybrid covering section should include scenarios with row-backed field attribution",
    );
    assert!(
        hybrid_row_section.contains("| `token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50` | token | 1 | 0 | 50 | 50 | 240 |"),
        "hybrid covering section should expose path hits, field accesses, row gets, and total costs",
    );
}

fn storage_matrix_sample(key: &str, surface: &str, total: u64, store: u64) -> MatrixSample {
    report_matrix_sample(
        key,
        surface,
        total,
        store,
        "SELECT id FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1",
    )
}

fn report_matrix_sample(
    key: &str,
    surface: &str,
    total: u64,
    store: u64,
    sql: &str,
) -> MatrixSample {
    MatrixSample {
        key: key.to_string(),
        source: MatrixSource::Deterministic.label().to_string(),
        surface: surface.to_string(),
        family: "select.pk.all.pk_asc".to_string(),
        sql: sql.to_string(),
        route_family: "primary_order".to_string(),
        route_outcome: "eligible_but_not_pushed".to_string(),
        route_reason: Some("test_sample".to_string()),
        order_by_idx_hint: sql_order_by_idx_hint(sql),
        limit_stop_after: MatrixLimitStopAfter {
            possible: false,
            returned_limit: sql_clause_usize_value(sql, " LIMIT "),
            lookahead: sql_clause_usize_value(sql, " LIMIT ")
                .map_or(0, |limit| usize::from(limit > 0)),
            disabled_reason: Some("test_sample".to_string()),
            ..MatrixLimitStopAfter::default()
        },
        result_signature: Some("projection|PerfAuditHeapUser|id|1|1".to_string()),
        cursor_signature: None,
        compile_local_instructions: 1,
        compile_cache_key_local_instructions: 0,
        compile_cache_lookup_local_instructions: 0,
        compile_parse_local_instructions: 0,
        compile_parse_tokenize_local_instructions: 0,
        compile_parse_select_local_instructions: 0,
        compile_parse_expr_local_instructions: 0,
        compile_parse_predicate_local_instructions: 0,
        compile_aggregate_lane_check_local_instructions: 0,
        compile_prepare_local_instructions: 0,
        compile_lower_local_instructions: 0,
        compile_bind_local_instructions: 0,
        compile_cache_insert_local_instructions: 0,
        execute_local_instructions: total.saturating_sub(1),
        planner_local_instructions: 0,
        planner_schema_info_local_instructions: 0,
        planner_prepare_local_instructions: 0,
        planner_cache_key_local_instructions: 0,
        planner_cache_lookup_local_instructions: 0,
        planner_plan_build_local_instructions: 0,
        planner_cache_insert_local_instructions: 0,
        store_local_instructions: store,
        executor_local_instructions: total.saturating_sub(store),
        grouped_stream_local_instructions: 0,
        grouped_fold_local_instructions: 0,
        grouped_finalize_local_instructions: 0,
        scalar_aggregate_base_row_local_instructions: 0,
        scalar_aggregate_reducer_fold_local_instructions: 0,
        scalar_aggregate_expression_evaluations: 0,
        scalar_aggregate_filter_evaluations: 0,
        scalar_aggregate_rows_ingested: 0,
        scalar_aggregate_terminal_count: 0,
        scalar_aggregate_unique_input_expr_count: 0,
        scalar_aggregate_unique_filter_expr_count: 0,
        scalar_aggregate_sink_mode: None,
        pure_covering_decode_local_instructions: 0,
        pure_covering_row_assembly_local_instructions: 0,
        hybrid_covering_path_hits: 0,
        hybrid_covering_index_field_accesses: 0,
        hybrid_covering_row_field_accesses: 0,
        direct_data_row_scan_local_instructions: 0,
        direct_data_row_key_stream_local_instructions: 0,
        direct_data_row_row_read_local_instructions: 0,
        direct_data_row_key_encode_local_instructions: 0,
        direct_data_row_store_get_local_instructions: 0,
        direct_data_row_order_window_local_instructions: 0,
        direct_data_row_page_window_local_instructions: 0,
        kernel_row_scan_local_instructions: 0,
        kernel_row_key_stream_local_instructions: 0,
        kernel_row_row_read_local_instructions: 0,
        kernel_row_order_window_local_instructions: 0,
        kernel_row_page_window_local_instructions: 0,
        kernel_row_retained_layout_hits: 0,
        kernel_row_retained_slot_values: 0,
        kernel_row_retained_octet_length_values: 0,
        data_store_get_calls: 1,
        index_store_get_calls: 0,
        index_store_range_scan_calls: 0,
        index_store_entry_reads: 0,
        output_blob_values: 0,
        output_blob_bytes: 0,
        output_blob_hex_bytes: 0,
        sql_compiled_command_hits: 0,
        sql_compiled_command_misses: 1,
        shared_query_plan_hits: 0,
        shared_query_plan_misses: 1,
        total_local_instructions: total,
        outcome: MatrixOutcome {
            result_kind: "projection".to_string(),
            entity: "PerfAuditHeapUser".to_string(),
            row_count: 1,
        },
    }
}

fn route_classification_sample(sql: &str, family: &str) -> MatrixSample {
    MatrixSample {
        source: MatrixSource::Deterministic.label().to_string(),
        surface: MatrixSurface::User.label().to_string(),
        family: family.to_string(),
        sql: sql.to_string(),
        outcome: MatrixOutcome {
            result_kind: "projection".to_string(),
            entity: "PerfAuditUser".to_string(),
            row_count: 1,
        },
        ..MatrixSample::default()
    }
}

fn main_fixture_sample_with_kernel_scan(
    key: &str,
    surface: &str,
    total: u64,
    store: u64,
    sql: &str,
) -> MatrixSample {
    let mut sample = report_matrix_sample(key, surface, total, store, sql);
    sample.kernel_row_scan_local_instructions = total;
    sample.kernel_row_row_read_local_instructions = store;
    sample
}

fn main_fixture_sample_with_pure_covering(
    key: &str,
    surface: &str,
    total: u64,
    decode: u64,
    row_assembly: u64,
    sql: &str,
) -> MatrixSample {
    let mut sample = report_matrix_sample(key, surface, total, 0, sql);
    sample.pure_covering_decode_local_instructions = decode;
    sample.pure_covering_row_assembly_local_instructions = row_assembly;
    sample
}

fn main_fixture_sample_with_hybrid_covering(
    key: &str,
    surface: &str,
    total: u64,
    path_hits: u64,
    index_fields: u64,
    row_fields: u64,
    sql: &str,
) -> MatrixSample {
    let mut sample = report_matrix_sample(key, surface, total, 0, sql);
    sample.hybrid_covering_path_hits = path_hits;
    sample.hybrid_covering_index_field_accesses = index_fields;
    sample.hybrid_covering_row_field_accesses = row_fields;
    sample.data_store_get_calls = row_fields;
    sample
}

#[test]
fn sql_perf_matrix_reports_index_range_scan_hotspots() {
    let mut sample = report_matrix_sample(
        "token.collection_id.sparse_in.page_only.limit50",
        "token",
        240,
        30,
        "SELECT id FROM PerfAuditToken WHERE collection_id IN ('01KV5N439P0000000000000000', 'missing-collection-000') ORDER BY id ASC LIMIT 50",
    );
    sample.index_store_range_scan_calls = 251;
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        canister_wasm_profile: "test".to_string(),
        generated_scenario_count: 1,
        executed_scenario_count: 1,
        failed_scenario_count: 0,
        matrix_limit: 1,
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples: vec![sample],
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let range_scan_section = markdown
        .split("## Top Index Store Range Scans")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("index range-scan hotspot section should render");

    assert!(
        range_scan_section.contains("token.collection_id.sparse_in.page_only.limit50"),
        "range-scan hotspot section should include sparse IN scenarios",
    );
    assert!(
        range_scan_section.contains("| 251 |"),
        "range-scan hotspot section should expose index range traversal counts",
    );
}

#[test]
#[ignore = "PocketIC startup diagnostic; run manually with --ignored --nocapture"]
fn sql_perf_matrix_pocketic_startup_smoke() {
    eprintln!("sql_perf_matrix: resolving PocketIC binary");
    let pocket_ic_bin =
        try_ensure_pocket_ic_bin().expect("PocketIC binary should resolve for matrix run");
    eprintln!(
        "sql_perf_matrix: PocketIC binary {}",
        pocket_ic_bin.display()
    );
    eprintln!("sql_perf_matrix: acquiring PocketIC process lock");
    let _guard = try_acquire_pic_serial_guard().expect("PocketIC process lock should be acquired");
    eprintln!("sql_perf_matrix: PocketIC process lock acquired");
    eprintln!("sql_perf_matrix: starting fresh PocketIC instance");
    let pic = try_pic().expect("fresh PocketIC instance should start");
    eprintln!("sql_perf_matrix: fresh PocketIC instance started");
    let canister_id = pic.create_canister();
    eprintln!("sql_perf_matrix: created smoke canister {canister_id}");
}

#[test]
#[ignore = "optional SQLite comparison; run manually with sqlite3 and PocketIC"]
fn sql_perf_generated_matrix_compares_sqlite_reference_fixture() {
    let sqlite_path = sqlite3_path();
    let Ok(sqlite_version) = sqlite_version(&sqlite_path) else {
        eprintln!(
            "skipping SQLite audit comparison because `{}` is unavailable",
            sqlite_path.display()
        );
        return;
    };
    let db_path = sqlite_audit_db_path();
    setup_sqlite_audit_database(&sqlite_path, &db_path).unwrap_or_else(|err| {
        panic!(
            "failed to seed SQLite audit comparison database `{}`: {err}",
            db_path.display()
        )
    });

    eprintln!("sql_perf_sqlite: installing sql_perf fixture canister");
    let fixture = install_sql_perf_canister_fixture();
    eprintln!("sql_perf_sqlite: resetting and loading IcyDB fixture rows");
    reset_icydb_fixtures(&fixture);

    let scenarios = sqlite_audit_comparison_scenarios();
    let generated_scenario_count = deterministic_matrix().len();
    let timing_sample_count = sqlite_timing_sample_count();
    let mut comparisons = Vec::new();
    let mut failures = Vec::new();
    let verbose_progress = scenarios.len() <= 50;
    for (index, scenario) in scenarios.iter().enumerate() {
        if verbose_progress || index % 100 == 0 {
            eprintln!(
                "sql_perf_sqlite: comparing {}/{} {}",
                index + 1,
                scenarios.len(),
                scenario.key,
            );
        }
        match sqlite_audit_comparison_for_scenario(
            &fixture,
            &sqlite_path,
            &db_path,
            scenario,
            timing_sample_count,
        ) {
            Ok(comparison) => comparisons.push(comparison),
            Err(failure) => failures.push(*failure),
        }
    }
    let signature_mismatch_count = comparisons
        .iter()
        .filter(|scenario| !scenario.signatures_match)
        .count();

    let report = SqliteAuditComparisonReport {
        sqlite_version,
        sqlite_path: sqlite_path.display().to_string(),
        canister_wasm_profile: matrix_canister_wasm_profile().as_str().to_string(),
        generated_scenario_count,
        compared_scenario_count: scenarios.len(),
        common_success_count: comparisons.len(),
        icydb_failure_count: failures.len(),
        signature_mismatch_count,
        sample_count: timing_sample_count,
        scenario_key_filter: env::var(SQL_PERF_SQLITE_KEYS_ENV).ok(),
        fairness_notes: sqlite_audit_comparison_fairness_notes(),
        scenarios: comparisons,
        failures,
    };

    write_sqlite_audit_comparison_reports(&report);
    print_sqlite_audit_comparison_report(&report);

    let mismatches = report
        .scenarios
        .iter()
        .filter(|scenario| !scenario.signatures_match)
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    if sqlite_strict_enabled() {
        assert!(
            report.failures.is_empty(),
            "strict SQLite comparison should have no IcyDB failures: {:?}",
            report
                .failures
                .iter()
                .map(|failure| failure.key.as_str())
                .collect::<Vec<_>>(),
        );
        assert!(
            mismatches.is_empty(),
            "strict SQLite comparison signatures should match IcyDB for overlapping audit scenarios: {mismatches:?}",
        );
    }
}

#[test]
#[ignore = "expensive PocketIC hotspot scan; run manually with --ignored --nocapture"]
fn sql_perf_generated_matrix_reports_hotspots() {
    eprintln!("sql_perf_matrix: installing sql_perf fixture canister");
    let fixture = install_sql_perf_canister_fixture();
    eprintln!("sql_perf_matrix: resetting and loading fixture rows");
    reset_icydb_fixtures(&fixture);

    let mode = matrix_mode();
    let scenarios = generated_matrix(mode);
    let generated_scenario_count = scenarios.len();
    let scenario_key_filter = matrix_scenario_key_filter();
    let scenarios = filter_matrix_scenarios(scenarios, scenario_key_filter.as_deref());
    let matrix_limit = matrix_limit(scenarios.len());
    let selected = scenarios.into_iter().take(matrix_limit).collect::<Vec<_>>();
    eprintln!(
        "sql_perf_matrix: selected {} of {generated_scenario_count} generated scenarios",
        selected.len(),
    );
    let mut samples = Vec::new();
    let mut failures = Vec::new();
    for scenario in &selected {
        eprintln!("sql_perf_matrix: sampling {}", scenario.key);
        match sample_scenario(&fixture, scenario) {
            Ok(sample) => {
                eprintln!("sql_perf_matrix: sampled {}", scenario.key);
                samples.push(sample);
            }
            Err(failure) => {
                eprintln!("sql_perf_matrix: failed {}", scenario.key);
                failures.push(*failure);
            }
        }
    }
    let random_case_count = if mode == MatrixMode::Random {
        random_case_count()
    } else {
        0
    };

    let report = MatrixReport {
        matrix_mode: mode.label().to_string(),
        canister_wasm_profile: matrix_canister_wasm_profile().as_str().to_string(),
        generated_scenario_count,
        executed_scenario_count: samples.len(),
        failed_scenario_count: failures.len(),
        matrix_limit,
        scenario_key_filter,
        random_seed: (mode == MatrixMode::Random).then(random_seed),
        random_case_count,
        samples,
        failures,
    };

    write_matrix_reports(&report);
    print_matrix_summary(&report);
}

#[test]
#[ignore = "reads saved full-matrix reports; run manually after before/after matrix capture"]
fn sql_perf_generated_matrix_compares_saved_reports() {
    let baseline_path = matrix_delta_path_env("ICYDB_SQL_PERF_MATRIX_DELTA_BASELINE");
    let current_path = matrix_delta_path_env("ICYDB_SQL_PERF_MATRIX_DELTA_CURRENT");
    let output_stem = matrix_delta_output_stem();
    let focused_targets = matrix_delta_key_set_env("ICYDB_SQL_PERF_MATRIX_DELTA_FOCUSED_KEYS");
    let expected_improvements =
        matrix_delta_key_set_env("ICYDB_SQL_PERF_MATRIX_DELTA_EXPECTED_IMPROVEMENTS");
    let baseline = read_matrix_report(&baseline_path);
    let current = read_matrix_report(&current_path);
    let delta = matrix_delta_report(
        baseline_path.as_path(),
        &baseline,
        current_path.as_path(),
        &current,
        &focused_targets,
        &expected_improvements,
    );

    write_matrix_delta_reports(&delta, output_stem.as_path());
    println!("{}", matrix_delta_markdown(&delta));

    assert!(
        delta.closeout_failures.is_empty(),
        "matrix delta closeout gate failed: {:?}",
        delta.closeout_failures,
    );
}
