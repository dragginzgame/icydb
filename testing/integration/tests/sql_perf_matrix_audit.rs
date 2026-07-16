//! Module: sql_perf_matrix_audit
//! Responsibility: correctness-gated SQL performance matrix and differential evidence.
//! Does not own: production SQL semantics or shared scenario and verdict contracts.
//! Boundary: renders declared scenarios, executes audit surfaces, and emits typed evidence reports.

#[allow(
    dead_code,
    reason = "this performance target consumes only the rejection subset of the shared verdict"
)]
mod sql_harness;
mod sql_perf_baseline;
mod sql_perf_environment;
mod sql_perf_instrumentation;
mod sql_perf_measurement;
mod sql_perf_p1_shard;
mod sql_perf_p2;
mod sql_perf_p2_confirmation;
mod sql_perf_p2_shard;
mod sql_perf_phase;
mod sql_perf_profile;
mod sql_perf_receipt;
mod sql_perf_scale;
mod sql_perf_scale_baseline;
mod sql_perf_scale_shard;

use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashSet},
    env,
    fmt::Write as _,
    fs, io,
    path::{Path, PathBuf},
};

use candid::CandidType;
use ic_testkit::pic::{
    StandaloneCanisterFixture, try_acquire_pic_serial_guard, try_ensure_pocket_ic_bin, try_pic,
};
use icydb::{
    Error, ErrorOrigin,
    db::{EntitySchemaDescription, SqlQueryExecutionAttribution, sql::SqlQueryResult},
    diagnostic::{DiagnosticCode, ErrorClass},
};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildTarget, CanisterWasmProfile,
    build_fixture_canister_wasm_bytes_with_options, install_prebuilt_fixture_canister,
    reset_icydb_fixtures,
};
use serde::{Deserialize, Serialize};

use crate::sql_harness::{
    CorrectnessObservation, CorrectnessScenario, CorrectnessVerdict, DiagnosticFact,
    EligibleProvider, EvidenceStrength, ExpectedAcceptance, MutationKind, NullabilityClass,
    ObservedOutcome, PredicateFamily, QueryShape, RouteExpectation, RouteFact, RouteFamily,
    RouteObservation, RouteOutcome, RouteReason, RowOrder, ScenarioMetadata, StatementFamily,
    ValueTypeFamily, WindowSpec, correctness_verdict,
};
use crate::sql_perf_baseline::{
    P2BaselineVerdict, compare_performance_baseline, write_performance_baseline_comparison,
};
use crate::sql_perf_environment::{
    PerfEnvironmentIdentity, capture_perf_environment, validate_perf_environment,
};
use crate::sql_perf_instrumentation::{
    INSTRUMENTATION_SENTINEL_SCENARIO_ID, InstrumentationPathSample,
    build_instrumentation_calibration_report, read_instrumentation_calibration_report,
    write_instrumentation_calibration_report,
};
use crate::sql_perf_measurement::{
    PerformanceMeasurementCoverage, PerformanceMeasurementStatus, current_measurement_coverage,
};
use crate::sql_perf_p1_shard::{
    P1ShardArtifactError, P1ShardMergeError, P1ShardReport, build_p1_shard_report,
    merge_p1_shard_reports, read_p1_shard_report, validate_p1_shard_artifact_size,
    write_p1_shard_report,
};
use crate::sql_perf_p2::{
    P2SelectionRequirements, read_p2_candidate_selection, select_p2_candidates,
    write_p2_candidate_selection,
};
use crate::sql_perf_p2_confirmation::{
    P2WarmNotApplicableReason, P2WarmSampleInput, build_p2_confirmation,
};
use crate::sql_perf_p2_shard::{
    build_p2_shard_report, merge_p2_shard_reports, read_merged_p2_report, read_p2_shard_report,
    write_merged_p2_report, write_p2_shard_report,
};
use crate::sql_perf_phase::{
    PhaseOwnershipTable, PhaseReconciliation, current_phase_ownership, reconcile_phase,
};
use crate::sql_perf_profile::{PerformanceProfileError, SQL_PERFORMANCE_PROFILE};
use crate::sql_perf_receipt::{
    P1ReceiptError, P1ShardReceipt, build_p1_shard_receipts, p1_shard_receipt,
    validate_p1_shard_receipts,
};
use crate::sql_perf_scale::{
    ScaleEvidenceError, ScaleObservation, ScaleScenarioDeclaration, build_scale_observation,
    scale_scenario_declarations,
};
use crate::sql_perf_scale_shard::{
    MergedScaleShardReports, build_scale_shard_report, merge_scale_shard_reports,
    read_merged_scale_report, read_scale_shard_report, write_merged_scale_report,
    write_scale_shard_report,
};

const SQL_PERF_P1_SHARD_INDEX_ENV: &str = "ICYDB_SQL_PERF_P1_SHARD_INDEX";
const SQL_PERF_P1_SHARD_DIR_ENV: &str = "ICYDB_SQL_PERF_P1_SHARD_DIR";
const SQL_PERF_P2_SELECTION_PATH_ENV: &str = "ICYDB_SQL_PERF_P2_SELECTION_PATH";
const SQL_PERF_P2_SHARD_INDEX_ENV: &str = "ICYDB_SQL_PERF_P2_SHARD_INDEX";
const SQL_PERF_P2_SHARD_DIR_ENV: &str = "ICYDB_SQL_PERF_P2_SHARD_DIR";
const SQL_PERF_P2_REPORT_PATH_ENV: &str = "ICYDB_SQL_PERF_P2_REPORT_PATH";
const SQL_PERF_BASELINE_PATH_ENV: &str = "ICYDB_SQL_PERF_BASELINE_PATH";
const SQL_PERF_CURRENT_PATH_ENV: &str = "ICYDB_SQL_PERF_CURRENT_PATH";
const SQL_PERF_COMPARISON_PATH_ENV: &str = "ICYDB_SQL_PERF_COMPARISON_PATH";
const SQL_PERF_INSTRUMENTATION_REPORT_PATH_ENV: &str = "ICYDB_SQL_PERF_INSTRUMENTATION_REPORT_PATH";
const SQL_PERF_SCALE_BASELINE_PATH_ENV: &str = "ICYDB_SQL_PERF_SCALE_BASELINE_PATH";
const SQL_PERF_SCALE_CURRENT_PATH_ENV: &str = "ICYDB_SQL_PERF_SCALE_CURRENT_PATH";
const SQL_PERF_SCALE_SHARD_INDEX_ENV: &str = "ICYDB_SQL_PERF_SCALE_SHARD_INDEX";
const SQL_PERF_SCALE_SHARD_DIR_ENV: &str = "ICYDB_SQL_PERF_SCALE_SHARD_DIR";
const SQL_PERF_SCALE_REPORT_PATH_ENV: &str = "ICYDB_SQL_PERF_SCALE_REPORT_PATH";

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

///
/// ScalePayloadProfile
///
/// Exact blob-payload distribution reported by the audit canister's scale loader.
/// Owned by the scale fixture boundary and validated before query sampling.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ScalePayloadProfile {
    /// The selected surface has no blob payload fields.
    NotApplicable,

    /// Thumbnail lengths cycle through 32/64/128/256 bytes and chunk lengths
    /// cycle through 256/512/1,024/2,048 bytes.
    BlobCycleV1,
}

///
/// ScaleFixtureFacts
///
/// Realized deterministic distribution facts returned by one scale-fixture load.
/// Owned by the audit canister and checked against each sentinel declaration.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScaleFixtureFacts {
    /// Current hard-cut scale-fixture format version.
    profile_version: u32,

    /// Stable audit surface name loaded into the otherwise-empty canister.
    surface: String,

    /// Exact number of rows constructed and inserted for the surface.
    fixture_rows: u32,

    /// Rows matching the surface's declared impossible predicate.
    zero_match_rows: u32,

    /// Rows matching the surface's declared exact-key predicate.
    one_match_rows: u32,

    /// Rows matching the surface's declared quarter-selectivity predicate.
    quarter_match_rows: u32,

    /// Rows matching the surface's declared all-row predicate.
    all_match_rows: u32,

    /// Exact blob payload distribution, or typed non-applicability.
    payload_profile: ScalePayloadProfile,
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

    const fn warm_method(self) -> &'static str {
        match self {
            Self::Account => "warm_account_query_with_perf",
            Self::Blob => "warm_blob_query_with_perf",
            Self::HeapUser => "warm_heap_user_query_with_perf",
            Self::JournaledUser => "warm_journaled_user_query_with_perf",
            Self::Token => "warm_token_query_with_perf",
            Self::User => "warm_user_query_with_perf",
        }
    }

    const fn scale_load_method(self) -> &'static str {
        match self {
            Self::Account => "load_account_scale_fixture",
            Self::Blob => "load_blob_scale_fixture",
            Self::HeapUser => "load_heap_user_scale_fixture",
            Self::JournaledUser => "load_journaled_user_scale_fixture",
            Self::Token => "load_token_scale_fixture",
            Self::User => "load_user_scale_fixture",
        }
    }

    // This is the exact row cardinality loaded by the current SQL performance
    // fixture for each surface. P2 normalization consumes the fact directly;
    // it never infers fixture size from SQL text or returned rows.
    const fn fixture_row_count(self) -> u64 {
        match self {
            Self::Account | Self::Blob | Self::User => 6,
            Self::HeapUser | Self::JournaledUser => 512,
            Self::Token => 260,
        }
    }
}

///
/// ProjectionFragment
///
/// SQL projection payload paired with typed value and reference-provider metadata.
/// Owned by the performance matrix renderer and used only to construct declared scenarios.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProjectionFragment {
    key: &'static str,
    sql: &'static str,
    value_type: ValueTypeFamily,
    sqlite_eligible: bool,
}

///
/// PredicateFragment
///
/// SQL predicate payload paired with typed semantic and route metadata.
/// Owned by the performance matrix renderer and never used as classification authority.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PredicateFragment {
    key: &'static str,
    sql: &'static str,
    family: PredicateFamily,
    route: PredicateRoute,
}

///
/// OrderFragment
///
/// SQL ordering payload paired with its typed route identity.
/// Owned by the performance matrix renderer and used only to construct declared scenarios.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrderFragment {
    key: &'static str,
    sql: &'static str,
    route: OrderRoute,
}

///
/// PredicateRoute
///
/// Typed predicate route identity used to derive scenario route expectations.
/// Owned by the performance matrix renderer and declared independently of SQL text.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PredicateRoute {
    Active,
    Age,
    All,
    Bucket,
    FieldComparison,
    HandleActive,
    Label,
    LowerHandleActive,
    LowerName,
    Name,
    PrimaryKey,
    Score,
    TierActive,
}

///
/// OrderRoute
///
/// Typed ordering route identity used to derive scenario route expectations.
/// Owned by the performance matrix renderer and declared independently of SQL text.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrderRoute {
    Age,
    Bucket,
    BucketLabel,
    Handle,
    Label,
    LowerHandle,
    LowerName,
    Name,
    Primary,
    TierHandle,
    UnsupportedExpression,
}

/// Performance-matrix scenario using the shared correctness scenario contract.
type MatrixScenario = CorrectnessScenario<MatrixSurface>;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MatrixOutcome {
    result_kind: String,
    entity: String,
    row_count: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MatrixLimitStopAfter {
    possible: bool,
    returned_limit: Option<usize>,
    lookahead: usize,
    stopped_after_matches: Option<u64>,
    stopped_after_index_entries: Option<u64>,
    disabled_reason: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MatrixSample {
    key: String,
    surface: String,
    family: String,
    sql: String,
    fixture_row_count: u64,
    route_family: String,
    route_outcome: String,
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
    executor_invocation_local_instructions: u64,
    executor_local_instructions: u64,
    response_finalization_local_instructions: u64,
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
    total_phase_reconciliation: PhaseReconciliation,
    compile_phase_reconciliation: PhaseReconciliation,
    execute_phase_reconciliation: PhaseReconciliation,
    planner_phase_reconciliation: PhaseReconciliation,
    executor_invocation_phase_reconciliation: PhaseReconciliation,
    outcome: MatrixOutcome,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MatrixFailure {
    key: String,
    surface: String,
    family: String,
    sql: String,
    route_family: String,
    route_outcome: String,
    route_reason: String,
    code: u16,
    diagnostic_code: u16,
    diagnostic_label: String,
    class: String,
    origin: String,
    correctness_failure_owner: String,
    correctness_mismatch_category: String,
}

///
/// ScaleScenarioRunError
///
/// Typed failure while loading or sampling one isolated scale scenario.
/// Owned by the scale runner and retained without collapsing product or evidence causes.
///

#[derive(Debug)]
enum ScaleScenarioRunError {
    /// The canister rejected its deterministic scale-fixture load.
    FixtureLoad(Error),

    /// SQL execution produced a typed product/correctness failure.
    Query(Box<MatrixFailure>),

    /// The live sample disagreed with the reviewed scale declaration.
    Evidence(ScaleEvidenceError),
}

impl std::fmt::Display for ScaleScenarioRunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FixtureLoad(error) => write!(formatter, "scale fixture load failed: {error}"),
            Self::Query(failure) => write!(
                formatter,
                "scale query {:?} failed with diagnostic {} ({})",
                failure.key, failure.diagnostic_label, failure.diagnostic_code,
            ),
            Self::Evidence(error) => write!(formatter, "invalid scale evidence: {error}"),
        }
    }
}

impl std::error::Error for ScaleScenarioRunError {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MatrixReport {
    performance_profile_version: u32,
    expected_scenario_set_hash: String,
    observed_scenario_set_hash: String,
    broad_scan_complete: bool,
    canister_wasm_profile: String,
    environment: PerfEnvironmentIdentity,
    measurement_coverage: PerformanceMeasurementCoverage,
    declared_scenario_count: usize,
    successful_scenario_count: usize,
    failed_scenario_count: usize,
    phase_ownership: PhaseOwnershipTable,
    p1_shard_receipts: Vec<P1ShardReceipt>,
    samples: Vec<MatrixSample>,
    failures: Vec<MatrixFailure>,
}

/// Typed failure when a matrix report is not complete current-profile evidence.
#[derive(Debug)]
enum MatrixReportValidationError {
    /// The report names a performance profile version other than the current one.
    ProfileVersion {
        /// Current checked-in version.
        expected: u32,
        /// Reported version.
        actual: u32,
    },
    /// One report scenario-set identity differs from the current profile.
    ScenarioSetHash {
        /// Identity field being validated.
        field: &'static str,
        /// Current checked-in hash.
        expected: &'static str,
        /// Reported hash.
        actual: String,
    },
    /// The report does not claim a complete P1 broad scan.
    IncompleteBroadScan,
    /// The report was not measured with the required canister profile.
    UnsupportedWasmProfile(String),
    /// The complete captured performance environment is invalid.
    InvalidEnvironment(crate::sql_perf_environment::PerfEnvironmentError),
    /// The report's phase-ownership table differs from the current schema.
    PhaseOwnershipDrift,
    /// The report's measured/unmeasured resource table differs from current authority.
    MeasurementCoverageDrift,
    /// One sample's serialized reconciliation differs from its raw counters.
    PhaseReconciliationDrift(String),
    /// The report's declared scenario count differs from the profile.
    DeclaredScenarioCount {
        /// Current checked-in count.
        expected: usize,
        /// Reported count.
        actual: usize,
    },
    /// The successful count does not match the serialized sample vector.
    SuccessfulScenarioCount {
        /// Serialized sample count.
        expected: usize,
        /// Reported successful count.
        actual: usize,
    },
    /// The failed count does not match the serialized failure vector.
    FailedScenarioCount {
        /// Serialized failure count.
        expected: usize,
        /// Reported failed count.
        actual: usize,
    },
    /// Serialized success and failure identities do not form the exact profile.
    InvalidOutcomeScenarioSet(PerformanceProfileError),
    /// Serialized P1 receipts do not form the exact eight-shard profile.
    InvalidReceipts(P1ReceiptError),
    /// Serialized receipts differ from receipts derived from serialized outcomes.
    ReceiptOutcomeDrift,
}

impl std::fmt::Display for MatrixReportValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProfileVersion { expected, actual } => write!(
                formatter,
                "performance profile version drifted: expected {expected}, observed {actual}",
            ),
            Self::ScenarioSetHash {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "{field} drifted: expected {expected}, observed {actual}",
            ),
            Self::IncompleteBroadScan => formatter.write_str("P1 broad scan is incomplete"),
            Self::UnsupportedWasmProfile(profile) => {
                write!(formatter, "unsupported canister wasm profile {profile:?}")
            }
            Self::InvalidEnvironment(error) => {
                write!(formatter, "invalid matrix environment: {error}")
            }
            Self::PhaseOwnershipDrift => {
                formatter.write_str("performance phase-ownership table drifted")
            }
            Self::MeasurementCoverageDrift => {
                formatter.write_str("performance measurement coverage drifted")
            }
            Self::PhaseReconciliationDrift(scenario_id) => write!(
                formatter,
                "performance phase reconciliation drifted for scenario {scenario_id:?}",
            ),
            Self::DeclaredScenarioCount { expected, actual } => write!(
                formatter,
                "declared P1 scenario count drifted: expected {expected}, observed {actual}",
            ),
            Self::SuccessfulScenarioCount { expected, actual } => write!(
                formatter,
                "successful P1 scenario count drifted: expected {expected}, observed {actual}",
            ),
            Self::FailedScenarioCount { expected, actual } => write!(
                formatter,
                "failed P1 scenario count drifted: expected {expected}, observed {actual}",
            ),
            Self::InvalidOutcomeScenarioSet(error) => {
                write!(formatter, "invalid P1 outcome scenario set: {error}")
            }
            Self::InvalidReceipts(error) => write!(formatter, "invalid P1 receipts: {error}"),
            Self::ReceiptOutcomeDrift => {
                formatter.write_str("P1 receipts differ from the report's serialized outcomes")
            }
        }
    }
}

impl std::error::Error for MatrixReportValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidEnvironment(error) => Some(error),
            Self::InvalidOutcomeScenarioSet(error) => Some(error),
            Self::InvalidReceipts(error) => Some(error),
            Self::ProfileVersion { .. }
            | Self::ScenarioSetHash { .. }
            | Self::IncompleteBroadScan
            | Self::UnsupportedWasmProfile(_)
            | Self::PhaseOwnershipDrift
            | Self::MeasurementCoverageDrift
            | Self::PhaseReconciliationDrift(_)
            | Self::DeclaredScenarioCount { .. }
            | Self::SuccessfulScenarioCount { .. }
            | Self::FailedScenarioCount { .. }
            | Self::ReceiptOutcomeDrift => None,
        }
    }
}

/// Typed failure while encoding or publishing one matrix report.
#[derive(Debug)]
enum MatrixReportArtifactError {
    /// The in-memory report is not complete current-profile evidence.
    InvalidReport(MatrixReportValidationError),
    /// One artifact filesystem operation failed.
    Io {
        /// Artifact path.
        path: PathBuf,
        /// Stable operation description.
        operation: &'static str,
        /// Filesystem cause.
        source: io::Error,
    },
    /// The artifact exceeds the checked-in byte budget.
    TooLarge {
        /// Artifact path.
        path: PathBuf,
        /// Observed bytes, capped at one byte beyond the limit.
        observed_bytes: usize,
        /// Checked-in maximum bytes.
        max_bytes: usize,
    },
    /// The in-memory report could not be encoded as current JSON.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },
}

impl std::fmt::Display for MatrixReportArtifactError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidReport(error) => write!(formatter, "invalid matrix report: {error}"),
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "matrix report {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "matrix report {} exceeds its byte budget: observed at least {observed_bytes}, maximum {max_bytes}",
                path.display(),
            ),
            Self::Encode { path, source } => write!(
                formatter,
                "matrix report {} could not be encoded: {source}",
                path.display(),
            ),
        }
    }
}

impl std::error::Error for MatrixReportArtifactError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidReport(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::Encode { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

///
/// PerfShardSelectionError
///
/// Typed failure while selecting one independently executable performance shard.
/// Owned by the matrix runner and shared by its P1 and P2 command boundaries.
///

#[derive(Debug)]
enum PerfShardSelectionError {
    /// The required shard-index environment variable is absent or not Unicode.
    Environment {
        /// Required environment variable.
        variable: &'static str,
        /// Environment lookup cause.
        source: std::env::VarError,
    },

    /// The shard-index value is not an unsigned eight-bit integer.
    InvalidNumber {
        /// Required environment variable.
        variable: &'static str,
        /// Invalid environment value.
        value: String,
        /// Integer parsing cause.
        source: std::num::ParseIntError,
    },

    /// The parsed shard index is outside the checked-in range.
    OutOfRange {
        /// Stable performance stage label.
        stage: &'static str,
        /// Parsed zero-based shard index.
        shard_index: u8,
        /// Checked-in shard count.
        shard_count: u8,
    },
}

impl std::fmt::Display for PerfShardSelectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Environment { variable, source } => {
                write!(
                    formatter,
                    "{variable} must select one performance shard: {source}"
                )
            }
            Self::InvalidNumber {
                variable,
                value,
                source,
            } => write!(
                formatter,
                "{variable} value {value:?} is not a shard index: {source}",
            ),
            Self::OutOfRange {
                stage,
                shard_index,
                shard_count,
            } => write!(
                formatter,
                "{stage} shard index {shard_index} is outside checked-in shard count {shard_count}",
            ),
        }
    }
}

impl std::error::Error for PerfShardSelectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Environment { source, .. } => Some(source),
            Self::InvalidNumber { source, .. } => Some(source),
            Self::OutOfRange { .. } => None,
        }
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

const fn token_primary_page_metadata(
    limit: usize,
    value_type: ValueTypeFamily,
    predicate: PredicateFamily,
    route: RouteExpectation,
    sqlite_eligible: bool,
) -> ScenarioMetadata {
    read_metadata(
        &["select.scalar_rows"],
        QueryShape::Scalar,
        value_type,
        predicate,
        WindowSpec::ordered(limit, 0, "id ASC"),
        route,
        sqlite_eligible,
    )
}

const fn token_count_metadata(predicate: PredicateFamily) -> ScenarioMetadata {
    read_metadata(
        &["select.global_aggregate"],
        QueryShape::GlobalAggregate,
        ValueTypeFamily::Numeric,
        predicate,
        WindowSpec::NONE,
        not_paginated_route(),
        true,
    )
}

fn token_branch_page_hotspots() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "token.collection_stage_id.branch_set.page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Numeric,
                PredicateFamily::Compound,
                secondary_index_route(),
                true,
            ),
        ),
        scenario(
            "token.collection_stage_id.branch_set.covering_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.covering_page_only",
            token_branch_page_sql("id, collection_id, stage", TOKEN_BRANCH_STAGES, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Mixed,
                PredicateFamily::Compound,
                secondary_index_route(),
                true,
            ),
        ),
        scenario(
            "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Mixed,
                PredicateFamily::Compound,
                secondary_index_route(),
                true,
            ),
        ),
        scenario(
            "token.collection_stage_id.branch_set.full_entity.limit50",
            MatrixSurface::Token,
            "route.branch_set.full_entity",
            token_branch_page_sql("*", TOKEN_BRANCH_STAGES, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Mixed,
                PredicateFamily::Compound,
                secondary_index_route(),
                false,
            ),
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
            token_primary_page_metadata(
                3,
                ValueTypeFamily::Mixed,
                PredicateFamily::Compound,
                secondary_index_route(),
                true,
            ),
        ),
        scenario(
            "token.collection_stage_id.prefixed_stage_range.page_only.limit50",
            MatrixSurface::Token,
            "route.prefixed_range.page_only",
            token_prefixed_stage_range_page_sql("id", 50),
            read_metadata(
                &["select.scalar_rows"],
                QueryShape::Scalar,
                ValueTypeFamily::Numeric,
                PredicateFamily::Range,
                WindowSpec::ordered(50, 0, "stage ASC, id ASC"),
                secondary_index_route(),
                true,
            ),
        ),
    ]
}

fn token_branch_count_and_wide_hotspots() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "token.collection_stage_id.branch_set.count",
            MatrixSurface::Token,
            "route.branch_set.count",
            token_branch_count_sql(TOKEN_BRANCH_STAGES),
            token_count_metadata(PredicateFamily::Compound),
        ),
        scenario(
            "token.collection_stage_id.branch_set.duplicate_count",
            MatrixSurface::Token,
            "route.branch_set.duplicate_count",
            token_branch_count_sql(TOKEN_BRANCH_STAGES_WITH_DUPLICATE),
            token_count_metadata(PredicateFamily::Compound),
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.wide_page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES_WIDE, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Numeric,
                PredicateFamily::Compound,
                secondary_index_route(),
                true,
            ),
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.wide_noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES_WIDE, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Mixed,
                PredicateFamily::Compound,
                secondary_index_route(),
                true,
            ),
        ),
    ]
}

fn token_sparse_lookup_hotspots() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "token.collection_id.sparse_in.page_only.limit50",
            MatrixSurface::Token,
            "route.sparse_in.page_only",
            token_sparse_collection_in_page_sql(250, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Numeric,
                PredicateFamily::SparseMembership,
                equality_prefix_route(),
                true,
            ),
        ),
        scenario(
            "token.collection_id.sparse_in.count",
            MatrixSurface::Token,
            "route.sparse_in.count",
            token_sparse_collection_in_count_sql(250),
            token_count_metadata(PredicateFamily::SparseMembership),
        ),
    ]
}

fn token_branch_route_hotspot_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = token_branch_page_hotspots();
    scenarios.extend(token_branch_count_and_wide_hotspots());
    scenarios.extend(token_branch_over_cap_hotspot_matrix());
    scenarios.extend(token_sparse_lookup_hotspots());
    scenarios
}

fn token_branch_over_cap_hotspot_matrix() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "token.collection_stage_id.overcap_fallback.page_only.limit50",
            MatrixSurface::Token,
            "route.branch_over_cap.page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES_OVER_CAP, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Numeric,
                PredicateFamily::Compound,
                materialized_sort_route(),
                true,
            ),
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
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Numeric,
                PredicateFamily::Compound,
                secondary_index_route(),
                true,
            ),
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_over_cap.noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES_OVER_CAP, 50),
            token_primary_page_metadata(
                50,
                ValueTypeFamily::Mixed,
                PredicateFamily::Compound,
                materialized_sort_route(),
                true,
            ),
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
    projections: &[ProjectionFragment],
    predicates: &[PredicateFragment],
    orders: &[OrderFragment],
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
                        surface,
                        family,
                        sql,
                        metadata: scalar_select_metadata(
                            surface,
                            projection,
                            predicate,
                            order,
                            usize::try_from(*limit).unwrap_or(usize::MAX),
                        ),
                    });
                }
            }
        }
    }

    scenarios
}

const fn projection(
    key: &'static str,
    sql: &'static str,
    value_type: ValueTypeFamily,
    sqlite_eligible: bool,
) -> ProjectionFragment {
    ProjectionFragment {
        key,
        sql,
        value_type,
        sqlite_eligible,
    }
}

const fn predicate(
    key: &'static str,
    sql: &'static str,
    family: PredicateFamily,
    route: PredicateRoute,
) -> PredicateFragment {
    PredicateFragment {
        key,
        sql,
        family,
        route,
    }
}

const fn order(key: &'static str, sql: &'static str, route: OrderRoute) -> OrderFragment {
    OrderFragment { key, sql, route }
}

const fn read_metadata(
    contract_features: &'static [&'static str],
    shape: QueryShape,
    value_type: ValueTypeFamily,
    predicate: PredicateFamily,
    window: WindowSpec,
    route: RouteExpectation,
    sqlite_eligible: bool,
) -> ScenarioMetadata {
    ScenarioMetadata {
        contract_features,
        provider_id: if sqlite_eligible {
            "perf.matrix.sqlite"
        } else {
            "perf.matrix.contract"
        },
        provider: if sqlite_eligible {
            EligibleProvider::SqliteReference
        } else {
            EligibleProvider::IcyDbContractOnly
        },
        evidence_strength: if sqlite_eligible {
            EvidenceStrength::ReferenceOracle
        } else {
            EvidenceStrength::ContractAssertion
        },
        statement: StatementFamily::Select,
        shape,
        value_type,
        nullability: NullabilityClass::NonNullable,
        predicate,
        window,
        mutation: MutationKind::None,
        row_order: RowOrder::Ordered,
        route,
        required_route: None,
        expected: ExpectedAcceptance::Accepted,
    }
}

const fn metadata_statement(
    contract_features: &'static [&'static str],
    statement: StatementFamily,
) -> ScenarioMetadata {
    ScenarioMetadata {
        contract_features,
        provider_id: "perf.matrix.contract",
        provider: EligibleProvider::IcyDbContractOnly,
        evidence_strength: EvidenceStrength::ContractAssertion,
        statement,
        shape: QueryShape::Metadata,
        value_type: ValueTypeFamily::Catalog,
        nullability: NullabilityClass::NotApplicable,
        predicate: PredicateFamily::None,
        window: WindowSpec::NONE,
        mutation: MutationKind::None,
        row_order: RowOrder::Ordered,
        route: not_paginated_route(),
        required_route: None,
        expected: ExpectedAcceptance::Accepted,
    }
}

const fn not_paginated_route() -> RouteExpectation {
    RouteExpectation::Fixed(RouteFact::new(
        RouteFamily::NotOrderedOrNotPaginated,
        RouteOutcome::UnchangedOrNotApplicable,
        RouteReason::NotAPaginatedSelect,
    ))
}

const fn grouped_materialized_route() -> RouteExpectation {
    RouteExpectation::Fixed(RouteFact::new(
        RouteFamily::MaterializedOrder,
        RouteOutcome::Materialized,
        RouteReason::GroupedAggregateMaterialized,
    ))
}

const fn residual_ordered_route() -> RouteExpectation {
    RouteExpectation::Fixed(RouteFact::new(
        RouteFamily::ResidualFilterOrderedScan,
        RouteOutcome::ResidualUnbounded,
        RouteReason::ResidualFilterRequiresCandidateScan,
    ))
}

const fn secondary_index_route() -> RouteExpectation {
    RouteExpectation::IndexOrder {
        family: RouteFamily::SecondaryOrder,
        candidate_reason: RouteReason::SecondaryOrderCandidate,
        pushed_reason: RouteReason::SecondaryOrderLimitStopProven,
    }
}

const fn equality_prefix_route() -> RouteExpectation {
    RouteExpectation::IndexOrder {
        family: RouteFamily::EqualityPrefixOrderedSuffix,
        candidate_reason: RouteReason::EqualityPrefixOrderedSuffixCandidate,
        pushed_reason: RouteReason::EqualityPrefixOrderedSuffixLimitStopProven,
    }
}

const fn materialized_sort_route() -> RouteExpectation {
    RouteExpectation::Fixed(RouteFact::new(
        RouteFamily::MaterializedOrder,
        RouteOutcome::Materialized,
        RouteReason::RequiresMaterializedSort,
    ))
}

fn scalar_select_metadata(
    surface: MatrixSurface,
    projection: &ProjectionFragment,
    predicate: &PredicateFragment,
    order: &OrderFragment,
    limit: usize,
) -> ScenarioMetadata {
    let sqlite_eligible = projection.sqlite_eligible
        && !matches!(
            surface,
            MatrixSurface::HeapUser | MatrixSurface::JournaledUser
        );
    scalar_select_metadata_from_facts(
        surface,
        projection.value_type,
        sqlite_eligible,
        predicate.family,
        predicate.route,
        order,
        limit,
    )
}

fn scalar_select_metadata_from_facts(
    surface: MatrixSurface,
    value_type: ValueTypeFamily,
    sqlite_eligible: bool,
    predicate_family: PredicateFamily,
    predicate_route: PredicateRoute,
    order: &OrderFragment,
    limit: usize,
) -> ScenarioMetadata {
    read_metadata(
        &["select.scalar_rows"],
        QueryShape::Scalar,
        value_type,
        predicate_family,
        WindowSpec::ordered(limit, 0, order.sql),
        select_route_expectation(surface, predicate_route, order.route),
        sqlite_eligible,
    )
}

fn select_route_expectation(
    surface: MatrixSurface,
    predicate: PredicateRoute,
    order: OrderRoute,
) -> RouteExpectation {
    if order == OrderRoute::Primary {
        return RouteExpectation::PrimaryOrder {
            candidate_reason: if matches!(
                surface,
                MatrixSurface::HeapUser | MatrixSurface::JournaledUser
            ) {
                RouteReason::StorageMirrorPrimaryOrderCandidate
            } else {
                RouteReason::PrimaryOrderCandidate
            },
            residual_filter: !matches!(predicate, PredicateRoute::All | PredicateRoute::PrimaryKey),
        };
    }
    if matches!(
        surface,
        MatrixSurface::HeapUser | MatrixSurface::JournaledUser
    ) {
        return RouteExpectation::Fixed(RouteFact::new(
            RouteFamily::MaterializedOrder,
            RouteOutcome::Materialized,
            RouteReason::StorageMirrorHasPrimaryIndexOnly,
        ));
    }
    if order == OrderRoute::UnsupportedExpression {
        return RouteExpectation::Fixed(RouteFact::new(
            RouteFamily::UnsupportedAccessKind,
            RouteOutcome::Unsupported,
            RouteReason::OrderExpressionNotClassified,
        ));
    }
    if surface == MatrixSurface::Blob && order == OrderRoute::Bucket {
        return RouteExpectation::Fixed(RouteFact::new(
            RouteFamily::SecondaryOrder,
            RouteOutcome::MissingTieBreaker,
            RouteReason::IndexOrderSuffixGap,
        ));
    }
    if !predicate_order_is_compatible(predicate, order) {
        return RouteExpectation::Fixed(RouteFact::new(
            RouteFamily::IncompatibleFilterFirstOrder,
            RouteOutcome::Materialized,
            RouteReason::FilterOrderMismatch,
        ));
    }
    if matches!(
        predicate,
        PredicateRoute::Active
            | PredicateRoute::FieldComparison
            | PredicateRoute::HandleActive
            | PredicateRoute::LowerHandleActive
            | PredicateRoute::TierActive
    ) {
        return RouteExpectation::Fixed(RouteFact::new(
            RouteFamily::ResidualFilterOrderedScan,
            RouteOutcome::ResidualUnbounded,
            RouteReason::ResidualFilterRequiresCandidateScan,
        ));
    }

    RouteExpectation::IndexOrder {
        family: RouteFamily::SecondaryOrder,
        candidate_reason: RouteReason::SecondaryOrderCandidate,
        pushed_reason: RouteReason::SecondaryOrderLimitStopProven,
    }
}

fn predicate_order_is_compatible(predicate: PredicateRoute, order: OrderRoute) -> bool {
    match predicate {
        PredicateRoute::All => true,
        PredicateRoute::PrimaryKey
        | PredicateRoute::FieldComparison
        | PredicateRoute::Score
        | PredicateRoute::TierActive => false,
        PredicateRoute::Age => order == OrderRoute::Age,
        PredicateRoute::Name => order == OrderRoute::Name,
        PredicateRoute::LowerName => order == OrderRoute::LowerName,
        PredicateRoute::HandleActive => order == OrderRoute::Handle,
        PredicateRoute::LowerHandleActive => order == OrderRoute::LowerHandle,
        PredicateRoute::Bucket => matches!(order, OrderRoute::Bucket | OrderRoute::BucketLabel),
        PredicateRoute::Label => order == OrderRoute::Label,
        PredicateRoute::Active => order == OrderRoute::TierHandle,
    }
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

fn user_projections() -> Vec<ProjectionFragment> {
    vec![
        projection("pk", "id", ValueTypeFamily::Numeric, true),
        projection("narrow", "id, name", ValueTypeFamily::Mixed, true),
        projection(
            "wide",
            "id, name, age, age_nat, rank, active",
            ValueTypeFamily::Mixed,
            false,
        ),
        projection(
            "numeric_expr",
            "id, age + rank AS total",
            ValueTypeFamily::Numeric,
            true,
        ),
        projection(
            "text_expr",
            "id, LOWER(name) AS lower_name",
            ValueTypeFamily::Text,
            true,
        ),
    ]
}

fn user_predicates() -> Vec<PredicateFragment> {
    vec![
        predicate("all", "", PredicateFamily::None, PredicateRoute::All),
        predicate(
            "pk_range",
            "id >= 2",
            PredicateFamily::PrimaryKey,
            PredicateRoute::PrimaryKey,
        ),
        predicate(
            "age_range",
            "age >= 24 AND age < 40",
            PredicateFamily::Range,
            PredicateRoute::Age,
        ),
        predicate(
            "name_prefix",
            "name LIKE 'A%'",
            PredicateFamily::Prefix,
            PredicateRoute::Name,
        ),
        predicate(
            "lower_name_prefix",
            "LOWER(name) LIKE 'a%'",
            PredicateFamily::CasefoldPrefix,
            PredicateRoute::LowerName,
        ),
        predicate(
            "active_true",
            "active = true",
            PredicateFamily::Boolean,
            PredicateRoute::Active,
        ),
        predicate(
            "age_in",
            "age IN (24, 31, 43)",
            PredicateFamily::Membership,
            PredicateRoute::Age,
        ),
        predicate(
            "field_compare",
            "age > rank",
            PredicateFamily::FieldComparison,
            PredicateRoute::FieldComparison,
        ),
    ]
}

fn user_orders() -> Vec<OrderFragment> {
    vec![
        order("pk_asc", "id ASC", OrderRoute::Primary),
        order("pk_desc", "id DESC", OrderRoute::Primary),
        order("age_asc", "age ASC, id ASC", OrderRoute::Age),
        order("age_desc", "age DESC, id DESC", OrderRoute::Age),
        order("name_asc", "name ASC, id ASC", OrderRoute::Name),
        order(
            "lower_name_asc",
            "LOWER(name) ASC, id ASC",
            OrderRoute::LowerName,
        ),
        order(
            "numeric_expr_asc",
            "age + rank ASC, id ASC",
            OrderRoute::UnsupportedExpression,
        ),
    ]
}

fn account_projections() -> Vec<ProjectionFragment> {
    vec![
        projection("pk", "id", ValueTypeFamily::Numeric, true),
        projection("narrow", "id, handle", ValueTypeFamily::Mixed, true),
        projection(
            "wide",
            "id, handle, tier, active, score",
            ValueTypeFamily::Mixed,
            false,
        ),
        projection(
            "text_expr",
            "id, LOWER(handle) AS lower_handle",
            ValueTypeFamily::Text,
            true,
        ),
    ]
}

fn account_predicates() -> Vec<PredicateFragment> {
    vec![
        predicate("all", "", PredicateFamily::None, PredicateRoute::All),
        predicate(
            "active_true",
            "active = true",
            PredicateFamily::Boolean,
            PredicateRoute::Active,
        ),
        predicate(
            "tier_gold_active",
            "tier = 'gold' AND active = true",
            PredicateFamily::Compound,
            PredicateRoute::TierActive,
        ),
        predicate(
            "handle_prefix_active",
            "handle LIKE 'a%' AND active = true",
            PredicateFamily::Compound,
            PredicateRoute::HandleActive,
        ),
        predicate(
            "lower_handle_prefix_active",
            "LOWER(handle) LIKE 'a%' AND active = true",
            PredicateFamily::Compound,
            PredicateRoute::LowerHandleActive,
        ),
        predicate(
            "score_range",
            "score >= 20",
            PredicateFamily::Range,
            PredicateRoute::Score,
        ),
    ]
}

fn account_orders() -> Vec<OrderFragment> {
    vec![
        order("pk_asc", "id ASC", OrderRoute::Primary),
        order("handle_asc", "handle ASC, id ASC", OrderRoute::Handle),
        order("handle_desc", "handle DESC, id DESC", OrderRoute::Handle),
        order(
            "lower_handle_asc",
            "LOWER(handle) ASC, id ASC",
            OrderRoute::LowerHandle,
        ),
        order(
            "tier_handle_asc",
            "tier ASC, handle ASC, id ASC",
            OrderRoute::TierHandle,
        ),
    ]
}

fn blob_projections() -> Vec<ProjectionFragment> {
    vec![
        projection("pk", "id", ValueTypeFamily::Numeric, true),
        projection(
            "metadata",
            "id, label, bucket",
            ValueTypeFamily::Mixed,
            true,
        ),
        projection(
            "lengths",
            "id, label, OCTET_LENGTH(thumbnail), OCTET_LENGTH(chunk)",
            ValueTypeFamily::Mixed,
            false,
        ),
        projection(
            "thumbnail",
            "id, label, thumbnail",
            ValueTypeFamily::Blob,
            false,
        ),
        projection(
            "payload",
            "id, label, thumbnail, chunk",
            ValueTypeFamily::Blob,
            false,
        ),
    ]
}

fn blob_predicates() -> Vec<PredicateFragment> {
    vec![
        predicate("all", "", PredicateFamily::None, PredicateRoute::All),
        predicate(
            "bucket_eq",
            "bucket = 10",
            PredicateFamily::Range,
            PredicateRoute::Bucket,
        ),
        predicate(
            "bucket_range",
            "bucket >= 10 AND bucket < 40",
            PredicateFamily::Range,
            PredicateRoute::Bucket,
        ),
        predicate(
            "label_prefix",
            "label LIKE 'blob-%'",
            PredicateFamily::Prefix,
            PredicateRoute::Label,
        ),
    ]
}

fn blob_orders() -> Vec<OrderFragment> {
    vec![
        order("pk_asc", "id ASC", OrderRoute::Primary),
        order("bucket_asc", "bucket ASC, id ASC", OrderRoute::Bucket),
        order(
            "bucket_label_asc",
            "bucket ASC, label ASC, id ASC",
            OrderRoute::BucketLabel,
        ),
        order("label_asc", "label ASC, id ASC", OrderRoute::Label),
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

fn storage_mirror_projections() -> Vec<ProjectionFragment> {
    vec![
        projection("pk", "id", ValueTypeFamily::Numeric, false),
        projection("narrow", "id, name", ValueTypeFamily::Mixed, false),
        projection("wide", "id, name, age", ValueTypeFamily::Mixed, false),
    ]
}

fn storage_mirror_predicates() -> Vec<PredicateFragment> {
    vec![
        predicate("all", "", PredicateFamily::None, PredicateRoute::All),
        predicate(
            "pk_range",
            "id >= 2",
            PredicateFamily::PrimaryKey,
            PredicateRoute::PrimaryKey,
        ),
        predicate(
            "age_range",
            "age >= 24 AND age < 40",
            PredicateFamily::Range,
            PredicateRoute::Age,
        ),
        predicate(
            "name_range",
            "name >= 'a'",
            PredicateFamily::Range,
            PredicateRoute::Name,
        ),
    ]
}

fn storage_mirror_orders() -> Vec<OrderFragment> {
    vec![
        order("pk_asc", "id ASC", OrderRoute::Primary),
        order("pk_desc", "id DESC", OrderRoute::Primary),
        order("age_asc", "age ASC, id ASC", OrderRoute::Age),
        order("name_asc", "name ASC, id ASC", OrderRoute::Name),
    ]
}

fn user_global_aggregate_scenarios() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "user.aggregate.count_all",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser",
            read_metadata(
                &["select.global_aggregate"],
                QueryShape::GlobalAggregate,
                ValueTypeFamily::Numeric,
                PredicateFamily::None,
                WindowSpec::NONE,
                not_paginated_route(),
                true,
            ),
        ),
        scenario(
            "user.aggregate.count_active",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE active = true",
            read_metadata(
                &["select.global_aggregate"],
                QueryShape::GlobalAggregate,
                ValueTypeFamily::Numeric,
                PredicateFamily::Boolean,
                WindowSpec::NONE,
                not_paginated_route(),
                true,
            ),
        ),
        scenario(
            "user.aggregate.count_age_in",
            MatrixSurface::User,
            "aggregate.count_in",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE age IN (24, 31, 43)",
            read_metadata(
                &["select.global_aggregate"],
                QueryShape::GlobalAggregate,
                ValueTypeFamily::Numeric,
                PredicateFamily::Membership,
                WindowSpec::NONE,
                not_paginated_route(),
                true,
            ),
        ),
    ]
}

fn user_grouped_aggregate_scenarios() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "user.aggregate.group_age_count",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            read_metadata(
                &["select.grouped_aggregate"],
                QueryShape::Grouped,
                ValueTypeFamily::Numeric,
                PredicateFamily::None,
                WindowSpec::ordered(10, 0, "age ASC"),
                grouped_materialized_route(),
                true,
            ),
        ),
        scenario(
            "user.aggregate.group_active_avg_age",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT active, AVG(age) FROM PerfAuditUser GROUP BY active ORDER BY active ASC LIMIT 10",
            read_metadata(
                &["select.grouped_aggregate"],
                QueryShape::Grouped,
                ValueTypeFamily::Mixed,
                PredicateFamily::None,
                WindowSpec::ordered(10, 0, "active ASC"),
                grouped_materialized_route(),
                false,
            ),
        ),
        scenario(
            "user.aggregate.group_age_having_alias",
            MatrixSurface::User,
            "aggregate.grouped_having",
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
            read_metadata(
                &["select.grouped_aggregate", "having.grouped_aggregate"],
                QueryShape::Grouped,
                ValueTypeFamily::Numeric,
                PredicateFamily::Compound,
                WindowSpec::ordered(5, 0, "high_count DESC, age ASC"),
                grouped_materialized_route(),
                true,
            ),
        ),
    ]
}

fn account_aggregate_scenarios() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "account.aggregate.group_tier_count",
            MatrixSurface::Account,
            "aggregate.grouped",
            "SELECT tier, COUNT(*) FROM PerfAuditAccount WHERE active = true GROUP BY tier ORDER BY tier ASC LIMIT 10",
            read_metadata(
                &["select.grouped_aggregate"],
                QueryShape::Grouped,
                ValueTypeFamily::Mixed,
                PredicateFamily::Boolean,
                WindowSpec::ordered(10, 0, "tier ASC"),
                grouped_materialized_route(),
                true,
            ),
        ),
        scenario(
            "account.aggregate.count_active_tier_in",
            MatrixSurface::Account,
            "aggregate.count_in",
            "SELECT COUNT(*) FROM PerfAuditAccount WHERE active = true AND tier IN ('gold', 'silver')",
            read_metadata(
                &["select.global_aggregate"],
                QueryShape::GlobalAggregate,
                ValueTypeFamily::Numeric,
                PredicateFamily::Compound,
                WindowSpec::NONE,
                not_paginated_route(),
                true,
            ),
        ),
        scenario(
            "account.select.tier_membership.handle_order.limit3",
            MatrixSurface::Account,
            "select.compound_order_limit",
            "SELECT id, handle FROM PerfAuditAccount WHERE tier IN ('gold', 'silver') AND active = true ORDER BY handle ASC, id ASC LIMIT 3",
            read_metadata(
                &["select.scalar_rows"],
                QueryShape::Scalar,
                ValueTypeFamily::Mixed,
                PredicateFamily::Compound,
                WindowSpec::ordered(3, 0, "handle ASC, id ASC"),
                residual_ordered_route(),
                true,
            ),
        ),
    ]
}

fn blob_aggregate_scenarios() -> Vec<MatrixScenario> {
    vec![scenario(
        "blob.aggregate.count_bucket",
        MatrixSurface::Blob,
        "aggregate.count",
        "SELECT COUNT(*) FROM PerfAuditBlob WHERE bucket = 10",
        read_metadata(
            &["select.global_aggregate"],
            QueryShape::GlobalAggregate,
            ValueTypeFamily::Numeric,
            PredicateFamily::Range,
            WindowSpec::NONE,
            not_paginated_route(),
            true,
        ),
    )]
}

fn metadata_scenarios() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "user.metadata.explain_pk_limit",
            MatrixSurface::User,
            "metadata.explain",
            "EXPLAIN SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            metadata_statement(&["explain.query_delete"], StatementFamily::Explain),
        ),
        scenario(
            "user.metadata.describe",
            MatrixSurface::User,
            "metadata.describe",
            "DESCRIBE PerfAuditUser",
            metadata_statement(&["introspection.describe"], StatementFamily::Describe),
        ),
        scenario(
            "user.metadata.show_columns",
            MatrixSurface::User,
            "metadata.show_columns",
            "SHOW COLUMNS PerfAuditUser",
            metadata_statement(&["introspection.show_columns"], StatementFamily::Show),
        ),
        scenario(
            "user.metadata.show_indexes",
            MatrixSurface::User,
            "metadata.show_indexes",
            "SHOW INDEXES FROM PerfAuditUser",
            metadata_statement(&["introspection.show_indexes"], StatementFamily::Show),
        ),
        scenario(
            "user.metadata.show_entities",
            MatrixSurface::User,
            "metadata.show_entities",
            "SHOW ENTITIES",
            metadata_statement(&["introspection.show_entities"], StatementFamily::Show),
        ),
    ]
}

fn aggregate_and_metadata_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = user_global_aggregate_scenarios();
    scenarios.extend(user_grouped_aggregate_scenarios());
    scenarios.extend(account_aggregate_scenarios());
    scenarios.extend(blob_aggregate_scenarios());
    scenarios.extend(metadata_scenarios());
    scenarios
}

fn scenario(
    key: impl Into<String>,
    surface: MatrixSurface,
    family: impl Into<String>,
    sql: impl Into<String>,
    metadata: ScenarioMetadata,
) -> MatrixScenario {
    MatrixScenario {
        key: key.into(),
        surface,
        family: family.into(),
        sql: sql.into(),
        metadata,
    }
}

const fn top_n() -> usize {
    SQL_PERFORMANCE_PROFILE.confirmation_top_n_per_metric()
}

const fn matrix_canister_wasm_profile() -> CanisterWasmProfile {
    CanisterWasmProfile::WasmRelease
}

fn matrix_canister_build_options() -> CanisterBuildOptions {
    CanisterBuildOptions {
        profile: matrix_canister_wasm_profile(),
        build_target: CanisterBuildTarget::Local,
        ..CanisterBuildOptions::default()
    }
}

fn rejected_scenario_correctness_projection(
    scenario: &MatrixScenario,
    error_code: u16,
    diagnostic_code: u16,
) -> (String, String) {
    let verdict = correctness_verdict(
        scenario,
        &CorrectnessObservation {
            subject: ObservedOutcome::Rejected(DiagnosticFact {
                error_code,
                diagnostic_code,
            }),
            provider: None,
            route: None,
        },
    );
    let CorrectnessVerdict::Failed(failure) = verdict else {
        panic!(
            "admitted scenario `{}` rejection must fail correctness",
            scenario.key
        )
    };
    (
        failure.signature.owner.code().to_string(),
        failure.signature.category.code().to_string(),
    )
}

fn query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<SqlQueryPerfResult, Error> {
    fixture
        .query_call(scenario.surface.query_method(), (scenario.sql.clone(),))
        .unwrap_or_else(|err| panic!("{} should decode: {err}", scenario.surface.query_method()))
}

fn warm_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<SqlQueryPerfResult, Error> {
    fixture
        .update_call(scenario.surface.warm_method(), (scenario.sql.clone(),))
        .unwrap_or_else(|err| panic!("{} should decode: {err}", scenario.surface.warm_method()))
}

fn accepted_schema_descriptions(
    fixture: &StandaloneCanisterFixture,
) -> Result<Vec<EntitySchemaDescription>, Error> {
    fixture
        .query_call("accepted_schema_descriptions", ())
        .unwrap_or_else(|error| panic!("accepted schema descriptions should decode: {error}"))
}

fn capture_matrix_environment(
    fixture: &StandaloneCanisterFixture,
    wasm_bytes: &[u8],
) -> PerfEnvironmentIdentity {
    let accepted = accepted_schema_descriptions(fixture)
        .unwrap_or_else(|error| panic!("accepted schema descriptions failed: {error}"));
    let pocket_ic_binary = try_ensure_pocket_ic_bin()
        .unwrap_or_else(|error| panic!("PocketIC binary should resolve: {error}"));
    let identity = capture_perf_environment(
        SQL_PERFORMANCE_PROFILE,
        &workspace_root(),
        matrix_canister_wasm_profile().as_str(),
        wasm_bytes,
        &accepted,
        &pocket_ic_binary,
    )
    .unwrap_or_else(|error| panic!("performance environment capture failed: {error}"));
    validate_perf_environment(SQL_PERFORMANCE_PROFILE, &identity)
        .unwrap_or_else(|error| panic!("performance environment is invalid: {error}"));

    identity
}

fn capture_isolated_matrix_environment(wasm_bytes: &[u8]) -> PerfEnvironmentIdentity {
    let fixture = install_prebuilt_fixture_canister("sql_perf", wasm_bytes.to_vec());

    capture_matrix_environment(&fixture, wasm_bytes)
}

fn sample_isolated_attributed_instrumentation(
    wasm_bytes: &[u8],
    scenario: &MatrixScenario,
) -> InstrumentationPathSample {
    let fixture = install_prebuilt_fixture_canister("sql_perf", wasm_bytes.to_vec());
    reset_icydb_fixtures(&fixture);
    let perf: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_call("query_user_attributed_total_perf", (scenario.sql.clone(),))
        .unwrap_or_else(|error| {
            panic!("attributed instrumentation sentinel should decode: {error}")
        });
    let perf = perf.unwrap_or_else(|error| {
        panic!(
            "attributed instrumentation sentinel {} failed: {error}",
            scenario.key
        )
    });

    InstrumentationPathSample {
        result_signature: result_signature(&perf.result),
        instructions: perf.instructions,
    }
}

fn sample_isolated_total_only_instrumentation(
    wasm_bytes: &[u8],
    scenario: &MatrixScenario,
) -> InstrumentationPathSample {
    let fixture = install_prebuilt_fixture_canister("sql_perf", wasm_bytes.to_vec());
    reset_icydb_fixtures(&fixture);
    let perf: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_call("query_user_total_only_perf", (scenario.sql.clone(),))
        .unwrap_or_else(|error| {
            panic!("total-only instrumentation sentinel should decode: {error}")
        });
    let perf = perf.unwrap_or_else(|error| {
        panic!(
            "total-only instrumentation sentinel {} failed: {error}",
            scenario.key
        )
    });

    InstrumentationPathSample {
        result_signature: result_signature(&perf.result),
        instructions: perf.instructions,
    }
}

fn sample_isolated_p2_scenario(
    wasm_bytes: &[u8],
    scenario: &MatrixScenario,
    warm_mode: bool,
) -> MatrixSample {
    let fixture = install_prebuilt_fixture_canister("sql_perf", wasm_bytes.to_vec());
    reset_icydb_fixtures(&fixture);
    if warm_mode {
        warm_surface_with_perf(&fixture, scenario)
            .unwrap_or_else(|error| panic!("P2 warm-up for {} failed: {error}", scenario.key));
    }
    sample_scenario(&fixture, scenario)
        .unwrap_or_else(|failure| panic!("P2 sample for {} failed: {failure:?}", scenario.key))
}

fn load_scale_fixture(
    fixture: &StandaloneCanisterFixture,
    declaration: &ScaleScenarioDeclaration,
) -> Result<ScaleFixtureFacts, Error> {
    fixture
        .update_call(
            declaration.spec.surface.scale_load_method(),
            (declaration.fixture_rows,),
        )
        .unwrap_or_else(|error| {
            panic!(
                "{} should decode scale fixture facts: {error}",
                declaration.spec.surface.scale_load_method(),
            )
        })
}

fn sample_isolated_scale_scenario(
    wasm_bytes: &[u8],
    declaration: &ScaleScenarioDeclaration,
) -> Result<ScaleObservation, ScaleScenarioRunError> {
    let fixture = install_prebuilt_fixture_canister("sql_perf", wasm_bytes.to_vec());
    let facts =
        load_scale_fixture(&fixture, declaration).map_err(ScaleScenarioRunError::FixtureLoad)?;
    let sample =
        sample_scenario(&fixture, &declaration.scenario).map_err(ScaleScenarioRunError::Query)?;

    build_scale_observation(declaration, facts, sample).map_err(ScaleScenarioRunError::Evidence)
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
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        fixture_row_count: scenario.surface.fixture_row_count(),
        outcome: summarize_perf_outcome(&perf.result),
        ..MatrixSample::default()
    };
    fill_matrix_compile_sample(&mut sample, attribution);
    fill_matrix_execution_sample(&mut sample, attribution);
    fill_matrix_grouped_sample(&mut sample, attribution);
    fill_matrix_scalar_aggregate_sample(&mut sample, attribution);
    fill_matrix_projection_path_sample(&mut sample, attribution);
    fill_matrix_store_output_cache_sample(&mut sample, attribution);
    fill_matrix_phase_reconciliation(&mut sample);

    let route = route_fact_for_scenario(scenario, &sample);
    sample.route_family = route.family.code().to_string();
    sample.route_outcome = route.outcome.code().to_string();
    sample.route_reason = Some(route.reason.code().to_string());
    sample.order_by_idx_hint = scenario.metadata.window.order_hint.map(str::to_string);
    sample.limit_stop_after = limit_stop_after_for_scenario(scenario, &sample, route);
    sample.result_signature = Some(result_signature(&perf.result));
    sample.cursor_signature = cursor_signature(&perf.result);

    sample
}

fn route_fact_for_scenario(scenario: &MatrixScenario, sample: &MatrixSample) -> RouteFact {
    scenario.metadata.route.classify(
        scenario.metadata.window,
        RouteObservation {
            materialized_order: sample.direct_data_row_order_window_local_instructions != 0
                || sample.kernel_row_order_window_local_instructions != 0,
            data_store_get_calls: sample.data_store_get_calls,
            index_store_entry_reads: sample.index_store_entry_reads,
        },
    )
}

fn limit_stop_after_for_scenario(
    scenario: &MatrixScenario,
    sample: &MatrixSample,
    route: RouteFact,
) -> MatrixLimitStopAfter {
    let returned_limit = scenario.metadata.window.limit;
    let possible = route.outcome == RouteOutcome::Pushed;
    MatrixLimitStopAfter {
        possible,
        returned_limit,
        lookahead: returned_limit.map_or(0, |limit| usize::from(limit > 0)),
        stopped_after_matches: possible
            .then(|| u64::try_from(sample.outcome.row_count).unwrap_or(u64::MAX)),
        stopped_after_index_entries: possible.then_some(sample.index_store_entry_reads),
        disabled_reason: (!possible).then(|| {
            if scenario.metadata.window.limit.is_none() {
                "no_limit".to_string()
            } else if scenario.metadata.window.order_hint.is_none() {
                "no_order_by".to_string()
            } else {
                route.reason.code().to_string()
            }
        }),
    }
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
    sample.executor_invocation_local_instructions =
        execution.executor_invocation_local_instructions;
    sample.executor_local_instructions = execution.executor_local_instructions;
    sample.response_finalization_local_instructions =
        execution.response_finalization_local_instructions;
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

fn fill_matrix_phase_reconciliation(sample: &mut MatrixSample) {
    let [total, compile, execute, planner, executor_invocation] =
        expected_phase_reconciliations(sample);
    sample.total_phase_reconciliation = total;
    sample.compile_phase_reconciliation = compile;
    sample.execute_phase_reconciliation = execute;
    sample.planner_phase_reconciliation = planner;
    sample.executor_invocation_phase_reconciliation = executor_invocation;
}

fn expected_phase_reconciliations(sample: &MatrixSample) -> [PhaseReconciliation; 5] {
    [
        reconcile_phase(
            sample.total_local_instructions,
            &[
                sample.compile_local_instructions,
                sample.execute_local_instructions,
            ],
        ),
        reconcile_phase(
            sample.compile_local_instructions,
            &[
                sample.compile_cache_key_local_instructions,
                sample.compile_cache_lookup_local_instructions,
                sample.compile_parse_local_instructions,
                sample.compile_aggregate_lane_check_local_instructions,
                sample.compile_prepare_local_instructions,
                sample.compile_lower_local_instructions,
                sample.compile_bind_local_instructions,
                sample.compile_cache_insert_local_instructions,
            ],
        ),
        reconcile_phase(
            sample.execute_local_instructions,
            &[
                sample.planner_local_instructions,
                sample.store_local_instructions,
                sample.executor_local_instructions,
                sample.response_finalization_local_instructions,
            ],
        ),
        reconcile_phase(
            sample.planner_local_instructions,
            &[
                sample.planner_schema_info_local_instructions,
                sample.planner_prepare_local_instructions,
                sample.planner_cache_key_local_instructions,
                sample.planner_cache_lookup_local_instructions,
                sample.planner_plan_build_local_instructions,
                sample.planner_cache_insert_local_instructions,
            ],
        ),
        reconcile_phase(
            sample.executor_invocation_local_instructions,
            &[
                sample.store_local_instructions,
                sample.executor_local_instructions,
            ],
        ),
    ]
}

fn matrix_failure_from_error(scenario: &MatrixScenario, err: Error) -> MatrixFailure {
    let diagnostic_code = err.diagnostic_code();
    let error_code = err.code().raw();
    let diagnostic_error_code = diagnostic_code.error_code().raw();
    let (correctness_failure_owner, correctness_mismatch_category) =
        rejected_scenario_correctness_projection(scenario, error_code, diagnostic_error_code);
    MatrixFailure {
        key: scenario.key.clone(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        route_family: failed_route_family(),
        route_outcome: failed_route_outcome(),
        route_reason: failed_route_reason(),
        code: error_code,
        diagnostic_code: diagnostic_error_code,
        diagnostic_label: diagnostic_label(diagnostic_code).to_string(),
        class: error_class_label(err.class()).to_string(),
        origin: format!("{:?}", err.origin()),
        correctness_failure_owner,
        correctness_mismatch_category,
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

fn report_stem() -> PathBuf {
    env::var("ICYDB_SQL_PERF_MATRIX_OUT").map_or_else(
        |_| {
            workspace_root()
                .join("artifacts/perf-audit")
                .join("sql_perf_deterministic_matrix")
        },
        PathBuf::from,
    )
}

fn performance_shard_index(
    variable: &'static str,
    stage: &'static str,
) -> Result<u8, PerfShardSelectionError> {
    let value = env::var(variable)
        .map_err(|source| PerfShardSelectionError::Environment { variable, source })?;
    let shard_index =
        value
            .parse::<u8>()
            .map_err(|source| PerfShardSelectionError::InvalidNumber {
                variable,
                value: value.clone(),
                source,
            })?;
    let shard_count = SQL_PERFORMANCE_PROFILE.shard_count();
    if shard_index >= shard_count {
        return Err(PerfShardSelectionError::OutOfRange {
            stage,
            shard_index,
            shard_count,
        });
    }

    Ok(shard_index)
}

fn p1_shard_directory() -> PathBuf {
    env::var_os(SQL_PERF_P1_SHARD_DIR_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_p1_shards"),
        PathBuf::from,
    )
}

fn p1_shard_path(directory: &Path, shard_index: u8) -> PathBuf {
    directory.join(format!("p1-shard-{shard_index}.json"))
}

fn p2_selection_path() -> PathBuf {
    env::var_os(SQL_PERF_P2_SELECTION_PATH_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_p2_candidates.json"),
        PathBuf::from,
    )
}

fn p2_shard_directory() -> PathBuf {
    env::var_os(SQL_PERF_P2_SHARD_DIR_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_p2_shards"),
        PathBuf::from,
    )
}

fn p2_shard_path(directory: &Path, shard_index: u8) -> PathBuf {
    directory.join(format!("p2-shard-{shard_index}.json"))
}

fn p2_report_path() -> PathBuf {
    env::var_os(SQL_PERF_P2_REPORT_PATH_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_p2_report.json"),
        PathBuf::from,
    )
}

fn required_perf_artifact_path(variable: &'static str) -> PathBuf {
    env::var_os(variable).map_or_else(
        || panic!("{variable} must name one current-format performance artifact"),
        PathBuf::from,
    )
}

fn performance_comparison_path() -> PathBuf {
    env::var_os(SQL_PERF_COMPARISON_PATH_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_comparison.json"),
        PathBuf::from,
    )
}

fn instrumentation_report_path() -> PathBuf {
    env::var_os(SQL_PERF_INSTRUMENTATION_REPORT_PATH_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_instrumentation.json"),
        PathBuf::from,
    )
}

fn scale_shard_directory() -> PathBuf {
    env::var_os(SQL_PERF_SCALE_SHARD_DIR_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_scale_shards"),
        PathBuf::from,
    )
}

fn scale_shard_path(directory: &Path, shard_index: u8) -> PathBuf {
    directory.join(format!("scale-shard-{shard_index}.json"))
}

fn scale_report_path() -> PathBuf {
    env::var_os(SQL_PERF_SCALE_REPORT_PATH_ENV).map_or_else(
        || workspace_root().join("artifacts/perf-audit/sql_perf_scale_report.json"),
        PathBuf::from,
    )
}

fn merge_saved_scale_reports(scenarios: &[MatrixScenario]) -> MergedScaleShardReports {
    let directory = scale_shard_directory();
    let reports = (0..SQL_PERFORMANCE_PROFILE.shard_count())
        .map(|shard_index| {
            let path = scale_shard_path(&directory, shard_index);
            read_scale_shard_report(
                &path,
                SQL_PERFORMANCE_PROFILE,
                matrix_canister_wasm_profile().as_str(),
                scenarios,
            )
            .unwrap_or_else(|error| panic!("scale shard artifact failed: {error}"))
        })
        .collect::<Vec<_>>();
    let merged = merge_scale_shard_reports(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        scenarios,
        reports,
    )
    .unwrap_or_else(|error| panic!("scale shard merge failed: {error}"));
    let path = scale_report_path();
    write_merged_scale_report(
        &path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        scenarios,
        &merged,
    )
    .unwrap_or_else(|error| panic!("merged scale artifact failed: {error}"));

    merged
}

fn validate_matrix_report_for_publication(
    report: &MatrixReport,
) -> Result<(), MatrixReportValidationError> {
    let profile = SQL_PERFORMANCE_PROFILE;
    if report.performance_profile_version != profile.version() {
        return Err(MatrixReportValidationError::ProfileVersion {
            expected: profile.version(),
            actual: report.performance_profile_version,
        });
    }
    if report.expected_scenario_set_hash != profile.expected_scenario_set_hash() {
        return Err(MatrixReportValidationError::ScenarioSetHash {
            field: "expected scenario-set hash",
            expected: profile.expected_scenario_set_hash(),
            actual: report.expected_scenario_set_hash.clone(),
        });
    }
    if report.observed_scenario_set_hash != profile.expected_scenario_set_hash() {
        return Err(MatrixReportValidationError::ScenarioSetHash {
            field: "observed scenario-set hash",
            expected: profile.expected_scenario_set_hash(),
            actual: report.observed_scenario_set_hash.clone(),
        });
    }
    if !report.broad_scan_complete {
        return Err(MatrixReportValidationError::IncompleteBroadScan);
    }
    if report.canister_wasm_profile != CanisterWasmProfile::WasmRelease.as_str() {
        return Err(MatrixReportValidationError::UnsupportedWasmProfile(
            report.canister_wasm_profile.clone(),
        ));
    }
    validate_perf_environment(SQL_PERFORMANCE_PROFILE, &report.environment)
        .map_err(MatrixReportValidationError::InvalidEnvironment)?;
    if report.phase_ownership != current_phase_ownership() {
        return Err(MatrixReportValidationError::PhaseOwnershipDrift);
    }
    validate_matrix_measurement_coverage(report.measurement_coverage)?;
    if report.declared_scenario_count != profile.expected_scenario_count() {
        return Err(MatrixReportValidationError::DeclaredScenarioCount {
            expected: profile.expected_scenario_count(),
            actual: report.declared_scenario_count,
        });
    }
    if report.successful_scenario_count != report.samples.len() {
        return Err(MatrixReportValidationError::SuccessfulScenarioCount {
            expected: report.samples.len(),
            actual: report.successful_scenario_count,
        });
    }
    if report.failed_scenario_count != report.failures.len() {
        return Err(MatrixReportValidationError::FailedScenarioCount {
            expected: report.failures.len(),
            actual: report.failed_scenario_count,
        });
    }
    profile
        .validate_scenario_set(
            report
                .samples
                .iter()
                .map(|sample| sample.key.as_str())
                .chain(report.failures.iter().map(|failure| failure.key.as_str())),
        )
        .map_err(MatrixReportValidationError::InvalidOutcomeScenarioSet)?;
    for sample in &report.samples {
        let observed = [
            sample.total_phase_reconciliation,
            sample.compile_phase_reconciliation,
            sample.execute_phase_reconciliation,
            sample.planner_phase_reconciliation,
            sample.executor_invocation_phase_reconciliation,
        ];
        if observed != expected_phase_reconciliations(sample) {
            return Err(MatrixReportValidationError::PhaseReconciliationDrift(
                sample.key.clone(),
            ));
        }
    }
    validate_p1_shard_receipts(profile, &report.p1_shard_receipts)
        .map_err(MatrixReportValidationError::InvalidReceipts)?;
    let declared_scenarios = deterministic_matrix();
    let declared_ids = declared_scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let successful_ids = report
        .samples
        .iter()
        .map(|sample| sample.key.as_str())
        .collect::<Vec<_>>();
    let failed_ids = report
        .failures
        .iter()
        .map(|failure| failure.key.as_str())
        .collect::<Vec<_>>();
    let expected_receipts =
        build_p1_shard_receipts(profile, &declared_ids, &successful_ids, &failed_ids)
            .map_err(MatrixReportValidationError::InvalidReceipts)?;
    if report.p1_shard_receipts != expected_receipts {
        return Err(MatrixReportValidationError::ReceiptOutcomeDrift);
    }

    Ok(())
}

fn validate_matrix_measurement_coverage(
    coverage: PerformanceMeasurementCoverage,
) -> Result<(), MatrixReportValidationError> {
    if coverage != current_measurement_coverage() {
        return Err(MatrixReportValidationError::MeasurementCoverageDrift);
    }

    Ok(())
}

fn write_matrix_reports(report: &MatrixReport) -> Result<(), MatrixReportArtifactError> {
    validate_matrix_report_for_publication(report)
        .map_err(MatrixReportArtifactError::InvalidReport)?;
    let stem = report_stem();
    if let Some(parent) = stem.parent() {
        fs::create_dir_all(parent).map_err(|source| MatrixReportArtifactError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }

    let json_path = stem.with_extension("json");
    let md_path = stem.with_extension("md");
    let json =
        serde_json::to_vec_pretty(report).map_err(|source| MatrixReportArtifactError::Encode {
            path: json_path.clone(),
            source,
        })?;
    validate_matrix_report_size(&json_path, json.len())?;
    fs::write(&json_path, json).map_err(|source| MatrixReportArtifactError::Io {
        path: json_path.clone(),
        operation: "written",
        source,
    })?;
    fs::write(&md_path, matrix_markdown(report)).map_err(|source| {
        MatrixReportArtifactError::Io {
            path: md_path.clone(),
            operation: "written",
            source,
        }
    })?;

    println!("matrix JSON: {}", json_path.display());
    println!("matrix Markdown: {}", md_path.display());

    Ok(())
}

fn validate_matrix_report_size(
    path: &Path,
    observed_bytes: usize,
) -> Result<(), MatrixReportArtifactError> {
    let max_bytes = SQL_PERFORMANCE_PROFILE.max_artifact_bytes();
    if observed_bytes > max_bytes {
        return Err(MatrixReportArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes,
            max_bytes,
        });
    }

    Ok(())
}

fn matrix_markdown(report: &MatrixReport) -> String {
    let mut output = String::new();
    writeln!(output, "# SQL Perf Deterministic Matrix").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "- performance profile version: {}",
        report.performance_profile_version,
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- expected scenario-set hash: `{}`",
        report.expected_scenario_set_hash,
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- observed scenario-set hash: `{}`",
        report.observed_scenario_set_hash,
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- broad scan complete: {}",
        report.broad_scan_complete,
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- canister wasm profile: {}",
        report.canister_wasm_profile,
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- declared scenarios: {}",
        report.declared_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- successful scenarios: {}",
        report.successful_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- failed scenarios: {}",
        report.failed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- P1 shard receipts: {}/{}",
        report.p1_shard_receipts.len(),
        SQL_PERFORMANCE_PROFILE.shard_count(),
    )
    .expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");

    append_measurement_coverage_table(&mut output, report.measurement_coverage);
    append_phase_ownership_table(&mut output, &report.phase_ownership);
    append_p1_shard_receipt_table(&mut output, &report.p1_shard_receipts);
    append_instruction_hotspot_tables(&mut output, &report.samples);
    append_storage_backend_comparison_table(&mut output, &report.samples);
    append_route_classification_summary(&mut output, report);
    append_failure_table(&mut output, &report.failures);

    output
}

fn append_measurement_coverage_table(
    output: &mut String,
    coverage: PerformanceMeasurementCoverage,
) {
    writeln!(output, "## Measurement Coverage").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(output, "| Dimension | Status |").expect("write to string should succeed");
    writeln!(output, "|---|---|").expect("write to string should succeed");
    for (dimension, status) in coverage.entries() {
        writeln!(output, "| `{dimension}` | `{}` |", status.code())
            .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_phase_ownership_table(output: &mut String, ownership: &PhaseOwnershipTable) {
    writeln!(output, "## Phase Ownership (version {})", ownership.version)
        .expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(output, "| Parent | Relationship | Children |")
        .expect("write to string should succeed");
    writeln!(output, "|---|---|---|").expect("write to string should succeed");
    for entry in &ownership.entries {
        writeln!(
            output,
            "| `{}` | `{}` | {} |",
            entry.parent,
            entry.relationship.code(),
            entry
                .children
                .iter()
                .map(|child| format!("`{child}`"))
                .collect::<Vec<_>>()
                .join(", "),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_p1_shard_receipt_table(output: &mut String, receipts: &[P1ShardReceipt]) {
    if receipts.is_empty() {
        return;
    }

    writeln!(output, "## P1 Broad-Scan Shard Receipts").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Shard | Expected | Observed | Success | Failure | Complete | Expected Hash | Observed Hash |",
    )
    .expect("write to string should succeed");
    writeln!(output, "|---:|---:|---:|---:|---:|---|---|---|")
        .expect("write to string should succeed");
    for receipt in receipts {
        writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | `{}` | `{}` |",
            receipt.shard_index,
            receipt.expected_scenario_count,
            receipt.observed_scenario_count,
            receipt.successful_scenario_count,
            receipt.failed_scenario_count,
            receipt.complete,
            receipt.expected_shard_hash,
            receipt.observed_shard_hash,
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
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

fn route_for_sample(sample: &MatrixSample) -> (String, String, Option<String>) {
    (
        sample.route_family.clone(),
        sample.route_outcome.clone(),
        sample.route_reason.clone(),
    )
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
            failure.route_family.clone(),
            failure.route_outcome.clone(),
            failure.route_reason.clone(),
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
        "Top Executor Invocation Instructions",
        ranked_by(samples, |sample| {
            sample.executor_invocation_local_instructions
        }),
    );
    append_ranked_table(
        output,
        "Top Executor Runtime Instructions",
        ranked_by(samples, |sample| sample.executor_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Response Finalization Instructions",
        ranked_by(samples, |sample| {
            sample.response_finalization_local_instructions
        }),
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
    append_phase_reconciliation_tables(output, samples);
}

fn append_phase_reconciliation_tables(output: &mut String, samples: &[MatrixSample]) {
    append_phase_reconciliation_table(
        output,
        "Top Total Phase Residual",
        samples,
        |sample| sample.total_local_instructions,
        |sample| sample.total_phase_reconciliation,
    );
    append_phase_reconciliation_table(
        output,
        "Top Compile Phase Residual",
        samples,
        |sample| sample.compile_local_instructions,
        |sample| sample.compile_phase_reconciliation,
    );
    append_phase_reconciliation_table(
        output,
        "Top Execute Phase Residual",
        samples,
        |sample| sample.execute_local_instructions,
        |sample| sample.execute_phase_reconciliation,
    );
    append_phase_reconciliation_table(
        output,
        "Top Planner Phase Residual",
        samples,
        |sample| sample.planner_local_instructions,
        |sample| sample.planner_phase_reconciliation,
    );
    append_phase_reconciliation_table(
        output,
        "Top Executor Invocation Phase Residual",
        samples,
        |sample| sample.executor_invocation_local_instructions,
        |sample| sample.executor_invocation_phase_reconciliation,
    );
}

fn append_phase_reconciliation_table<Parent, Reconciliation>(
    output: &mut String,
    title: &str,
    samples: &[MatrixSample],
    parent: Parent,
    reconciliation: Reconciliation,
) where
    Parent: Fn(&MatrixSample) -> u64,
    Reconciliation: Fn(&MatrixSample) -> PhaseReconciliation,
{
    let mut ranked = samples.iter().collect::<Vec<_>>();
    ranked.sort_by_key(|sample| {
        let value = reconciliation(sample);
        Reverse(
            value
                .unaccounted_local_instructions
                .max(value.over_attributed_local_instructions),
        )
    });
    ranked.truncate(top_n());

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Parent | Attributable | Unaccounted | Over-attributed | Unaccounted bp |",
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---:|---:|").expect("write to string should succeed");
    for sample in ranked {
        let value = reconciliation(sample);
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} |",
            sample.key,
            sample.surface,
            parent(sample),
            value.attributable_local_instructions,
            value.unaccounted_local_instructions,
            value.over_attributed_local_instructions,
            value.unaccounted_basis_points.map_or_else(
                || "n/a".to_string(),
                |basis_points| basis_points.to_string()
            ),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
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
        "| Scenario | Surface | Total | Compile | Execute | Planner | Store | Executor Invocation | Executor Runtime | Response Finalization | data_store.get | index_store.get | index_store.ranges | index_store.entries | Rows | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.total_local_instructions,
            sample.compile_local_instructions,
            sample.execute_local_instructions,
            sample.planner_local_instructions,
            sample.store_local_instructions,
            sample.executor_invocation_local_instructions,
            sample.executor_local_instructions,
            sample.response_finalization_local_instructions,
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
        "| Scenario | Surface | Owner | Mismatch | Code | Diagnostic | Class | Origin | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---|---|---:|---|---|---|---|")
        .expect("write to string should succeed");
    for failure in failures.iter().take(top_n()) {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} ({}) | {} | {} | `{}` |",
            failure.key,
            failure.surface,
            failure.correctness_failure_owner,
            failure.correctness_mismatch_category,
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
        read_metadata(
            &["select.scalar_rows"],
            QueryShape::Scalar,
            ValueTypeFamily::Numeric,
            PredicateFamily::None,
            WindowSpec::ordered_unbounded("unsupported_expression"),
            RouteExpectation::Fixed(RouteFact::new(
                RouteFamily::UnsupportedAccessKind,
                RouteOutcome::Unsupported,
                RouteReason::OrderExpressionNotClassified,
            )),
            false,
        ),
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
    assert_eq!(failure.correctness_failure_owner, "product_failure");
    assert_eq!(failure.correctness_mismatch_category, "acceptance");
}

fn print_matrix_summary(report: &MatrixReport) {
    println!("{}", matrix_markdown(report));
}

#[test]
fn sql_perf_deterministic_matrix_has_stable_shape() {
    let deterministic = deterministic_matrix();
    SQL_PERFORMANCE_PROFILE
        .validate_scenario_set(deterministic.iter().map(|scenario| scenario.key.as_str()))
        .unwrap_or_else(|error| panic!("checked-in performance profile drifted: {error}"));
    assert!(
        deterministic.len() >= 1_000,
        "deterministic matrix should be broad enough to hunt hotspots; got {}",
        deterministic.len(),
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
fn sql_perf_p1_receipts_cover_the_profile_exactly() {
    let deterministic = deterministic_matrix();
    let declared_ids = deterministic
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let receipts =
        build_p1_shard_receipts(SQL_PERFORMANCE_PROFILE, &declared_ids, &declared_ids, &[])
            .expect("complete deterministic outcomes should produce all P1 receipts");
    let shard_counts = receipts
        .iter()
        .map(|receipt| receipt.observed_scenario_count)
        .collect::<Vec<_>>();

    assert_eq!(receipts.len(), 8);
    assert_eq!(shard_counts, vec![230, 221, 231, 205, 221, 223, 227, 200]);
    assert_eq!(shard_counts.iter().sum::<usize>(), 1_758);
    assert!(receipts.iter().all(|receipt| receipt.complete));
    validate_p1_shard_receipts(SQL_PERFORMANCE_PROFILE, &receipts)
        .expect("all deterministic receipts should merge");
}

#[test]
fn sql_perf_p1_receipts_reject_missing_duplicate_and_misassigned_evidence() {
    let deterministic = deterministic_matrix();
    let declared_ids = deterministic
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let mut receipts =
        build_p1_shard_receipts(SQL_PERFORMANCE_PROFILE, &declared_ids, &declared_ids, &[])
            .expect("complete deterministic outcomes should produce all P1 receipts");

    assert!(matches!(
        validate_p1_shard_receipts(SQL_PERFORMANCE_PROFILE, &receipts[..7]),
        Err(P1ReceiptError::ReceiptCountMismatch {
            expected: 8,
            actual: 7,
        })
    ));

    receipts[1].shard_index = receipts[0].shard_index;
    assert!(matches!(
        validate_p1_shard_receipts(SQL_PERFORMANCE_PROFILE, &receipts),
        Err(P1ReceiptError::DuplicateReceipt(_))
    ));

    let scenario_id = declared_ids[0];
    let assigned = SQL_PERFORMANCE_PROFILE
        .scenario_shard(scenario_id)
        .expect("declared scenario should shard");
    let wrong_shard = (assigned + 1) % SQL_PERFORMANCE_PROFILE.shard_count();
    assert!(matches!(
        p1_shard_receipt(
            SQL_PERFORMANCE_PROFILE,
            wrong_shard,
            &declared_ids,
            &[scenario_id],
            &[],
        ),
        Err(P1ReceiptError::ScenarioAssignedToDifferentShard { .. })
    ));
}

fn synthetic_p1_shard_reports() -> Vec<P1ShardReport> {
    let scenarios = deterministic_matrix();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();

    (0..SQL_PERFORMANCE_PROFILE.shard_count())
        .map(|shard_index| {
            let samples = scenarios
                .iter()
                .filter(|scenario| {
                    SQL_PERFORMANCE_PROFILE
                        .scenario_shard(&scenario.key)
                        .expect("declared scenario should shard")
                        == shard_index
                })
                .map(|scenario| MatrixSample {
                    key: scenario.key.clone(),
                    ..MatrixSample::default()
                })
                .collect::<Vec<_>>();
            build_p1_shard_report(
                SQL_PERFORMANCE_PROFILE,
                matrix_canister_wasm_profile().as_str(),
                crate::sql_perf_environment::tests::identity(),
                shard_index,
                &declared_ids,
                samples,
                Vec::new(),
            )
            .expect("complete deterministic shard should build")
        })
        .collect()
}

#[test]
fn sql_perf_p1_shard_artifacts_are_strict_bounded_and_merge_exactly() {
    let scenarios = deterministic_matrix();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let reports = synthetic_p1_shard_reports();
    let mut unknown_field =
        serde_json::to_value(&reports[0]).expect("current P1 shard should serialize");
    unknown_field
        .as_object_mut()
        .expect("P1 shard should be a JSON object")
        .insert("unexpected".to_string(), serde_json::Value::Bool(true));
    assert!(
        serde_json::from_value::<P1ShardReport>(unknown_field).is_err(),
        "unknown shard artifact fields must fail current-format decoding",
    );

    let max_bytes = SQL_PERFORMANCE_PROFILE.max_artifact_bytes();
    assert!(
        validate_p1_shard_artifact_size(Path::new("p1-shard.json"), max_bytes, max_bytes).is_ok()
    );
    assert!(matches!(
        validate_p1_shard_artifact_size(
            Path::new("p1-shard.json"),
            max_bytes + 1,
            max_bytes,
        ),
        Err(P1ShardArtifactError::TooLarge {
            observed_bytes,
            max_bytes: observed_max_bytes,
            ..
        }) if observed_bytes == max_bytes + 1 && observed_max_bytes == max_bytes
    ));

    let merged = merge_p1_shard_reports(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &declared_ids,
        reports,
    )
    .expect("all exact shard reports should merge");
    assert_eq!(merged.receipts.len(), 8);
    assert_eq!(merged.samples.len(), 1_758);
    assert!(merged.failures.is_empty());
    assert!(
        merged
            .samples
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
    );
}

#[test]
fn sql_perf_p1_shard_merge_rejects_missing_duplicate_and_tampered_evidence() {
    let scenarios = deterministic_matrix();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let required_wasm_profile = matrix_canister_wasm_profile().as_str();

    let mut missing = synthetic_p1_shard_reports();
    missing.pop();
    assert!(matches!(
        merge_p1_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            required_wasm_profile,
            &declared_ids,
            missing,
        ),
        Err(P1ShardMergeError::ReportCountMismatch {
            expected: 8,
            actual: 7,
        })
    ));

    let mut duplicate = synthetic_p1_shard_reports();
    duplicate[7] = duplicate[0].clone();
    assert!(matches!(
        merge_p1_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            required_wasm_profile,
            &declared_ids,
            duplicate,
        ),
        Err(P1ShardMergeError::DuplicateReport(0))
    ));

    let mut tampered = synthetic_p1_shard_reports();
    let mut tampered_value =
        serde_json::to_value(&tampered[0]).expect("current P1 shard should serialize");
    tampered_value["samples"][0]["key"] = serde_json::Value::String("tampered".to_string());
    tampered[0] = serde_json::from_value(tampered_value)
        .expect("structurally current P1 shard should decode before semantic validation");
    assert!(matches!(
        merge_p1_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            required_wasm_profile,
            &declared_ids,
            tampered,
        ),
        Err(P1ShardMergeError::InvalidReport { shard_index: 0, .. })
    ));
}

#[test]
fn sql_perf_deterministic_matrix_includes_branch_route_hotspots() {
    let deterministic = deterministic_matrix();
    let scenarios_by_key = deterministic
        .iter()
        .map(|scenario| (scenario.key.as_str(), scenario))
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
        let scenario = scenarios_by_key
            .get(key)
            .copied()
            .unwrap_or_else(|| panic!("deterministic matrix should include route hotspot {key}"));
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

fn assert_branch_route_hotspot_sql_shapes(scenarios_by_key: &BTreeMap<&str, &MatrixScenario>) {
    let branch = scenarios_by_key
        .get("token.collection_stage_id.branch_set.page_only.limit50")
        .expect("branch-set route hotspot should exist")
        .to_owned();
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
        .to_owned();
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
        .to_owned();
    assert!(
        wide_branch.sql.contains(
            "stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden')"
        ),
        "wide branch-set hotspot should cover the admitted nine-branch route"
    );

    let over_cap = scenarios_by_key
        .get("token.collection_stage_id.overcap_fallback.page_only.limit50")
        .expect("over-cap route hotspot should exist")
        .to_owned();
    assert!(
        over_cap.sql.contains(
            "stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07')"
        ),
        "over-cap route hotspot should exceed the branch-set admission cap"
    );

    let over_cap_pruned = scenarios_by_key
        .get("token.collection_stage_id.overcap_pruned.page_only.limit50")
        .expect("post-exclusion over-cap route hotspot should exist")
        .to_owned();
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

fn assert_sparse_collection_in_route_hotspots(scenarios_by_key: &BTreeMap<&str, &MatrixScenario>) {
    let sparse_in = scenarios_by_key
        .get("token.collection_id.sparse_in.page_only.limit50")
        .expect("sparse collection IN route hotspot should exist")
        .to_owned();
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

    let sparse_count = scenarios_by_key
        .get("token.collection_id.sparse_in.count")
        .expect("sparse collection IN count hotspot should exist")
        .to_owned();
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
fn sql_perf_matrix_route_and_window_facts_are_constructor_owned() {
    let scenarios = deterministic_matrix()
        .into_iter()
        .map(|scenario| (scenario.key.clone(), scenario))
        .collect::<BTreeMap<_, _>>();

    assert_primary_route_facts(&scenarios);
    assert_secondary_route_facts(&scenarios);
    assert_token_route_facts(&scenarios);
}

fn assert_primary_route_facts(scenarios: &BTreeMap<String, MatrixScenario>) {
    let primary = scenarios
        .get("user.select.pk.all.pk_asc.limit1")
        .expect("primary-order matrix case should exist");
    let mut sample = MatrixSample {
        outcome: MatrixOutcome {
            row_count: 1,
            ..MatrixOutcome::default()
        },
        ..MatrixSample::default()
    };
    sample.data_store_get_calls = 1;
    let route = route_fact_for_scenario(primary, &sample);
    assert_eq!(
        route,
        RouteFact::new(
            RouteFamily::PrimaryOrder,
            RouteOutcome::Pushed,
            RouteReason::PrimaryOrderLimitStopProven,
        )
    );
    assert_eq!(primary.metadata.window.order_hint, Some("id ASC"));

    assert_eq!(
        limit_stop_after_for_scenario(primary, &sample, route),
        MatrixLimitStopAfter {
            possible: true,
            returned_limit: Some(1),
            lookahead: 1,
            stopped_after_matches: Some(1),
            stopped_after_index_entries: Some(0),
            disabled_reason: None,
        },
    );
    let residual = scenarios
        .get("user.select.pk.active_true.pk_desc.limit3")
        .expect("residual primary-order matrix case should exist");
    assert_eq!(
        route_fact_for_scenario(residual, &MatrixSample::default()),
        RouteFact::new(
            RouteFamily::ResidualFilterOrderedScan,
            RouteOutcome::ResidualUnbounded,
            RouteReason::ResidualFilterRequiresCandidateScan,
        )
    );

    let mut changed_sql = primary.clone();
    changed_sql.sql = "rendered SQL is not route authority".to_string();
    assert_eq!(route_fact_for_scenario(&changed_sql, &sample), route);
}

fn assert_secondary_route_facts(scenarios: &BTreeMap<String, MatrixScenario>) {
    let suffix_gap = scenarios
        .get("blob.select.metadata.bucket_range.bucket_asc.limit3")
        .expect("blob suffix-gap matrix case should exist");
    let suffix_route = route_fact_for_scenario(suffix_gap, &MatrixSample::default());
    assert_eq!(
        suffix_route,
        RouteFact::new(
            RouteFamily::SecondaryOrder,
            RouteOutcome::MissingTieBreaker,
            RouteReason::IndexOrderSuffixGap,
        )
    );
    assert_eq!(
        limit_stop_after_for_scenario(suffix_gap, &MatrixSample::default(), suffix_route)
            .disabled_reason
            .as_deref(),
        Some("index_order_suffix_gap")
    );
}

fn assert_token_route_facts(scenarios: &BTreeMap<String, MatrixScenario>) {
    let secondary_range = scenarios
        .get("token.collection_stage_id.prefixed_stage_range.page_only.limit50")
        .expect("secondary index-range matrix case should exist");
    let secondary_route = route_fact_for_scenario(
        secondary_range,
        &MatrixSample {
            index_store_entry_reads: 50,
            ..MatrixSample::default()
        },
    );
    assert_eq!(
        secondary_route,
        RouteFact::new(
            RouteFamily::SecondaryOrder,
            RouteOutcome::Pushed,
            RouteReason::SecondaryOrderLimitStopProven,
        )
    );
    assert_eq!(
        secondary_range.metadata.window.order_hint,
        Some("stage ASC, id ASC")
    );

    let equality_prefix = scenarios
        .get("token.collection_id.sparse_in.page_only.limit50")
        .expect("sparse child-expansion matrix case should exist");
    let equality_route = route_fact_for_scenario(
        equality_prefix,
        &MatrixSample {
            index_store_entry_reads: 50,
            ..MatrixSample::default()
        },
    );
    assert_eq!(
        equality_route,
        RouteFact::new(
            RouteFamily::EqualityPrefixOrderedSuffix,
            RouteOutcome::Pushed,
            RouteReason::EqualityPrefixOrderedSuffixLimitStopProven,
        )
    );
    assert_eq!(equality_prefix.metadata.window.order_hint, Some("id ASC"));

    let materialized = scenarios
        .get("token.collection_stage_id.overcap_fallback.page_only.limit50")
        .expect("over-cap materialized matrix case should exist");
    assert_eq!(
        route_fact_for_scenario(materialized, &MatrixSample::default()),
        RouteFact::new(
            RouteFamily::MaterializedOrder,
            RouteOutcome::Materialized,
            RouteReason::RequiresMaterializedSort,
        )
    );
}

fn test_matrix_report(samples: Vec<MatrixSample>, failures: Vec<MatrixFailure>) -> MatrixReport {
    let declared_scenario_count = samples.len() + failures.len();

    MatrixReport {
        performance_profile_version: 1,
        expected_scenario_set_hash: "test-scenario-set".to_string(),
        observed_scenario_set_hash: "test-scenario-set".to_string(),
        broad_scan_complete: false,
        canister_wasm_profile: "wasm-release".to_string(),
        environment: crate::sql_perf_environment::tests::identity(),
        measurement_coverage: current_measurement_coverage(),
        declared_scenario_count,
        successful_scenario_count: samples.len(),
        failed_scenario_count: failures.len(),
        phase_ownership: current_phase_ownership(),
        p1_shard_receipts: Vec::new(),
        samples,
        failures,
    }
}

#[test]
fn sql_perf_matrix_report_decoding_is_strict_and_size_bounded() {
    let report = test_matrix_report(Vec::new(), Vec::new());
    let encoded = serde_json::to_vec(&report).expect("current matrix report should serialize");
    let decoded = serde_json::from_slice::<MatrixReport>(&encoded)
        .expect("current matrix report should decode");
    assert_eq!(decoded, report);

    let mut unknown_field = serde_json::to_value(&report)
        .expect("current matrix report should serialize as a JSON value");
    unknown_field
        .as_object_mut()
        .expect("matrix report should be a JSON object")
        .insert("unexpected".to_string(), serde_json::Value::Bool(true));
    assert!(
        serde_json::from_value::<MatrixReport>(unknown_field).is_err(),
        "unknown artifact fields must fail current-format decoding",
    );

    let max_bytes = SQL_PERFORMANCE_PROFILE.max_artifact_bytes();
    assert!(validate_matrix_report_size(Path::new("matrix.json"), max_bytes).is_ok());
    assert!(matches!(
        validate_matrix_report_size(Path::new("matrix.json"), max_bytes + 1),
        Err(MatrixReportArtifactError::TooLarge {
            observed_bytes,
            max_bytes: observed_max_bytes,
            ..
        }) if observed_bytes == max_bytes + 1 && observed_max_bytes == max_bytes
    ));
}

#[test]
fn sql_perf_matrix_markdown_reports_measured_and_unmeasured_resources() {
    let markdown = matrix_markdown(&test_matrix_report(Vec::new(), Vec::new()));

    assert!(markdown.contains("| `instruction_attribution` | `measured` |"));
    assert!(markdown.contains("| `projected_blob_output_bytes` | `measured` |"));
    assert!(markdown.contains("| `peak_heap_bytes` | `not_measured` |"));
    assert!(markdown.contains("| `allocator_traffic_bytes` | `not_measured` |"));
    assert!(markdown.contains("| `stable_memory_byte_volume` | `not_measured` |"));
}

#[test]
fn sql_perf_matrix_publication_requires_complete_current_profile_evidence() {
    let scenarios = deterministic_matrix();
    let samples = scenarios
        .iter()
        .map(|scenario| MatrixSample {
            key: scenario.key.clone(),
            ..MatrixSample::default()
        })
        .collect::<Vec<_>>();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let successful_ids = samples
        .iter()
        .map(|sample| sample.key.as_str())
        .collect::<Vec<_>>();
    let receipts =
        build_p1_shard_receipts(SQL_PERFORMANCE_PROFILE, &declared_ids, &successful_ids, &[])
            .expect("complete profile should produce publication receipts");
    let mut report = MatrixReport {
        performance_profile_version: SQL_PERFORMANCE_PROFILE.version(),
        expected_scenario_set_hash: SQL_PERFORMANCE_PROFILE
            .expected_scenario_set_hash()
            .to_string(),
        observed_scenario_set_hash: SQL_PERFORMANCE_PROFILE
            .expected_scenario_set_hash()
            .to_string(),
        broad_scan_complete: true,
        canister_wasm_profile: CanisterWasmProfile::WasmRelease.as_str().to_string(),
        environment: crate::sql_perf_environment::tests::identity(),
        measurement_coverage: current_measurement_coverage(),
        declared_scenario_count: samples.len(),
        successful_scenario_count: samples.len(),
        failed_scenario_count: 0,
        phase_ownership: current_phase_ownership(),
        p1_shard_receipts: receipts,
        samples,
        failures: Vec::new(),
    };

    validate_matrix_report_for_publication(&report)
        .expect("complete current-profile report should be publishable");
    report.measurement_coverage.peak_heap_bytes = PerformanceMeasurementStatus::Measured;
    assert!(matches!(
        validate_matrix_report_for_publication(&report),
        Err(MatrixReportValidationError::MeasurementCoverageDrift)
    ));
    report.measurement_coverage = current_measurement_coverage();
    report.samples[0]
        .total_phase_reconciliation
        .unaccounted_local_instructions = 1;
    assert!(matches!(
        validate_matrix_report_for_publication(&report),
        Err(MatrixReportValidationError::PhaseReconciliationDrift(_))
    ));
    fill_matrix_phase_reconciliation(&mut report.samples[0]);
    report.broad_scan_complete = false;
    assert!(matches!(
        validate_matrix_report_for_publication(&report),
        Err(MatrixReportValidationError::IncompleteBroadScan)
    ));
    report.broad_scan_complete = true;
    report.p1_shard_receipts[0].expected_shard_hash = "forged".to_string();
    report.p1_shard_receipts[0].observed_shard_hash = "forged".to_string();
    assert!(matches!(
        validate_matrix_report_for_publication(&report),
        Err(MatrixReportValidationError::ReceiptOutcomeDrift)
    ));
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
        correctness_failure_owner: "product_failure".to_string(),
        correctness_mismatch_category: "acceptance".to_string(),
    };
    let report = test_matrix_report(vec![sample], vec![failure]);

    let markdown = matrix_markdown(&report);

    assert!(
        markdown.contains("- canister wasm profile: wasm-release"),
        "matrix markdown should include the fixture wasm profile",
    );
    assert!(
        markdown.contains("## Route Classification Summary"),
        "matrix markdown should expose route classification coverage",
    );
    assert!(
        markdown.contains("## Phase Ownership (version 1)")
            && markdown.contains("`nested_observation`"),
        "matrix markdown should carry the versioned additive/nested ownership table",
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
    let report = test_matrix_report(samples, Vec::new());

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
    let report = test_matrix_report(samples, Vec::new());

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

    let report = test_matrix_report(vec![sample], Vec::new());

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
fn sql_perf_matrix_preserves_execution_boundary_attribution() {
    let mut attribution = SqlQueryExecutionAttribution {
        execute_local_instructions: 160,
        total_local_instructions: 160,
        ..SqlQueryExecutionAttribution::default()
    };
    attribution.execution.store_local_instructions = 40;
    attribution.execution.executor_invocation_local_instructions = 120;
    attribution.execution.executor_local_instructions = 80;
    attribution
        .execution
        .response_finalization_local_instructions = 40;
    let mut sample = MatrixSample::default();

    fill_matrix_execution_sample(&mut sample, &attribution);
    fill_matrix_phase_reconciliation(&mut sample);

    assert_eq!(sample.executor_invocation_local_instructions, 120);
    assert_eq!(sample.executor_local_instructions, 80);
    assert_eq!(sample.response_finalization_local_instructions, 40);
    assert_eq!(
        sample.executor_invocation_phase_reconciliation,
        PhaseReconciliation {
            attributable_local_instructions: 120,
            unaccounted_local_instructions: 0,
            over_attributed_local_instructions: 0,
            unaccounted_basis_points: Some(0),
        },
    );
    assert_eq!(
        sample.execute_phase_reconciliation,
        PhaseReconciliation {
            attributable_local_instructions: 160,
            unaccounted_local_instructions: 0,
            over_attributed_local_instructions: 0,
            unaccounted_basis_points: Some(0),
        },
    );

    sample.key = "user.execution.boundary".to_string();
    sample.surface = "user".to_string();
    sample.sql = "SELECT id FROM PerfAuditUser".to_string();
    let mut markdown = String::new();
    append_instruction_hotspot_tables(&mut markdown, &[sample]);
    assert!(markdown.contains("## Top Executor Invocation Instructions"));
    assert!(markdown.contains("## Top Executor Runtime Instructions"));
    assert!(markdown.contains("## Top Response Finalization Instructions"));
    assert!(markdown.contains("## Top Execute Phase Residual"));
    assert!(markdown.contains("## Top Executor Invocation Phase Residual"));
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
    let report = test_matrix_report(samples, Vec::new());

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
    let report = test_matrix_report(samples, Vec::new());

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

fn test_fixture_row_count(surface: &str) -> u64 {
    match surface {
        "account" | "blob" | "user" => 6,
        "heap_user" | "journaled_user" => 512,
        "token" => 260,
        other => panic!("test sample has unknown matrix surface `{other}`"),
    }
}

fn report_matrix_sample(
    key: &str,
    surface: &str,
    total: u64,
    store: u64,
    sql: &str,
) -> MatrixSample {
    let execute = total.saturating_sub(1);
    let executor_runtime = execute.saturating_sub(store);
    let mut sample = MatrixSample {
        key: key.to_string(),
        surface: surface.to_string(),
        family: "select.pk.all.pk_asc".to_string(),
        sql: sql.to_string(),
        fixture_row_count: test_fixture_row_count(surface),
        route_family: "primary_order".to_string(),
        route_outcome: "eligible_but_not_pushed".to_string(),
        route_reason: Some("test_sample".to_string()),
        limit_stop_after: MatrixLimitStopAfter {
            possible: false,
            disabled_reason: Some("test_sample".to_string()),
            ..MatrixLimitStopAfter::default()
        },
        result_signature: Some("projection|PerfAuditHeapUser|id|1|1".to_string()),
        compile_local_instructions: 1,
        execute_local_instructions: execute,
        store_local_instructions: store,
        executor_invocation_local_instructions: execute,
        executor_local_instructions: executor_runtime,
        data_store_get_calls: 1,
        sql_compiled_command_misses: 1,
        shared_query_plan_misses: 1,
        total_local_instructions: total,
        outcome: MatrixOutcome {
            result_kind: "projection".to_string(),
            entity: "PerfAuditHeapUser".to_string(),
            row_count: 1,
        },
        ..MatrixSample::default()
    };
    fill_matrix_phase_reconciliation(&mut sample);

    sample
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
    let report = test_matrix_report(vec![sample], Vec::new());

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
#[ignore = "expensive PocketIC P1 shard; set ICYDB_SQL_PERF_P1_SHARD_INDEX and run manually"]
fn sql_perf_p1_shard_reports_hotspots() {
    let shard_index = performance_shard_index(SQL_PERF_P1_SHARD_INDEX_ENV, "P1")
        .unwrap_or_else(|error| panic!("P1 shard selection failed: {error}"));
    let scenarios = deterministic_matrix();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    SQL_PERFORMANCE_PROFILE
        .validate_scenario_set(declared_ids.iter().copied())
        .unwrap_or_else(|error| panic!("checked-in performance profile drifted: {error}"));
    let shard_scenarios = scenarios
        .iter()
        .filter(|scenario| {
            SQL_PERFORMANCE_PROFILE
                .scenario_shard(&scenario.key)
                .unwrap_or_else(|error| panic!("scenario sharding failed: {error}"))
                == shard_index
        })
        .collect::<Vec<_>>();

    eprintln!("sql_perf_matrix: building one wasm-release module for P1 shard {shard_index}");
    let wasm =
        build_fixture_canister_wasm_bytes_with_options("sql_perf", matrix_canister_build_options());
    let fixture = install_prebuilt_fixture_canister("sql_perf", wasm.clone());
    eprintln!("sql_perf_matrix: resetting and loading fixture rows");
    reset_icydb_fixtures(&fixture);
    let environment = capture_matrix_environment(&fixture, &wasm);

    eprintln!(
        "sql_perf_matrix: sampling {} scenarios assigned to P1 shard {shard_index}",
        shard_scenarios.len(),
    );
    let mut samples = Vec::new();
    let mut failures = Vec::new();
    for scenario in shard_scenarios {
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
    let report = build_p1_shard_report(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        environment,
        shard_index,
        &declared_ids,
        samples,
        failures,
    )
    .unwrap_or_else(|error| panic!("P1 shard evidence is incomplete: {error}"));
    let path = p1_shard_path(&p1_shard_directory(), shard_index);
    write_p1_shard_report(
        &path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &declared_ids,
        &report,
    )
    .unwrap_or_else(|error| panic!("P1 shard artifact failed: {error}"));

    println!("P1 shard JSON: {}", path.display());
}

#[test]
#[ignore = "expensive PocketIC scale shard; set ICYDB_SQL_PERF_SCALE_SHARD_INDEX and run manually"]
fn sql_perf_scale_shard_measures_declared_ladders() {
    let shard_index = performance_shard_index(SQL_PERF_SCALE_SHARD_INDEX_ENV, "scale")
        .unwrap_or_else(|error| panic!("scale shard selection failed: {error}"));
    let p1_scenarios = deterministic_matrix();
    let declarations = scale_scenario_declarations(SQL_PERFORMANCE_PROFILE, &p1_scenarios)
        .unwrap_or_else(|error| panic!("scale declarations are invalid: {error}"));
    let shard_declarations = declarations
        .iter()
        .filter(|declaration| {
            SQL_PERFORMANCE_PROFILE
                .scenario_shard(&declaration.scenario.key)
                .unwrap_or_else(|error| panic!("scale scenario sharding failed: {error}"))
                == shard_index
        })
        .collect::<Vec<_>>();

    eprintln!("sql_perf_matrix: building one wasm-release module for scale shard {shard_index}");
    let wasm =
        build_fixture_canister_wasm_bytes_with_options("sql_perf", matrix_canister_build_options());
    let environment = capture_isolated_matrix_environment(&wasm);
    eprintln!(
        "sql_perf_matrix: sampling {} scale scenarios assigned to shard {shard_index}",
        shard_declarations.len(),
    );
    let mut observations = Vec::with_capacity(shard_declarations.len());
    for declaration in shard_declarations {
        eprintln!("sql_perf_matrix: sampling {}", declaration.scenario.key);
        observations.push(
            sample_isolated_scale_scenario(&wasm, declaration).unwrap_or_else(|error| {
                panic!(
                    "scale scenario {} failed: {error}",
                    declaration.scenario.key
                )
            }),
        );
    }
    let report = build_scale_shard_report(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        environment,
        &p1_scenarios,
        shard_index,
        observations,
    )
    .unwrap_or_else(|error| panic!("scale shard evidence is incomplete: {error}"));
    let path = scale_shard_path(&scale_shard_directory(), shard_index);
    write_scale_shard_report(
        &path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &p1_scenarios,
        &report,
    )
    .unwrap_or_else(|error| panic!("scale shard artifact failed: {error}"));

    println!("scale shard JSON: {}", path.display());
}

#[test]
#[ignore = "merges all eight saved P1 and scale shard artifacts; run manually after shard capture"]
fn sql_perf_p1_merges_saved_shards() {
    let scenarios = deterministic_matrix();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let directory = p1_shard_directory();
    let reports = (0..SQL_PERFORMANCE_PROFILE.shard_count())
        .map(|shard_index| {
            let path = p1_shard_path(&directory, shard_index);
            read_p1_shard_report(
                &path,
                SQL_PERFORMANCE_PROFILE,
                matrix_canister_wasm_profile().as_str(),
                &declared_ids,
            )
            .unwrap_or_else(|error| panic!("P1 shard artifact failed: {error}"))
        })
        .collect::<Vec<_>>();
    let merged = merge_p1_shard_reports(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &declared_ids,
        reports,
    )
    .unwrap_or_else(|error| panic!("P1 shard merge failed: {error}"));
    let merged_scale = merge_saved_scale_reports(&scenarios);
    assert_eq!(
        merged.environment, merged_scale.environment,
        "P1 and scale artifacts must describe the same environment and measured subject",
    );
    let successful_scenario_count = merged.samples.len();
    let failed_scenario_count = merged.failures.len();
    let requirements = P2SelectionRequirements::from_profile(
        SQL_PERFORMANCE_PROFILE,
        Vec::new(),
        merged_scale.p2_representatives.clone(),
    );
    let selection = select_p2_candidates(
        SQL_PERFORMANCE_PROFILE,
        &merged.environment,
        &scenarios,
        &merged.samples,
        &requirements,
    )
    .unwrap_or_else(|error| panic!("P2 candidate selection failed: {error}"));
    let selection_path = p2_selection_path();
    write_p2_candidate_selection(
        &selection_path,
        SQL_PERFORMANCE_PROFILE,
        &declared_ids,
        &selection,
    )
    .unwrap_or_else(|error| panic!("P2 candidate artifact failed: {error}"));
    let report = MatrixReport {
        performance_profile_version: SQL_PERFORMANCE_PROFILE.version(),
        expected_scenario_set_hash: SQL_PERFORMANCE_PROFILE
            .expected_scenario_set_hash()
            .to_string(),
        observed_scenario_set_hash: SQL_PERFORMANCE_PROFILE
            .expected_scenario_set_hash()
            .to_string(),
        broad_scan_complete: true,
        canister_wasm_profile: matrix_canister_wasm_profile().as_str().to_string(),
        environment: merged.environment.clone(),
        measurement_coverage: current_measurement_coverage(),
        declared_scenario_count: declared_ids.len(),
        successful_scenario_count,
        failed_scenario_count,
        phase_ownership: current_phase_ownership(),
        p1_shard_receipts: merged.receipts,
        samples: merged.samples,
        failures: merged.failures,
    };

    write_matrix_reports(&report)
        .unwrap_or_else(|error| panic!("merged P1 matrix artifact failed: {error}"));
    print_matrix_summary(&report);
    println!(
        "scale observations: {}; normalized costs: {}; adjacent slopes: {}",
        merged_scale.observations.len(),
        merged_scale.normalized_costs.len(),
        merged_scale.slopes.len(),
    );
    println!("scale report JSON: {}", scale_report_path().display());
    println!("P2 candidate JSON: {}", selection_path.display());
}

#[test]
#[ignore = "expensive PocketIC P2 shard; set ICYDB_SQL_PERF_P2_SHARD_INDEX and run manually"]
fn sql_perf_p2_shard_confirms_selected_candidates() {
    let shard_index = performance_shard_index(SQL_PERF_P2_SHARD_INDEX_ENV, "P2")
        .unwrap_or_else(|error| panic!("P2 shard selection failed: {error}"));
    let scenarios = deterministic_matrix();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let selection =
        read_p2_candidate_selection(&p2_selection_path(), SQL_PERFORMANCE_PROFILE, &declared_ids)
            .unwrap_or_else(|error| panic!("P2 candidate artifact failed: {error}"));
    let declarations = scenarios
        .iter()
        .map(|scenario| (scenario.key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let candidates = selection
        .candidates
        .iter()
        .filter(|candidate| candidate.shard_index == shard_index)
        .cloned()
        .collect::<Vec<_>>();

    eprintln!("sql_perf_matrix: building one wasm-release module for P2 shard {shard_index}");
    let wasm =
        build_fixture_canister_wasm_bytes_with_options("sql_perf", matrix_canister_build_options());
    let environment = capture_isolated_matrix_environment(&wasm);
    assert_eq!(
        environment, selection.environment,
        "P2 must confirm the exact environment and measured subject selected by P1",
    );
    eprintln!(
        "sql_perf_matrix: confirming {} candidates assigned to P2 shard {shard_index}",
        candidates.len(),
    );
    let mut confirmations = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let scenario = declarations
            .get(candidate.scenario_id.as_str())
            .copied()
            .unwrap_or_else(|| panic!("P2 candidate {} has no declaration", candidate.scenario_id));
        eprintln!("sql_perf_matrix: confirming {} cold", candidate.scenario_id);
        let cold_samples = (0..SQL_PERFORMANCE_PROFILE.cold_samples_per_confirmation())
            .map(|_| sample_isolated_p2_scenario(&wasm, scenario, false))
            .collect::<Vec<_>>();
        let warm_input = if scenario.metadata.statement == StatementFamily::Select {
            eprintln!("sql_perf_matrix: confirming {} warm", candidate.scenario_id);
            P2WarmSampleInput::Required(
                (0..SQL_PERFORMANCE_PROFILE.warm_samples_per_confirmation())
                    .map(|_| sample_isolated_p2_scenario(&wasm, scenario, true))
                    .collect::<Vec<_>>(),
            )
        } else {
            eprintln!(
                "sql_perf_matrix: {} has no non-SELECT warm mode",
                candidate.scenario_id,
            );
            P2WarmSampleInput::NotApplicable(P2WarmNotApplicableReason::NonSelectStatement)
        };
        confirmations.push(
            build_p2_confirmation(SQL_PERFORMANCE_PROFILE, candidate, cold_samples, warm_input)
                .unwrap_or_else(|error| panic!("P2 confirmation failed: {error}")),
        );
    }

    let report = build_p2_shard_report(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        environment,
        &scenarios,
        &selection,
        shard_index,
        confirmations,
    )
    .unwrap_or_else(|error| panic!("P2 shard evidence is incomplete: {error}"));
    let path = p2_shard_path(&p2_shard_directory(), shard_index);
    write_p2_shard_report(
        &path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
        &selection,
        &report,
    )
    .unwrap_or_else(|error| panic!("P2 shard artifact failed: {error}"));

    println!("P2 shard JSON: {}", path.display());
}

#[test]
#[ignore = "merges all eight saved P2 shard artifacts; run manually after shard capture"]
fn sql_perf_p2_merges_saved_shards() {
    let scenarios = deterministic_matrix();
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let selection =
        read_p2_candidate_selection(&p2_selection_path(), SQL_PERFORMANCE_PROFILE, &declared_ids)
            .unwrap_or_else(|error| panic!("P2 candidate artifact failed: {error}"));
    let directory = p2_shard_directory();
    let reports = (0..SQL_PERFORMANCE_PROFILE.shard_count())
        .map(|shard_index| {
            read_p2_shard_report(
                &p2_shard_path(&directory, shard_index),
                SQL_PERFORMANCE_PROFILE,
                matrix_canister_wasm_profile().as_str(),
                &scenarios,
                &selection,
            )
            .unwrap_or_else(|error| panic!("P2 shard artifact failed: {error}"))
        })
        .collect::<Vec<_>>();
    let merged = merge_p2_shard_reports(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
        &selection,
        reports,
    )
    .unwrap_or_else(|error| panic!("P2 shard merge failed: {error}"));
    let report_path = p2_report_path();
    write_merged_p2_report(
        &report_path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
        &merged,
    )
    .unwrap_or_else(|error| panic!("merged P2 artifact failed: {error}"));

    println!(
        "P2 merge complete: {} confirmations across {} receipts",
        merged.confirmations.len(),
        merged.receipts.len(),
    );
    println!("P2 report JSON: {}", report_path.display());
}

#[test]
#[ignore = "expensive PocketIC instrumentation calibration; run manually for Tier D evidence"]
fn sql_perf_calibrates_attribution_overhead() {
    let scenarios = deterministic_matrix();
    let scenario = scenarios
        .iter()
        .find(|scenario| scenario.key == INSTRUMENTATION_SENTINEL_SCENARIO_ID)
        .unwrap_or_else(|| {
            panic!(
                "instrumentation sentinel {INSTRUMENTATION_SENTINEL_SCENARIO_ID:?} has no declaration"
            )
        });
    assert_eq!(
        scenario.surface,
        MatrixSurface::User,
        "instrumentation sentinel must use the endpoint with both attributed and total-only paths",
    );

    eprintln!("sql_perf_matrix: building one wasm-release module for instrumentation calibration");
    let wasm =
        build_fixture_canister_wasm_bytes_with_options("sql_perf", matrix_canister_build_options());
    let environment = capture_isolated_matrix_environment(&wasm);
    let sample_count = SQL_PERFORMANCE_PROFILE.cold_samples_per_confirmation();
    let attributed_samples = (0..sample_count)
        .map(|_| sample_isolated_attributed_instrumentation(&wasm, scenario))
        .collect::<Vec<_>>();
    let total_only_samples = (0..sample_count)
        .map(|_| sample_isolated_total_only_instrumentation(&wasm, scenario))
        .collect::<Vec<_>>();
    let report = build_instrumentation_calibration_report(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        environment,
        attributed_samples,
        total_only_samples,
    )
    .unwrap_or_else(|error| panic!("instrumentation calibration failed: {error}"));
    let path = instrumentation_report_path();
    write_instrumentation_calibration_report(
        &path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &report,
    )
    .unwrap_or_else(|error| panic!("instrumentation artifact failed: {error}"));
    let decoded = read_instrumentation_calibration_report(
        &path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
    )
    .unwrap_or_else(|error| panic!("written instrumentation artifact is invalid: {error}"));
    assert_eq!(decoded, report);

    println!(
        "instrumentation calibration: attributed_median={} total_only_median={} overhead={} ({} bp); raw_wasm_bytes={}",
        report.attributed_median_instructions,
        report.total_only_median_instructions,
        report.overhead_instructions,
        report.overhead_basis_points,
        report.environment.subject.raw_wasm_bytes,
    );
    println!("instrumentation report JSON: {}", path.display());
}

#[test]
#[ignore = "compares explicit saved P2 baseline/current artifacts; run manually after both captures"]
fn sql_perf_compares_saved_baseline() {
    let scenarios = deterministic_matrix();
    let baseline_path = required_perf_artifact_path(SQL_PERF_BASELINE_PATH_ENV);
    let current_path = required_perf_artifact_path(SQL_PERF_CURRENT_PATH_ENV);
    let baseline_scale_path = required_perf_artifact_path(SQL_PERF_SCALE_BASELINE_PATH_ENV);
    let current_scale_path = required_perf_artifact_path(SQL_PERF_SCALE_CURRENT_PATH_ENV);
    let baseline = read_merged_p2_report(
        &baseline_path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
    )
    .unwrap_or_else(|error| panic!("P2 baseline artifact failed: {error}"));
    let current = read_merged_p2_report(
        &current_path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
    )
    .unwrap_or_else(|error| panic!("current P2 artifact failed: {error}"));
    let baseline_scale = read_merged_scale_report(
        &baseline_scale_path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
    )
    .unwrap_or_else(|error| panic!("scale baseline artifact failed: {error}"));
    let current_scale = read_merged_scale_report(
        &current_scale_path,
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
    )
    .unwrap_or_else(|error| panic!("current scale artifact failed: {error}"));
    let comparison = compare_performance_baseline(
        SQL_PERFORMANCE_PROFILE,
        matrix_canister_wasm_profile().as_str(),
        &scenarios,
        &baseline,
        &current,
        &baseline_scale,
        &current_scale,
    )
    .unwrap_or_else(|error| panic!("P2 reports are not comparable: {error}"));
    let comparison_path = performance_comparison_path();
    write_performance_baseline_comparison(&comparison_path, SQL_PERFORMANCE_PROFILE, &comparison)
        .unwrap_or_else(|error| panic!("P2 comparison artifact failed: {error}"));

    assert_eq!(
        comparison.observation_only_metric_count, 0,
        "P2 closeout requires reviewed thresholds for every required metric",
    );
    assert!(
        matches!(comparison.verdict, P2BaselineVerdict::Passed),
        "P2 baseline regression gate failed: {:?}",
        comparison.verdict,
    );
    println!("P2 comparison JSON: {}", comparison_path.display());
}
