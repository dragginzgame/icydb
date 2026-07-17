//!
//! Dedicated SQL perf-audit canister used only for instruction-sampling and
//! access-shape coverage.
//!

#[cfg(feature = "sql")]
use candid::{CandidType, Deserialize};
#[cfg(feature = "sql")]
use ic_cdk::query;
use ic_cdk::update;
use icydb::types::{Blob, Timestamp, Ulid};
#[cfg(feature = "sql")]
use icydb::{
    ErrorCode, ErrorOrigin,
    db::{
        DirectDataRowAttribution, EntitySchemaDescription, GroupedCountAttribution,
        GroupedExecutionAttribution, QueryExecutionAttribution, SqlCompileAttribution,
        SqlExecutionAttribution, SqlPureCoveringAttribution, SqlQueryCacheAttribution,
        SqlQueryExecutionAttribution, response::QueryResponse, sql::SqlQueryResult,
    },
    prelude::*,
    traits::EntityFor,
};
#[cfg(feature = "sql")]
use icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditCanister;
use icydb_testing_audit_sql_perf_fixtures::sql_perf::{
    PerfAuditAccount, PerfAuditBlob, PerfAuditHeapUser, PerfAuditJournaledUser, PerfAuditToken,
    PerfAuditUser,
};

icydb::start!();

// SqlQueryPerfResult
//
// Dedicated audit envelope that preserves the SQL result payload while
// attaching one compile/execute instruction sample for the measured query call
// or one average sample across a same-call loop.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct FluentTotalOnlyPerfResult {
    row_count: u32,
    instructions: u64,
}

///
/// ScalePayloadProfile
///
/// Exact blob-payload distribution loaded by one SQL scale fixture.
/// Owned by the audit canister and returned to the host as fixture evidence.
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[cfg(feature = "sql")]
enum ScalePayloadProfile {
    /// The selected surface has no blob payload fields.
    #[serde(rename = "not_applicable")]
    NotApplicable,

    /// Thumbnail lengths cycle through 32/64/128/256 bytes and chunk lengths
    /// cycle through 256/512/1,024/2,048 bytes.
    #[serde(rename = "blob_cycle_v1")]
    BlobCycleV1,
}

///
/// ScaleFixtureFacts
///
/// Realized deterministic distribution facts for one loaded scale surface.
/// Owned by the audit canister and validated by the host before sampling.
///

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
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

// FluentQueryPerfOutcome
//
// Dedicated fluent audit summary keeps the canister response stable and small:
// only the response family and row count are needed for perf-baseline checks.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct FluentQueryPerfOutcome {
    result_kind: String,
    entity: String,
    row_count: u32,
}

// FluentQueryPerfResult
//
// Dedicated fluent perf envelope mirrors the SQL audit shape but carries one
// reduced fluent response summary instead of the full query payload.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct FluentQueryPerfResult {
    outcome: FluentQueryPerfOutcome,
    attribution: QueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct FocusedPkPerfRow {
    scenario_key: String,
    terminal: String,
    selected_access: String,
    admission_result: String,
    error_code: Option<String>,
    total_instructions: u64,
    planner_instructions: u64,
    execute_instructions: u64,
    store_instructions: u64,
    data_store_get: u64,
    index_ranges: u64,
    rows_decoded: u64,
    rows_returned: u64,
    result_signature: String,
    canonicalization_result: String,
    raw_key_count: u32,
    deduplicated_key_count: u32,
    explanation: String,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct StorageWritePerfResult {
    first_insert_local_instructions: u64,
    steady_insert_avg_local_instructions: u64,
    steady_update_avg_local_instructions: u64,
    steady_delete_avg_local_instructions: u64,
    write_then_read_back_local_instructions: u64,
    read_back_rows: u32,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct SqlWriteMaterializationPerfResult {
    local_instructions: [u64; 4],
    rows: [u32; 4],
}

#[cfg(feature = "sql")]
const STORAGE_WRITE_MATRIX_RUNS: u32 = 10;
#[cfg(feature = "sql")]
const SQL_WRITE_MATERIALIZATION_ROWS: i32 = 32;
const JOURNALED_REENTRY_PROBE_ROWS: i32 = 32;
const TOKEN_TARGET_COLLECTION: &str = "01KV5N439P0000000000000000";
const TOKEN_OTHER_COLLECTION: &str = "01KV5N439P1111111111111111";
#[cfg(feature = "sql")]
const SCALE_FIXTURE_PROFILE_VERSION: u32 = 1;
#[cfg(feature = "sql")]
const SCALE_FIXTURE_ROW_CARDINALITIES: &[u32] = &[16, 256, 2_048];

#[cfg(feature = "sql")]
const fn query_validate_error() -> icydb::Error {
    icydb::Error::from_error_code(ErrorCode::QUERY_VALIDATE, ErrorOrigin::Query)
}

#[cfg(feature = "sql")]
const fn invalid_perf_loop_runs_error() -> icydb::Error {
    query_validate_error()
}

#[cfg(feature = "sql")]
fn validate_scale_fixture_rows(row_count: u32) -> Result<i32, icydb::Error> {
    if !SCALE_FIXTURE_ROW_CARDINALITIES.contains(&row_count) {
        return Err(query_validate_error());
    }

    i32::try_from(row_count).map_err(|_| query_validate_error())
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupedCountTotals {
    borrowed_hash_computations: u64,
    bucket_candidate_checks: u64,
    existing_group_hits: u64,
    new_group_inserts: u64,
    row_materialization_local_instructions: u64,
    group_lookup_local_instructions: u64,
    existing_group_update_local_instructions: u64,
    new_group_insert_local_instructions: u64,
}

#[cfg(feature = "sql")]
impl GroupedCountTotals {
    const fn record_fluent(&mut self, attribution: &QueryExecutionAttribution) {
        let Some(grouped) = attribution.grouped else {
            return;
        };

        self.borrowed_hash_computations = self
            .borrowed_hash_computations
            .saturating_add(grouped.count.borrowed_hash_computations);
        self.bucket_candidate_checks = self
            .bucket_candidate_checks
            .saturating_add(grouped.count.bucket_candidate_checks);
        self.existing_group_hits = self
            .existing_group_hits
            .saturating_add(grouped.count.existing_group_hits);
        self.new_group_inserts = self
            .new_group_inserts
            .saturating_add(grouped.count.new_group_inserts);
        self.row_materialization_local_instructions = self
            .row_materialization_local_instructions
            .saturating_add(grouped.count.row_materialization_local_instructions);
        self.group_lookup_local_instructions = self
            .group_lookup_local_instructions
            .saturating_add(grouped.count.group_lookup_local_instructions);
        self.existing_group_update_local_instructions = self
            .existing_group_update_local_instructions
            .saturating_add(grouped.count.existing_group_update_local_instructions);
        self.new_group_insert_local_instructions = self
            .new_group_insert_local_instructions
            .saturating_add(grouped.count.new_group_insert_local_instructions);
    }

    const fn record_grouped_count(&mut self, count: GroupedCountAttribution) {
        self.borrowed_hash_computations = self
            .borrowed_hash_computations
            .saturating_add(count.borrowed_hash_computations);
        self.bucket_candidate_checks = self
            .bucket_candidate_checks
            .saturating_add(count.bucket_candidate_checks);
        self.existing_group_hits = self
            .existing_group_hits
            .saturating_add(count.existing_group_hits);
        self.new_group_inserts = self
            .new_group_inserts
            .saturating_add(count.new_group_inserts);
        self.row_materialization_local_instructions = self
            .row_materialization_local_instructions
            .saturating_add(count.row_materialization_local_instructions);
        self.group_lookup_local_instructions = self
            .group_lookup_local_instructions
            .saturating_add(count.group_lookup_local_instructions);
        self.existing_group_update_local_instructions = self
            .existing_group_update_local_instructions
            .saturating_add(count.existing_group_update_local_instructions);
        self.new_group_insert_local_instructions = self
            .new_group_insert_local_instructions
            .saturating_add(count.new_group_insert_local_instructions);
    }
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_arguments)]
#[expect(
    clippy::field_reassign_with_default,
    reason = "perf attribution DTOs intentionally use default-backed assignment so future diagnostics counters do not break audit initializers"
)]
fn average_attribution(
    total_compile_local_instructions: u64,
    total_compile_cache_key_local_instructions: u64,
    total_compile_cache_lookup_local_instructions: u64,
    total_compile_parse_local_instructions: u64,
    total_compile_parse_tokenize_local_instructions: u64,
    total_compile_parse_select_local_instructions: u64,
    total_compile_parse_expr_local_instructions: u64,
    total_compile_parse_predicate_local_instructions: u64,
    total_compile_aggregate_lane_check_local_instructions: u64,
    total_compile_prepare_local_instructions: u64,
    total_compile_lower_local_instructions: u64,
    total_compile_bind_local_instructions: u64,
    total_compile_cache_insert_local_instructions: u64,
    total_plan_lookup_local_instructions: u64,
    total_planner_local_instructions: u64,
    total_store_local_instructions: u64,
    total_executor_invocation_local_instructions: u64,
    total_executor_local_instructions: u64,
    total_response_finalization_local_instructions: u64,
    total_pure_covering_decode_local_instructions: u64,
    total_pure_covering_row_assembly_local_instructions: u64,
    total_grouped_stream_local_instructions: u64,
    total_grouped_fold_local_instructions: u64,
    total_grouped_finalize_local_instructions: u64,
    total_grouped_count_borrowed_hash_computations: u64,
    total_grouped_count_bucket_candidate_checks: u64,
    total_grouped_count_existing_group_hits: u64,
    total_grouped_count_new_group_inserts: u64,
    total_grouped_count_row_materialization_local_instructions: u64,
    total_grouped_count_group_lookup_local_instructions: u64,
    total_grouped_count_existing_group_update_local_instructions: u64,
    total_grouped_count_new_group_insert_local_instructions: u64,
    total_store_get_calls: u64,
    total_index_store_get_calls: u64,
    total_index_store_range_scan_calls: u64,
    total_index_store_entry_reads: u64,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_sql_compiled_command_cache_hits: u64,
    total_sql_compiled_command_cache_misses: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    saw_pure_covering: bool,
    saw_grouped: bool,
    runs: u32,
) -> SqlQueryExecutionAttribution {
    let divisor = u64::from(runs);

    let mut attribution = SqlQueryExecutionAttribution::default();
    attribution.compile_local_instructions = total_compile_local_instructions / divisor;
    attribution.compile = SqlCompileAttribution {
        cache_key_local_instructions: total_compile_cache_key_local_instructions / divisor,
        cache_lookup_local_instructions: total_compile_cache_lookup_local_instructions / divisor,
        parse_local_instructions: total_compile_parse_local_instructions / divisor,
        parse_tokenize_local_instructions: total_compile_parse_tokenize_local_instructions
            / divisor,
        parse_select_local_instructions: total_compile_parse_select_local_instructions / divisor,
        parse_expr_local_instructions: total_compile_parse_expr_local_instructions / divisor,
        parse_predicate_local_instructions: total_compile_parse_predicate_local_instructions
            / divisor,
        aggregate_lane_check_local_instructions:
            total_compile_aggregate_lane_check_local_instructions / divisor,
        prepare_local_instructions: total_compile_prepare_local_instructions / divisor,
        lower_local_instructions: total_compile_lower_local_instructions / divisor,
        bind_local_instructions: total_compile_bind_local_instructions / divisor,
        cache_insert_local_instructions: total_compile_cache_insert_local_instructions / divisor,
    };
    attribution.plan_lookup_local_instructions = total_plan_lookup_local_instructions / divisor;
    attribution.execution = SqlExecutionAttribution {
        planner_local_instructions: total_planner_local_instructions / divisor,
        planner_schema_info_local_instructions: 0,
        planner_prepare_local_instructions: 0,
        planner_cache_key_local_instructions: 0,
        planner_cache_lookup_local_instructions: 0,
        planner_plan_build_local_instructions: 0,
        planner_cache_insert_local_instructions: 0,
        store_local_instructions: total_store_local_instructions / divisor,
        executor_invocation_local_instructions: total_executor_invocation_local_instructions
            / divisor,
        executor_local_instructions: total_executor_local_instructions / divisor,
        response_finalization_local_instructions: total_response_finalization_local_instructions
            / divisor,
    };
    if saw_pure_covering {
        attribution.pure_covering = Some(SqlPureCoveringAttribution {
            decode_local_instructions: total_pure_covering_decode_local_instructions / divisor,
            row_assembly_local_instructions: total_pure_covering_row_assembly_local_instructions
                / divisor,
        });
    }
    if saw_grouped {
        attribution.grouped = Some(GroupedExecutionAttribution {
            stream_local_instructions: total_grouped_stream_local_instructions / divisor,
            fold_local_instructions: total_grouped_fold_local_instructions / divisor,
            finalize_local_instructions: total_grouped_finalize_local_instructions / divisor,
            count: GroupedCountAttribution {
                borrowed_hash_computations: total_grouped_count_borrowed_hash_computations
                    / divisor,
                bucket_candidate_checks: total_grouped_count_bucket_candidate_checks / divisor,
                existing_group_hits: total_grouped_count_existing_group_hits / divisor,
                new_group_inserts: total_grouped_count_new_group_inserts / divisor,
                row_materialization_local_instructions:
                    total_grouped_count_row_materialization_local_instructions / divisor,
                group_lookup_local_instructions: total_grouped_count_group_lookup_local_instructions
                    / divisor,
                existing_group_update_local_instructions:
                    total_grouped_count_existing_group_update_local_instructions / divisor,
                new_group_insert_local_instructions:
                    total_grouped_count_new_group_insert_local_instructions / divisor,
            },
        });
    }
    attribution.store_get_calls = total_store_get_calls / divisor;
    attribution.index_store_get_calls = total_index_store_get_calls / divisor;
    attribution.index_store_range_scan_calls = total_index_store_range_scan_calls / divisor;
    attribution.index_store_entry_reads = total_index_store_entry_reads / divisor;
    attribution.response_decode_local_instructions =
        total_response_decode_local_instructions / divisor;
    attribution.execute_local_instructions = total_execute_local_instructions / divisor;
    attribution.total_local_instructions = total_local_instructions / divisor;
    attribution.cache = SqlQueryCacheAttribution {
        sql_compiled_command_hits: total_sql_compiled_command_cache_hits,
        sql_compiled_command_misses: total_sql_compiled_command_cache_misses,
        shared_query_plan_hits: total_shared_query_plan_cache_hits,
        shared_query_plan_misses: total_shared_query_plan_cache_misses,
    };

    attribution
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_arguments)]
#[expect(
    clippy::field_reassign_with_default,
    reason = "perf attribution DTOs intentionally use default-backed assignment so future diagnostics counters do not break audit initializers"
)]
fn average_fluent_attribution(
    total_compile_local_instructions: u64,
    total_compile_schema_catalog_local_instructions: u64,
    total_compile_schema_info_local_instructions: u64,
    total_compile_prepare_local_instructions: u64,
    total_compile_cache_key_local_instructions: u64,
    total_compile_cache_lookup_local_instructions: u64,
    total_compile_plan_build_local_instructions: u64,
    total_compile_cache_insert_local_instructions: u64,
    total_plan_lookup_local_instructions: u64,
    total_executor_invocation_local_instructions: u64,
    total_response_finalization_local_instructions: u64,
    total_load_plan_local_instructions: u64,
    total_row_layout_local_instructions: u64,
    total_continuation_signature_local_instructions: u64,
    total_scalar_runtime_handoff_local_instructions: u64,
    total_route_plan_local_instructions: u64,
    total_runtime_prepare_local_instructions: u64,
    total_runtime_local_instructions: u64,
    total_finalize_local_instructions: u64,
    total_direct_data_row_scan_local_instructions: u64,
    total_direct_data_row_key_stream_local_instructions: u64,
    total_direct_data_row_row_read_local_instructions: u64,
    total_direct_data_row_key_encode_local_instructions: u64,
    total_direct_data_row_store_get_local_instructions: u64,
    total_direct_data_row_order_window_local_instructions: u64,
    total_direct_data_row_page_window_local_instructions: u64,
    total_grouped_stream_local_instructions: u64,
    total_grouped_fold_local_instructions: u64,
    total_grouped_finalize_local_instructions: u64,
    total_grouped_count_borrowed_hash_computations: u64,
    total_grouped_count_bucket_candidate_checks: u64,
    total_grouped_count_existing_group_hits: u64,
    total_grouped_count_new_group_inserts: u64,
    total_grouped_count_row_materialization_local_instructions: u64,
    total_grouped_count_group_lookup_local_instructions: u64,
    total_grouped_count_existing_group_update_local_instructions: u64,
    total_grouped_count_new_group_insert_local_instructions: u64,
    total_store_get_calls: u64,
    total_index_store_get_calls: u64,
    total_index_store_range_scan_calls: u64,
    total_index_store_entry_reads: u64,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    saw_direct_data_row: bool,
    saw_grouped: bool,
    runs: u32,
) -> QueryExecutionAttribution {
    let divisor = u64::from(runs);

    let mut attribution = QueryExecutionAttribution::default();
    attribution.compile_local_instructions = total_compile_local_instructions / divisor;
    attribution.compile_schema_catalog_local_instructions =
        total_compile_schema_catalog_local_instructions / divisor;
    attribution.compile_schema_info_local_instructions =
        total_compile_schema_info_local_instructions / divisor;
    attribution.compile_prepare_local_instructions =
        total_compile_prepare_local_instructions / divisor;
    attribution.compile_cache_key_local_instructions =
        total_compile_cache_key_local_instructions / divisor;
    attribution.compile_cache_lookup_local_instructions =
        total_compile_cache_lookup_local_instructions / divisor;
    attribution.compile_plan_build_local_instructions =
        total_compile_plan_build_local_instructions / divisor;
    attribution.compile_cache_insert_local_instructions =
        total_compile_cache_insert_local_instructions / divisor;
    attribution.plan_lookup_local_instructions = total_plan_lookup_local_instructions / divisor;
    attribution.executor_invocation_local_instructions =
        total_executor_invocation_local_instructions / divisor;
    attribution.response_finalization_local_instructions =
        total_response_finalization_local_instructions / divisor;
    attribution.load_plan_local_instructions = total_load_plan_local_instructions / divisor;
    attribution.row_layout_local_instructions = total_row_layout_local_instructions / divisor;
    attribution.continuation_signature_local_instructions =
        total_continuation_signature_local_instructions / divisor;
    attribution.scalar_runtime_handoff_local_instructions =
        total_scalar_runtime_handoff_local_instructions / divisor;
    attribution.route_plan_local_instructions = total_route_plan_local_instructions / divisor;
    attribution.runtime_prepare_local_instructions =
        total_runtime_prepare_local_instructions / divisor;
    attribution.runtime_local_instructions = total_runtime_local_instructions / divisor;
    attribution.finalize_local_instructions = total_finalize_local_instructions / divisor;
    if saw_direct_data_row {
        attribution.direct_data_row = Some(DirectDataRowAttribution {
            scan_local_instructions: total_direct_data_row_scan_local_instructions / divisor,
            key_stream_local_instructions: total_direct_data_row_key_stream_local_instructions
                / divisor,
            row_read_local_instructions: total_direct_data_row_row_read_local_instructions
                / divisor,
            key_encode_local_instructions: total_direct_data_row_key_encode_local_instructions
                / divisor,
            store_get_local_instructions: total_direct_data_row_store_get_local_instructions
                / divisor,
            order_window_local_instructions: total_direct_data_row_order_window_local_instructions
                / divisor,
            page_window_local_instructions: total_direct_data_row_page_window_local_instructions
                / divisor,
        });
    }
    if saw_grouped {
        attribution.grouped = Some(GroupedExecutionAttribution {
            stream_local_instructions: total_grouped_stream_local_instructions / divisor,
            fold_local_instructions: total_grouped_fold_local_instructions / divisor,
            finalize_local_instructions: total_grouped_finalize_local_instructions / divisor,
            count: GroupedCountAttribution {
                borrowed_hash_computations: total_grouped_count_borrowed_hash_computations
                    / divisor,
                bucket_candidate_checks: total_grouped_count_bucket_candidate_checks / divisor,
                existing_group_hits: total_grouped_count_existing_group_hits / divisor,
                new_group_inserts: total_grouped_count_new_group_inserts / divisor,
                row_materialization_local_instructions:
                    total_grouped_count_row_materialization_local_instructions / divisor,
                group_lookup_local_instructions: total_grouped_count_group_lookup_local_instructions
                    / divisor,
                existing_group_update_local_instructions:
                    total_grouped_count_existing_group_update_local_instructions / divisor,
                new_group_insert_local_instructions:
                    total_grouped_count_new_group_insert_local_instructions / divisor,
            },
        });
    }
    attribution.store_get_calls = total_store_get_calls / divisor;
    attribution.index_store_get_calls = total_index_store_get_calls / divisor;
    attribution.index_store_range_scan_calls = total_index_store_range_scan_calls / divisor;
    attribution.index_store_entry_reads = total_index_store_entry_reads / divisor;
    attribution.response_decode_local_instructions =
        total_response_decode_local_instructions / divisor;
    attribution.execute_local_instructions = total_execute_local_instructions / divisor;
    attribution.total_local_instructions = total_local_instructions / divisor;
    attribution.shared_query_plan_cache_hits = total_shared_query_plan_cache_hits;
    attribution.shared_query_plan_cache_misses = total_shared_query_plan_cache_misses;

    attribution
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
fn query_entity_with_perf_loop<E>(sql: &str, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error>
where
    E: EntityFor<PerfAuditCanister>,
{
    if runs == 0 {
        return Err(invalid_perf_loop_runs_error());
    }

    let session = db()?;
    let mut first_result = None;
    let mut total_compile_local_instructions = 0_u64;
    let mut total_compile_cache_key_local_instructions = 0_u64;
    let mut total_compile_cache_lookup_local_instructions = 0_u64;
    let mut total_compile_parse_local_instructions = 0_u64;
    let mut total_compile_parse_tokenize_local_instructions = 0_u64;
    let mut total_compile_parse_select_local_instructions = 0_u64;
    let mut total_compile_parse_expr_local_instructions = 0_u64;
    let mut total_compile_parse_predicate_local_instructions = 0_u64;
    let mut total_compile_aggregate_lane_check_local_instructions = 0_u64;
    let mut total_compile_prepare_local_instructions = 0_u64;
    let mut total_compile_lower_local_instructions = 0_u64;
    let mut total_compile_bind_local_instructions = 0_u64;
    let mut total_compile_cache_insert_local_instructions = 0_u64;
    let mut total_plan_lookup_local_instructions = 0_u64;
    let mut total_planner_local_instructions = 0_u64;
    let mut total_store_local_instructions = 0_u64;
    let mut total_executor_invocation_local_instructions = 0_u64;
    let mut total_executor_local_instructions = 0_u64;
    let mut total_response_finalization_local_instructions = 0_u64;
    let mut total_pure_covering_decode_local_instructions = 0_u64;
    let mut total_pure_covering_row_assembly_local_instructions = 0_u64;
    let mut total_grouped_stream_local_instructions = 0_u64;
    let mut total_grouped_fold_local_instructions = 0_u64;
    let mut total_grouped_finalize_local_instructions = 0_u64;
    let mut grouped_count_totals = GroupedCountTotals::default();
    let mut total_store_get_calls = 0_u64;
    let mut total_index_store_get_calls = 0_u64;
    let mut total_index_store_range_scan_calls = 0_u64;
    let mut total_index_store_entry_reads = 0_u64;
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_sql_compiled_command_cache_hits = 0_u64;
    let mut total_sql_compiled_command_cache_misses = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;
    let mut saw_pure_covering = false;
    let mut saw_grouped = false;

    // Execute the same SQL through one session repeatedly so a real
    // session-local compiled-command cache can move the compile side honestly.
    for _ in 0..runs {
        let (result, attribution) = session.execute_trusted_sql_query_with_attribution::<E>(sql)?;
        if first_result.is_none() {
            first_result = Some(result);
        }

        total_compile_local_instructions =
            total_compile_local_instructions.saturating_add(attribution.compile_local_instructions);
        total_compile_cache_key_local_instructions = total_compile_cache_key_local_instructions
            .saturating_add(attribution.compile.cache_key_local_instructions);
        total_compile_cache_lookup_local_instructions =
            total_compile_cache_lookup_local_instructions
                .saturating_add(attribution.compile.cache_lookup_local_instructions);
        total_compile_parse_local_instructions = total_compile_parse_local_instructions
            .saturating_add(attribution.compile.parse_local_instructions);
        total_compile_parse_tokenize_local_instructions =
            total_compile_parse_tokenize_local_instructions
                .saturating_add(attribution.compile.parse_tokenize_local_instructions);
        total_compile_parse_select_local_instructions =
            total_compile_parse_select_local_instructions
                .saturating_add(attribution.compile.parse_select_local_instructions);
        total_compile_parse_expr_local_instructions = total_compile_parse_expr_local_instructions
            .saturating_add(attribution.compile.parse_expr_local_instructions);
        total_compile_parse_predicate_local_instructions =
            total_compile_parse_predicate_local_instructions
                .saturating_add(attribution.compile.parse_predicate_local_instructions);
        total_compile_aggregate_lane_check_local_instructions =
            total_compile_aggregate_lane_check_local_instructions
                .saturating_add(attribution.compile.aggregate_lane_check_local_instructions);
        total_compile_prepare_local_instructions = total_compile_prepare_local_instructions
            .saturating_add(attribution.compile.prepare_local_instructions);
        total_compile_lower_local_instructions = total_compile_lower_local_instructions
            .saturating_add(attribution.compile.lower_local_instructions);
        total_compile_bind_local_instructions = total_compile_bind_local_instructions
            .saturating_add(attribution.compile.bind_local_instructions);
        total_compile_cache_insert_local_instructions =
            total_compile_cache_insert_local_instructions
                .saturating_add(attribution.compile.cache_insert_local_instructions);
        total_plan_lookup_local_instructions = total_plan_lookup_local_instructions
            .saturating_add(attribution.plan_lookup_local_instructions);
        total_planner_local_instructions = total_planner_local_instructions
            .saturating_add(attribution.execution.planner_local_instructions);
        total_store_local_instructions = total_store_local_instructions
            .saturating_add(attribution.execution.store_local_instructions);
        total_executor_invocation_local_instructions = total_executor_invocation_local_instructions
            .saturating_add(attribution.execution.executor_invocation_local_instructions);
        total_executor_local_instructions = total_executor_local_instructions
            .saturating_add(attribution.execution.executor_local_instructions);
        total_response_finalization_local_instructions =
            total_response_finalization_local_instructions.saturating_add(
                attribution
                    .execution
                    .response_finalization_local_instructions,
            );
        if let Some(pure_covering) = attribution.pure_covering {
            saw_pure_covering = true;
            total_pure_covering_decode_local_instructions =
                total_pure_covering_decode_local_instructions
                    .saturating_add(pure_covering.decode_local_instructions);
            total_pure_covering_row_assembly_local_instructions =
                total_pure_covering_row_assembly_local_instructions
                    .saturating_add(pure_covering.row_assembly_local_instructions);
        }
        if let Some(grouped) = attribution.grouped {
            saw_grouped = true;
            total_grouped_stream_local_instructions = total_grouped_stream_local_instructions
                .saturating_add(grouped.stream_local_instructions);
            total_grouped_fold_local_instructions = total_grouped_fold_local_instructions
                .saturating_add(grouped.fold_local_instructions);
            total_grouped_finalize_local_instructions = total_grouped_finalize_local_instructions
                .saturating_add(grouped.finalize_local_instructions);
            grouped_count_totals.record_grouped_count(grouped.count);
        }
        total_store_get_calls = total_store_get_calls.saturating_add(attribution.store_get_calls);
        total_index_store_get_calls =
            total_index_store_get_calls.saturating_add(attribution.index_store_get_calls);
        total_index_store_range_scan_calls = total_index_store_range_scan_calls
            .saturating_add(attribution.index_store_range_scan_calls);
        total_index_store_entry_reads =
            total_index_store_entry_reads.saturating_add(attribution.index_store_entry_reads);
        total_response_decode_local_instructions = total_response_decode_local_instructions
            .saturating_add(attribution.response_decode_local_instructions);
        total_execute_local_instructions =
            total_execute_local_instructions.saturating_add(attribution.execute_local_instructions);
        total_local_instructions =
            total_local_instructions.saturating_add(attribution.total_local_instructions);
        total_sql_compiled_command_cache_hits = total_sql_compiled_command_cache_hits
            .saturating_add(attribution.cache.sql_compiled_command_hits);
        total_sql_compiled_command_cache_misses = total_sql_compiled_command_cache_misses
            .saturating_add(attribution.cache.sql_compiled_command_misses);
        total_shared_query_plan_cache_hits = total_shared_query_plan_cache_hits
            .saturating_add(attribution.cache.shared_query_plan_hits);
        total_shared_query_plan_cache_misses = total_shared_query_plan_cache_misses
            .saturating_add(attribution.cache.shared_query_plan_misses);
    }

    Ok(SqlQueryPerfResult {
        result: first_result.expect("perf loop with runs > 0 should record one result"),
        attribution: average_attribution(
            total_compile_local_instructions,
            total_compile_cache_key_local_instructions,
            total_compile_cache_lookup_local_instructions,
            total_compile_parse_local_instructions,
            total_compile_parse_tokenize_local_instructions,
            total_compile_parse_select_local_instructions,
            total_compile_parse_expr_local_instructions,
            total_compile_parse_predicate_local_instructions,
            total_compile_aggregate_lane_check_local_instructions,
            total_compile_prepare_local_instructions,
            total_compile_lower_local_instructions,
            total_compile_bind_local_instructions,
            total_compile_cache_insert_local_instructions,
            total_plan_lookup_local_instructions,
            total_planner_local_instructions,
            total_store_local_instructions,
            total_executor_invocation_local_instructions,
            total_executor_local_instructions,
            total_response_finalization_local_instructions,
            total_pure_covering_decode_local_instructions,
            total_pure_covering_row_assembly_local_instructions,
            total_grouped_stream_local_instructions,
            total_grouped_fold_local_instructions,
            total_grouped_finalize_local_instructions,
            grouped_count_totals.borrowed_hash_computations,
            grouped_count_totals.bucket_candidate_checks,
            grouped_count_totals.existing_group_hits,
            grouped_count_totals.new_group_inserts,
            grouped_count_totals.row_materialization_local_instructions,
            grouped_count_totals.group_lookup_local_instructions,
            grouped_count_totals.existing_group_update_local_instructions,
            grouped_count_totals.new_group_insert_local_instructions,
            total_store_get_calls,
            total_index_store_get_calls,
            total_index_store_range_scan_calls,
            total_index_store_entry_reads,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_sql_compiled_command_cache_hits,
            total_sql_compiled_command_cache_misses,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
            saw_pure_covering,
            saw_grouped,
            runs,
        ),
    })
}

#[cfg(feature = "sql")]
fn summarize_fluent_outcome<E>(result: &QueryResponse<E>) -> FluentQueryPerfOutcome
where
    E: EntityFor<PerfAuditCanister>,
{
    match result {
        QueryResponse::Rows(rows) => FluentQueryPerfOutcome {
            result_kind: "rows".to_string(),
            entity: E::MODEL.name().to_string(),
            row_count: rows.count(),
        },
        QueryResponse::Grouped(grouped) => FluentQueryPerfOutcome {
            result_kind: "grouped".to_string(),
            entity: E::MODEL.name().to_string(),
            row_count: u32::try_from(grouped.items().len()).unwrap_or(u32::MAX),
        },
    }
}

#[cfg(feature = "sql")]
fn run_user_fluent_scenario_once(
    session: &icydb::db::DbSession<PerfAuditCanister>,
    scenario: &str,
) -> Result<(FluentQueryPerfOutcome, QueryExecutionAttribution), icydb::Error> {
    match scenario {
        "user.id.order_only.asc.limit1" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_asc("id")
                .partial_window(1)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.id.order_only.asc.limit2" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_asc("id")
                .partial_window(2)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.age.order_only.asc.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_asc("age")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.age.order_only.asc.limit2.parity" => {
            let query = session
                .load::<PerfAuditUser>()
                .order_asc("age")
                .order_asc("id")
                .partial_window(2)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.active_true.order_age.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter_eq("active", true)
                .order_asc("age")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.field_compare.age_eq_age_nat.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter_eq_field("age", "age_nat")
                .order_asc("age")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.field_between.rank_age_age.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter_between_fields("rank", "age", "age")
                .order_asc("age")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.rank.in_list.limit3" => {
            let query = session
                .load::<PerfAuditUser>()
                .filter_in("rank", [17_i32, 28_i32, 30_i32])
                .order_asc("age")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "user.grouped.age_count.limit10" => {
            let query = session
                .load::<PerfAuditUser>()
                .group_by("age")?
                .aggregate(count())
                .order_asc("age")
                .partial_window(10)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        _ => Err(query_validate_error()),
    }
}

#[cfg(feature = "sql")]
fn run_account_fluent_scenario_once(
    session: &icydb::db::DbSession<PerfAuditCanister>,
    scenario: &str,
) -> Result<(FluentQueryPerfOutcome, QueryExecutionAttribution), icydb::Error> {
    match scenario {
        "account.active_true.order_handle.asc.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter_eq("active", true)
                .order_asc("handle")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "account.gold_active.order_handle.asc.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter_eq("active", true)
                .filter_eq("tier", "gold")
                .order_asc("handle")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "account.score_gte_75.order_score.limit3" => {
            let query = session
                .load::<PerfAuditAccount>()
                .filter_gte("score", 75_u64)
                .order_asc("score")
                .order_asc("id")
                .partial_window(3)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        _ => Err(query_validate_error()),
    }
}

#[cfg(feature = "sql")]
fn run_token_fluent_scenario_once(
    session: &icydb::db::DbSession<PerfAuditCanister>,
    scenario: &str,
) -> Result<(FluentQueryPerfOutcome, QueryExecutionAttribution), icydb::Error> {
    match scenario {
        "token.collection_stage_id.branch_set.full_entity.limit50" => {
            let query = session
                .load::<PerfAuditToken>()
                .filter_eq("collection_id", TOKEN_TARGET_COLLECTION)
                .filter_in("stage", ["Draft", "Review"])
                .order_asc("id")
                .partial_window(50)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "token.collection_stage_id.branch_set.duplicate_full_entity.limit50" => {
            let query = session
                .load::<PerfAuditToken>()
                .filter_eq("collection_id", TOKEN_TARGET_COLLECTION)
                .filter_in("stage", ["Draft", "Draft", "Review"])
                .order_asc("id")
                .partial_window(50)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "token.collection_stage_id.branch_set.wide_full_entity.limit50" => {
            let query = session
                .load::<PerfAuditToken>()
                .filter_eq("collection_id", TOKEN_TARGET_COLLECTION)
                .filter_in(
                    "stage",
                    [
                        "Draft",
                        "Review",
                        "Published",
                        "Archived",
                        "Queued",
                        "Rejected",
                        "Minted",
                        "Burned",
                        "Frozen",
                    ],
                )
                .order_asc("id")
                .partial_window(50)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        "token.collection_id.full_entity.limit300" => {
            let query = session
                .load::<PerfAuditToken>()
                .filter_eq("collection_id", TOKEN_TARGET_COLLECTION)
                .order_asc("id")
                .partial_window(300)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        _ => Err(query_validate_error()),
    }
}

#[cfg(feature = "sql")]
fn run_journaled_user_fluent_scenario_once(
    session: &icydb::db::DbSession<PerfAuditCanister>,
    scenario: &str,
) -> Result<(FluentQueryPerfOutcome, QueryExecutionAttribution), icydb::Error> {
    match scenario {
        "journaled_user.id.order_only.asc.limit1" => {
            let query = session
                .load::<PerfAuditJournaledUser>()
                .order_asc("id")
                .partial_window(1)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        _ => Err(query_validate_error()),
    }
}

#[cfg(feature = "sql")]
fn run_heap_user_fluent_scenario_once(
    session: &icydb::db::DbSession<PerfAuditCanister>,
    scenario: &str,
) -> Result<(FluentQueryPerfOutcome, QueryExecutionAttribution), icydb::Error> {
    match scenario {
        "heap_user.id.order_only.asc.limit1" => {
            let query = session
                .load::<PerfAuditHeapUser>()
                .order_asc("id")
                .partial_window(1)
                .trusted_read_unchecked();
            let (result, attribution) = query.execute_with_attribution()?;

            Ok((summarize_fluent_outcome(&result), attribution))
        }
        _ => Err(query_validate_error()),
    }
}

#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
fn query_fluent_scenario_loop(
    surface: &str,
    scenario: &str,
    runs: u32,
) -> Result<FluentQueryPerfResult, icydb::Error> {
    if runs == 0 {
        return Err(invalid_perf_loop_runs_error());
    }

    let session = db()?;
    let mut first_outcome = None;
    let mut total_compile_local_instructions = 0_u64;
    let mut total_compile_schema_catalog_local_instructions = 0_u64;
    let mut total_compile_schema_info_local_instructions = 0_u64;
    let mut total_compile_prepare_local_instructions = 0_u64;
    let mut total_compile_cache_key_local_instructions = 0_u64;
    let mut total_compile_cache_lookup_local_instructions = 0_u64;
    let mut total_compile_plan_build_local_instructions = 0_u64;
    let mut total_compile_cache_insert_local_instructions = 0_u64;
    let mut total_plan_lookup_local_instructions = 0_u64;
    let mut total_executor_invocation_local_instructions = 0_u64;
    let mut total_response_finalization_local_instructions = 0_u64;
    let mut total_load_plan_local_instructions = 0_u64;
    let mut total_row_layout_local_instructions = 0_u64;
    let mut total_continuation_signature_local_instructions = 0_u64;
    let mut total_scalar_runtime_handoff_local_instructions = 0_u64;
    let mut total_route_plan_local_instructions = 0_u64;
    let mut total_runtime_prepare_local_instructions = 0_u64;
    let mut total_runtime_local_instructions = 0_u64;
    let mut total_finalize_local_instructions = 0_u64;
    let mut total_direct_data_row_scan_local_instructions = 0_u64;
    let mut total_direct_data_row_key_stream_local_instructions = 0_u64;
    let mut total_direct_data_row_row_read_local_instructions = 0_u64;
    let mut total_direct_data_row_key_encode_local_instructions = 0_u64;
    let mut total_direct_data_row_store_get_local_instructions = 0_u64;
    let mut total_direct_data_row_order_window_local_instructions = 0_u64;
    let mut total_direct_data_row_page_window_local_instructions = 0_u64;
    let mut total_grouped_stream_local_instructions = 0_u64;
    let mut total_grouped_fold_local_instructions = 0_u64;
    let mut total_grouped_finalize_local_instructions = 0_u64;
    let mut grouped_count_totals = GroupedCountTotals::default();
    let mut total_store_get_calls = 0_u64;
    let mut total_index_store_get_calls = 0_u64;
    let mut total_index_store_range_scan_calls = 0_u64;
    let mut total_index_store_entry_reads = 0_u64;
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;
    let mut saw_direct_data_row = false;
    let mut saw_grouped = false;

    for _ in 0..runs {
        let (outcome, attribution) = match surface {
            "user" => run_user_fluent_scenario_once(&session, scenario)?,
            "account" => run_account_fluent_scenario_once(&session, scenario)?,
            "token" => run_token_fluent_scenario_once(&session, scenario)?,
            "heap_user" => run_heap_user_fluent_scenario_once(&session, scenario)?,
            "journaled_user" => run_journaled_user_fluent_scenario_once(&session, scenario)?,
            _ => {
                return Err(query_validate_error());
            }
        };

        if first_outcome.is_none() {
            first_outcome = Some(outcome);
        }

        total_compile_local_instructions =
            total_compile_local_instructions.saturating_add(attribution.compile_local_instructions);
        total_compile_schema_catalog_local_instructions =
            total_compile_schema_catalog_local_instructions
                .saturating_add(attribution.compile_schema_catalog_local_instructions);
        total_compile_schema_info_local_instructions = total_compile_schema_info_local_instructions
            .saturating_add(attribution.compile_schema_info_local_instructions);
        total_compile_prepare_local_instructions = total_compile_prepare_local_instructions
            .saturating_add(attribution.compile_prepare_local_instructions);
        total_compile_cache_key_local_instructions = total_compile_cache_key_local_instructions
            .saturating_add(attribution.compile_cache_key_local_instructions);
        total_compile_cache_lookup_local_instructions =
            total_compile_cache_lookup_local_instructions
                .saturating_add(attribution.compile_cache_lookup_local_instructions);
        total_compile_plan_build_local_instructions = total_compile_plan_build_local_instructions
            .saturating_add(attribution.compile_plan_build_local_instructions);
        total_compile_cache_insert_local_instructions =
            total_compile_cache_insert_local_instructions
                .saturating_add(attribution.compile_cache_insert_local_instructions);
        total_plan_lookup_local_instructions = total_plan_lookup_local_instructions
            .saturating_add(attribution.plan_lookup_local_instructions);
        total_executor_invocation_local_instructions = total_executor_invocation_local_instructions
            .saturating_add(attribution.executor_invocation_local_instructions);
        total_response_finalization_local_instructions =
            total_response_finalization_local_instructions
                .saturating_add(attribution.response_finalization_local_instructions);
        total_load_plan_local_instructions = total_load_plan_local_instructions
            .saturating_add(attribution.load_plan_local_instructions);
        total_row_layout_local_instructions = total_row_layout_local_instructions
            .saturating_add(attribution.row_layout_local_instructions);
        total_continuation_signature_local_instructions =
            total_continuation_signature_local_instructions
                .saturating_add(attribution.continuation_signature_local_instructions);
        total_scalar_runtime_handoff_local_instructions =
            total_scalar_runtime_handoff_local_instructions
                .saturating_add(attribution.scalar_runtime_handoff_local_instructions);
        total_route_plan_local_instructions = total_route_plan_local_instructions
            .saturating_add(attribution.route_plan_local_instructions);
        total_runtime_prepare_local_instructions = total_runtime_prepare_local_instructions
            .saturating_add(attribution.runtime_prepare_local_instructions);
        total_runtime_local_instructions =
            total_runtime_local_instructions.saturating_add(attribution.runtime_local_instructions);
        total_finalize_local_instructions = total_finalize_local_instructions
            .saturating_add(attribution.finalize_local_instructions);
        if let Some(direct_data_row) = attribution.direct_data_row {
            saw_direct_data_row = true;
            total_direct_data_row_scan_local_instructions =
                total_direct_data_row_scan_local_instructions
                    .saturating_add(direct_data_row.scan_local_instructions);
            total_direct_data_row_key_stream_local_instructions =
                total_direct_data_row_key_stream_local_instructions
                    .saturating_add(direct_data_row.key_stream_local_instructions);
            total_direct_data_row_row_read_local_instructions =
                total_direct_data_row_row_read_local_instructions
                    .saturating_add(direct_data_row.row_read_local_instructions);
            total_direct_data_row_key_encode_local_instructions =
                total_direct_data_row_key_encode_local_instructions
                    .saturating_add(direct_data_row.key_encode_local_instructions);
            total_direct_data_row_store_get_local_instructions =
                total_direct_data_row_store_get_local_instructions
                    .saturating_add(direct_data_row.store_get_local_instructions);
            total_direct_data_row_order_window_local_instructions =
                total_direct_data_row_order_window_local_instructions
                    .saturating_add(direct_data_row.order_window_local_instructions);
            total_direct_data_row_page_window_local_instructions =
                total_direct_data_row_page_window_local_instructions
                    .saturating_add(direct_data_row.page_window_local_instructions);
        }
        if let Some(grouped) = attribution.grouped {
            saw_grouped = true;
            total_grouped_stream_local_instructions = total_grouped_stream_local_instructions
                .saturating_add(grouped.stream_local_instructions);
            total_grouped_fold_local_instructions = total_grouped_fold_local_instructions
                .saturating_add(grouped.fold_local_instructions);
            total_grouped_finalize_local_instructions = total_grouped_finalize_local_instructions
                .saturating_add(grouped.finalize_local_instructions);
        }
        grouped_count_totals.record_fluent(&attribution);
        total_store_get_calls = total_store_get_calls.saturating_add(attribution.store_get_calls);
        total_index_store_get_calls =
            total_index_store_get_calls.saturating_add(attribution.index_store_get_calls);
        total_index_store_range_scan_calls = total_index_store_range_scan_calls
            .saturating_add(attribution.index_store_range_scan_calls);
        total_index_store_entry_reads =
            total_index_store_entry_reads.saturating_add(attribution.index_store_entry_reads);
        total_response_decode_local_instructions = total_response_decode_local_instructions
            .saturating_add(attribution.response_decode_local_instructions);
        total_execute_local_instructions =
            total_execute_local_instructions.saturating_add(attribution.execute_local_instructions);
        total_local_instructions =
            total_local_instructions.saturating_add(attribution.total_local_instructions);
        total_shared_query_plan_cache_hits = total_shared_query_plan_cache_hits
            .saturating_add(attribution.shared_query_plan_cache_hits);
        total_shared_query_plan_cache_misses = total_shared_query_plan_cache_misses
            .saturating_add(attribution.shared_query_plan_cache_misses);
    }

    Ok(FluentQueryPerfResult {
        outcome: first_outcome.expect("perf loop with runs > 0 should record one fluent outcome"),
        attribution: average_fluent_attribution(
            total_compile_local_instructions,
            total_compile_schema_catalog_local_instructions,
            total_compile_schema_info_local_instructions,
            total_compile_prepare_local_instructions,
            total_compile_cache_key_local_instructions,
            total_compile_cache_lookup_local_instructions,
            total_compile_plan_build_local_instructions,
            total_compile_cache_insert_local_instructions,
            total_plan_lookup_local_instructions,
            total_executor_invocation_local_instructions,
            total_response_finalization_local_instructions,
            total_load_plan_local_instructions,
            total_row_layout_local_instructions,
            total_continuation_signature_local_instructions,
            total_scalar_runtime_handoff_local_instructions,
            total_route_plan_local_instructions,
            total_runtime_prepare_local_instructions,
            total_runtime_local_instructions,
            total_finalize_local_instructions,
            total_direct_data_row_scan_local_instructions,
            total_direct_data_row_key_stream_local_instructions,
            total_direct_data_row_row_read_local_instructions,
            total_direct_data_row_key_encode_local_instructions,
            total_direct_data_row_store_get_local_instructions,
            total_direct_data_row_order_window_local_instructions,
            total_direct_data_row_page_window_local_instructions,
            total_grouped_stream_local_instructions,
            total_grouped_fold_local_instructions,
            total_grouped_finalize_local_instructions,
            grouped_count_totals.borrowed_hash_computations,
            grouped_count_totals.bucket_candidate_checks,
            grouped_count_totals.existing_group_hits,
            grouped_count_totals.new_group_inserts,
            grouped_count_totals.row_materialization_local_instructions,
            grouped_count_totals.group_lookup_local_instructions,
            grouped_count_totals.existing_group_update_local_instructions,
            grouped_count_totals.new_group_insert_local_instructions,
            total_store_get_calls,
            total_index_store_get_calls,
            total_index_store_range_scan_calls,
            total_index_store_entry_reads,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
            saw_direct_data_row,
            saw_grouped,
            runs,
        ),
    })
}

/// Clear all dedicated perf fixture rows from this canister.
#[update(name = "icydb_fixtures_reset")]
fn __icydb_fixtures_reset() -> Result<(), icydb::Error> {
    db()?.delete::<PerfAuditAccount>().execute()?;
    db()?.delete::<PerfAuditBlob>().execute()?;
    db()?.delete::<PerfAuditHeapUser>().execute()?;
    db()?.delete::<PerfAuditJournaledUser>().execute()?;
    db()?.delete::<PerfAuditToken>().execute()?;
    db()?.delete::<PerfAuditUser>().execute()?;

    Ok(())
}

/// Load one deterministic fixture batch tuned for SQL perf audit queries.
#[update(name = "icydb_fixtures_load")]
fn __icydb_fixtures_load() -> Result<(), icydb::Error> {
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(perf_audit_users())?;
    db()?.insert_many_atomic(perf_audit_heap_users())?;
    db()?.insert_many_atomic(perf_audit_journaled_users())?;
    db()?.insert_many_atomic(perf_audit_blobs())?;
    db()?.insert_many_atomic(perf_audit_accounts())?;
    db()?.insert_many_atomic(perf_audit_tokens())?;

    Ok(())
}

/// Load only the deterministic user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    let validated_rows = validate_scale_fixture_rows(row_count)?;
    let rows = perf_scale_users(validated_rows);
    let facts = scale_fixture_facts(
        "user",
        row_count,
        rows.len(),
        rows.iter().filter(|row| row.name.starts_with('A')).count(),
        rows.iter().filter(|row| row.id == 1).count(),
        rows.iter()
            .filter(|row| row.age >= 24 && row.age < 40)
            .count(),
        ScalePayloadProfile::NotApplicable,
    )?;
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(rows)?;

    Ok(facts)
}

/// Load only the deterministic account scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_account_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    let validated_rows = validate_scale_fixture_rows(row_count)?;
    let rows = perf_scale_accounts(validated_rows);
    let facts = scale_fixture_facts(
        "account",
        row_count,
        rows.len(),
        rows.iter()
            .filter(|row| row.handle.starts_with('a'))
            .count(),
        rows.iter().filter(|row| row.id == 1).count(),
        rows.iter()
            .filter(|row| row.tier == "gold" && row.active)
            .count(),
        ScalePayloadProfile::NotApplicable,
    )?;
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(rows)?;

    Ok(facts)
}

/// Load only the deterministic blob scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_blob_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    let validated_rows = validate_scale_fixture_rows(row_count)?;
    let rows = perf_scale_blobs(validated_rows);
    let facts = scale_fixture_facts(
        "blob",
        row_count,
        rows.len(),
        rows.iter()
            .filter(|row| row.label.starts_with("blob-"))
            .count(),
        rows.iter().filter(|row| row.id == 1).count(),
        rows.iter().filter(|row| row.bucket == 10).count(),
        ScalePayloadProfile::BlobCycleV1,
    )?;
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(rows)?;

    Ok(facts)
}

/// Load only the deterministic heap-user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_heap_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    let validated_rows = validate_scale_fixture_rows(row_count)?;
    let rows = perf_scale_heap_users(validated_rows);
    let facts = scale_user_mirror_fixture_facts("heap_user", row_count, &rows)?;
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(rows)?;

    Ok(facts)
}

/// Load only the deterministic journaled-user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_journaled_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    let validated_rows = validate_scale_fixture_rows(row_count)?;
    let rows = perf_scale_journaled_users(validated_rows);
    let facts = scale_journaled_user_fixture_facts(row_count, &rows)?;
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(rows)?;

    Ok(facts)
}

/// Load only the deterministic token scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_token_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    let validated_rows = validate_scale_fixture_rows(row_count)?;
    let rows = perf_scale_tokens(validated_rows);
    let first_id = Ulid::from_bytes(20_001_u128.to_be_bytes());
    let facts = scale_fixture_facts(
        "token",
        row_count,
        rows.len(),
        rows.iter()
            .filter(|row| row.collection_id == "missing-collection")
            .count(),
        rows.iter().filter(|row| row.id == first_id).count(),
        rows.iter()
            .filter(|row| row.collection_id == TOKEN_TARGET_COLLECTION)
            .count(),
        ScalePayloadProfile::NotApplicable,
    )?;
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(rows)?;

    Ok(facts)
}

/// Return accepted runtime schema descriptions in stable audit-surface order.
#[cfg(feature = "sql")]
#[query]
fn accepted_schema_descriptions() -> Result<Vec<EntitySchemaDescription>, icydb::Error> {
    let session = db()?;

    Ok(vec![
        session.try_describe_entity::<PerfAuditAccount>()?,
        session.try_describe_entity::<PerfAuditBlob>()?,
        session.try_describe_entity::<PerfAuditHeapUser>()?,
        session.try_describe_entity::<PerfAuditJournaledUser>()?,
        session.try_describe_entity::<PerfAuditToken>()?,
        session.try_describe_entity::<PerfAuditUser>()?,
    ])
}

/// Load a small journaled-only fixture for same-WASM upgrade/reentry
/// instruction probes. The full SQL perf corpus intentionally remains larger
/// than this audit budget.
#[update]
fn load_journaled_reentry_probe_fixture() -> Result<(), icydb::Error> {
    __icydb_fixtures_reset()?;
    db()?.insert_many_atomic(perf_audit_journaled_reentry_probe_users())?;

    Ok(())
}

/// Execute one PerfAuditUser-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_user(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db()?.execute_trusted_sql_query::<PerfAuditUser>(sql.as_str())
}

/// Execute one PerfAuditUser-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditUser-only SQL query through the fully attributed path
/// while measuring the same outer canister-local boundary as the total-only
/// calibration endpoint.
#[cfg(feature = "sql")]
#[query]
fn query_user_attributed_total_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let (result, _attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditUser>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlTotalOnlyPerfResult {
        result,
        instructions,
    })
}

/// Execute one PerfAuditUser-only SQL query through the normal non-attributed
/// path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_user_total_only_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_query::<PerfAuditUser>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlTotalOnlyPerfResult {
        result,
        instructions,
    })
}

/// Execute the primary user LIMIT 1 shape through the fluent query path and measure
/// only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_user_fluent_total_only_perf() -> Result<FluentTotalOnlyPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let response = db()?
        .load::<PerfAuditUser>()
        .order_asc("id")
        .partial_window(1)
        .trusted_read_unchecked()
        .execute()?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let outcome = summarize_fluent_outcome(&response);

    Ok(FluentTotalOnlyPerfResult {
        row_count: outcome.row_count,
        instructions,
    })
}

/// Execute one PerfAuditUser-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditUser-only SQL query repeatedly inside one canister
/// query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditUser>(sql.as_str(), runs)
}

#[cfg(feature = "sql")]
const fn unexpected_write_perf_count_error(
    _label: &str,
    _expected: u32,
    _actual: u32,
) -> icydb::Error {
    query_validate_error()
}

#[cfg(feature = "sql")]
const fn sql_write_result_row_count(result: &SqlQueryResult) -> Option<u32> {
    match result {
        SqlQueryResult::Count { row_count, .. } => Some(*row_count),
        SqlQueryResult::Projection(rows) => Some(rows.row_count),
        _ => None,
    }
}

#[cfg(feature = "sql")]
const fn ensure_sql_write_row_count(
    label: &str,
    result: &SqlQueryResult,
    expected: u32,
) -> Result<u32, icydb::Error> {
    let Some(actual) = sql_write_result_row_count(result) else {
        return Err(query_validate_error());
    };
    if actual != expected {
        return Err(unexpected_write_perf_count_error(label, expected, actual));
    }

    Ok(actual)
}

#[cfg(feature = "sql")]
fn measure_storage_write_matrix<E, B>(
    storage_label: &str,
    base_id: i32,
    build: B,
) -> Result<StorageWritePerfResult, icydb::Error>
where
    E: EntityFor<PerfAuditCanister>,
    B: Fn(i32, &str, i32) -> E + Copy,
{
    let session = db()?;
    let first_row = build(base_id, "first-insert", 41);
    let start = ic_cdk::api::performance_counter(1);
    session.insert(first_row)?;
    let first_insert_local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let mut steady_insert_total = 0_u64;
    for offset in 0..STORAGE_WRITE_MATRIX_RUNS {
        let id = base_id + 100 + i32::try_from(offset).unwrap_or(i32::MAX);
        let row = build(
            id,
            "steady-insert",
            42 + i32::try_from(offset % 7).unwrap_or(0),
        );
        let start = ic_cdk::api::performance_counter(1);
        session.insert(row)?;
        steady_insert_total =
            steady_insert_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
    }

    let mut steady_update_total = 0_u64;
    for offset in 0..STORAGE_WRITE_MATRIX_RUNS {
        let id = base_id + 100 + i32::try_from(offset).unwrap_or(i32::MAX);
        let row = build(
            id,
            "steady-update",
            51 + i32::try_from(offset % 7).unwrap_or(0),
        );
        let start = ic_cdk::api::performance_counter(1);
        session.update(row)?;
        steady_update_total =
            steady_update_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
    }

    let mut steady_delete_total = 0_u64;
    for offset in 0..STORAGE_WRITE_MATRIX_RUNS {
        let id = base_id + 100 + i32::try_from(offset).unwrap_or(i32::MAX);
        let start = ic_cdk::api::performance_counter(1);
        let deleted = session
            .delete::<E>()
            .filter(FieldRef::new("id").eq(id))
            .order_term(asc("id"))
            .max_affected(1)
            .execute()?;
        steady_delete_total =
            steady_delete_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
        if deleted != 1 {
            return Err(unexpected_write_perf_count_error(storage_label, 1, deleted));
        }
    }

    let read_back_id = base_id + 10_000;
    let read_back_row = build(read_back_id, "write-read-back", 73);
    let start = ic_cdk::api::performance_counter(1);
    session.insert(read_back_row)?;
    let response = session
        .load::<E>()
        .filter(FieldRef::new("id").eq(read_back_id))
        .order_asc("id")
        .partial_window(1)
        .trusted_read_unchecked()
        .execute()?;
    let write_then_read_back_local_instructions =
        ic_cdk::api::performance_counter(1).saturating_sub(start);
    let read_back_rows = summarize_fluent_outcome(&response).row_count;
    if read_back_rows != 1 {
        return Err(unexpected_write_perf_count_error(
            storage_label,
            1,
            read_back_rows,
        ));
    }

    Ok(StorageWritePerfResult {
        first_insert_local_instructions,
        steady_insert_avg_local_instructions: steady_insert_total
            / u64::from(STORAGE_WRITE_MATRIX_RUNS),
        steady_update_avg_local_instructions: steady_update_total
            / u64::from(STORAGE_WRITE_MATRIX_RUNS),
        steady_delete_avg_local_instructions: steady_delete_total
            / u64::from(STORAGE_WRITE_MATRIX_RUNS),
        write_then_read_back_local_instructions,
        read_back_rows,
    })
}

#[cfg(feature = "sql")]
fn sql_write_window_rows<E, B>(start_id: i32, label: &str, age: i32, build: B) -> Vec<E>
where
    B: Fn(i32, &str, i32) -> E + Copy,
{
    (0..SQL_WRITE_MATERIALIZATION_ROWS)
        .map(|offset| {
            build(
                start_id + offset,
                &format!("{label}-{offset:03}"),
                age + (offset % 7),
            )
        })
        .collect()
}

#[cfg(feature = "sql")]
fn measure_sql_write_statement<E>(
    label: &str,
    sql: &str,
    expected_rows: u32,
) -> Result<(u64, u32), icydb::Error>
where
    E: EntityFor<PerfAuditCanister>,
{
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_mutation::<E>(sql)?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let row_count = ensure_sql_write_row_count(label, &result, expected_rows)?;

    Ok((instructions, row_count))
}

#[cfg(feature = "sql")]
fn measure_sql_write_materialization_matrix<E, B>(
    entity_name: &str,
    base_id: i32,
    build: B,
) -> Result<SqlWriteMaterializationPerfResult, icydb::Error>
where
    E: EntityFor<PerfAuditCanister>,
    B: Fn(i32, &str, i32) -> E + Copy,
{
    let expected_rows = u32::try_from(SQL_WRITE_MATERIALIZATION_ROWS).unwrap_or(u32::MAX);
    let update_count_start = base_id + 2_000;
    let update_returning_start = base_id + 3_000;
    let delete_count_start = base_id + 4_000;
    let delete_returning_start = base_id + 5_000;

    db()?.insert_many_atomic(sql_write_window_rows(
        update_count_start,
        "update-count",
        41,
        build,
    ))?;
    db()?.insert_many_atomic(sql_write_window_rows(
        update_returning_start,
        "update-returning",
        51,
        build,
    ))?;
    db()?.insert_many_atomic(sql_write_window_rows(
        delete_count_start,
        "delete-count",
        61,
        build,
    ))?;
    db()?.insert_many_atomic(sql_write_window_rows(
        delete_returning_start,
        "delete-returning",
        71,
        build,
    ))?;

    let update_count_end = update_count_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let update_returning_end = update_returning_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let delete_count_end = delete_count_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let delete_returning_end = delete_returning_start + SQL_WRITE_MATERIALIZATION_ROWS;

    let update_count = measure_sql_write_statement::<E>(
        "SQL write materialization UPDATE count",
        &format!(
            "UPDATE {entity_name} SET age = 77 \
             WHERE id >= {update_count_start} AND id < {update_count_end}"
        ),
        expected_rows,
    )?;
    let update_returning = measure_sql_write_statement::<E>(
        "SQL write materialization UPDATE RETURNING",
        &format!(
            "UPDATE {entity_name} SET age = 78 \
             WHERE id >= {update_returning_start} AND id < {update_returning_end} \
             RETURNING id"
        ),
        expected_rows,
    )?;
    let delete_count = measure_sql_write_statement::<E>(
        "SQL write materialization DELETE count",
        &format!(
            "DELETE FROM {entity_name} \
             WHERE id >= {delete_count_start} AND id < {delete_count_end}"
        ),
        expected_rows,
    )?;
    let delete_returning = measure_sql_write_statement::<E>(
        "SQL write materialization DELETE RETURNING",
        &format!(
            "DELETE FROM {entity_name} \
             WHERE id >= {delete_returning_start} AND id < {delete_returning_end} \
             RETURNING id"
        ),
        expected_rows,
    )?;

    Ok(SqlWriteMaterializationPerfResult {
        local_instructions: [
            update_count.0,
            update_returning.0,
            delete_count.0,
            delete_returning.0,
        ],
        rows: [
            update_count.1,
            update_returning.1,
            delete_count.1,
            delete_returning.1,
        ],
    })
}

/// Measure the heap typed write path.
#[cfg(feature = "sql")]
#[update]
fn measure_heap_user_write_matrix_perf() -> Result<StorageWritePerfResult, icydb::Error> {
    measure_storage_write_matrix::<PerfAuditHeapUser, _>(
        "heap write matrix",
        30_000,
        build_perf_audit_heap_user,
    )
}

/// Measure the journaled typed write path.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_write_matrix_perf() -> Result<StorageWritePerfResult, icydb::Error> {
    measure_storage_write_matrix::<PerfAuditJournaledUser, _>(
        "journaled write matrix",
        40_000,
        build_perf_audit_journaled_user,
    )
}

/// Measure broad SQL write materialization shapes against heap storage.
#[cfg(feature = "sql")]
#[update]
fn measure_heap_user_sql_write_materialization_perf()
-> Result<SqlWriteMaterializationPerfResult, icydb::Error> {
    measure_sql_write_materialization_matrix::<PerfAuditHeapUser, _>(
        "PerfAuditHeapUser",
        50_000,
        build_perf_audit_heap_user,
    )
}

/// Measure broad SQL write materialization shapes against journaled storage.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_sql_write_materialization_perf()
-> Result<SqlWriteMaterializationPerfResult, icydb::Error> {
    measure_sql_write_materialization_matrix::<PerfAuditJournaledUser, _>(
        "PerfAuditJournaledUser",
        60_000,
        build_perf_audit_journaled_user,
    )
}

/// Execute one PerfAuditHeapUser-only SQL query and attach one local
/// instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditHeapUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditHeapUser-only SQL query through the normal
/// non-attributed path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_total_only_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_query::<PerfAuditHeapUser>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlTotalOnlyPerfResult {
        result,
        instructions,
    })
}

/// Execute the heap LIMIT 1 shape through the fluent query path and measure
/// only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_fluent_total_only_perf() -> Result<FluentTotalOnlyPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let response = db()?
        .load::<PerfAuditHeapUser>()
        .order_asc("id")
        .partial_window(1)
        .trusted_read_unchecked()
        .execute()?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let outcome = summarize_fluent_outcome(&response);

    Ok(FluentTotalOnlyPerfResult {
        row_count: outcome.row_count,
        instructions,
    })
}

/// Execute the heap LIMIT 1 shape through the fluent query path and attach the
/// shared fluent query phase attribution.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_fluent_with_perf() -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("heap_user", "heap_user.id.order_only.asc.limit1", 1)
}

/// Execute one PerfAuditHeapUser-only SQL query through the update surface so
/// the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_heap_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditHeapUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditHeapUser-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditHeapUser>(sql.as_str(), runs)
}

/// Execute one PerfAuditJournaledUser-only SQL query and attach one local
/// instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditJournaledUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditJournaledUser-only SQL query through the normal
/// non-attributed path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_total_only_perf(
    sql: String,
) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_query::<PerfAuditJournaledUser>(sql.as_str())?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SqlTotalOnlyPerfResult {
        result,
        instructions,
    })
}

/// Execute the journaled LIMIT 1 shape through the fluent query path and
/// measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_fluent_total_only_perf() -> Result<FluentTotalOnlyPerfResult, icydb::Error>
{
    let start = ic_cdk::api::performance_counter(1);
    let response = db()?
        .load::<PerfAuditJournaledUser>()
        .order_asc("id")
        .partial_window(1)
        .trusted_read_unchecked()
        .execute()?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let outcome = summarize_fluent_outcome(&response);

    Ok(FluentTotalOnlyPerfResult {
        row_count: outcome.row_count,
        instructions,
    })
}

/// Execute the journaled LIMIT 1 shape through an update call. After a
/// same-WASM upgrade this gives the integration harness one normal guarded
/// reentry probe that includes any required recovery/rebuild work.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_reentry_perf() -> Result<FluentTotalOnlyPerfResult, icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let response = db()?
        .load::<PerfAuditJournaledUser>()
        .order_asc("id")
        .partial_window(1)
        .trusted_read_unchecked()
        .execute()?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let outcome = summarize_fluent_outcome(&response);

    Ok(FluentTotalOnlyPerfResult {
        row_count: outcome.row_count,
        instructions,
    })
}

/// Execute the journaled LIMIT 1 shape through the fluent query path and
/// attach the shared fluent query phase attribution.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_fluent_with_perf() -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop(
        "journaled_user",
        "journaled_user.id.order_only.asc.limit1",
        1,
    )
}

/// Execute one PerfAuditJournaledUser-only SQL query through the update surface
/// so the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_journaled_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditJournaledUser>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditJournaledUser-only SQL query repeatedly inside
/// one canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditJournaledUser>(sql.as_str(), runs)
}

/// Execute one PerfAuditAccount-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_account(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db()?.execute_trusted_sql_query::<PerfAuditAccount>(sql.as_str())
}

/// Execute one PerfAuditAccount-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditAccount>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditAccount-only SQL query through the update surface so
/// the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_account_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditAccount>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditAccount-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditAccount>(sql.as_str(), runs)
}

/// Execute one PerfAuditBlob-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_blob(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db()?.execute_trusted_sql_query::<PerfAuditBlob>(sql.as_str())
}

/// Execute one PerfAuditBlob-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditBlob>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditBlob-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_blob_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditBlob>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditBlob-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditBlob>(sql.as_str(), runs)
}

/// Execute one PerfAuditToken-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_token(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    db()?.execute_trusted_sql_query::<PerfAuditToken>(sql.as_str())
}

/// Execute one PerfAuditToken-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditToken>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute one PerfAuditToken-only SQL query through the update surface so the
/// canister can persist warmed query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_token_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    let (result, attribution) =
        db()?.execute_trusted_sql_query_with_attribution::<PerfAuditToken>(sql.as_str())?;

    Ok(SqlQueryPerfResult {
        result,
        attribution,
    })
}

/// Execute the same PerfAuditToken-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    query_entity_with_perf_loop::<PerfAuditToken>(sql.as_str(), runs)
}

/// Execute one dedicated PerfAuditUser fluent perf scenario and attach one
/// local instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("user", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditUser fluent perf scenario through the update
/// surface so the shared lower query cache can persist for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_user_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("user", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditUser fluent perf scenario repeatedly inside
/// one canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_fluent_loop_with_perf(
    scenario: String,
    runs: u32,
) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("user", scenario.as_str(), runs)
}

/// Execute one dedicated PerfAuditAccount fluent perf scenario and attach one
/// local instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("account", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditAccount fluent perf scenario through the
/// update surface so the shared lower query cache can persist for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_account_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("account", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditAccount fluent perf scenario repeatedly
/// inside one canister query call and report the per-run average instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_fluent_loop_with_perf(
    scenario: String,
    runs: u32,
) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("account", scenario.as_str(), runs)
}

/// Execute one dedicated PerfAuditToken fluent perf scenario and attach one
/// local instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("token", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditToken fluent perf scenario through the
/// update surface so the shared lower query cache can persist for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_token_fluent_with_perf(scenario: String) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("token", scenario.as_str(), 1)
}

/// Execute one dedicated PerfAuditToken fluent perf scenario repeatedly inside
/// one canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_fluent_loop_with_perf(
    scenario: String,
    runs: u32,
) -> Result<FluentQueryPerfResult, icydb::Error> {
    query_fluent_scenario_loop("token", scenario.as_str(), runs)
}

#[cfg(feature = "sql")]
macro_rules! focused_fluent_row {
    (
        $scenario_key:expr,
        $terminal:expr,
        $canonicalization_result:expr,
        $raw_key_count:expr,
        $deduplicated_key_count:expr,
        $explanation:expr,
        $query:expr
    ) => {{
        let selected_access = match ($query).trusted_read_unchecked().explain_execution() {
            Ok(explain) => focused_access_label(format!("{:?}", explain.node_type()).as_str()),
            Err(err) => {
                let err: icydb::Error = err.into();
                format!("ExplainError({})", focused_error_code(&err))
            }
        };
        let public_result = ($query).execute_rows();
        match public_result {
            Ok(rows) => {
                let result_signature = focused_rows_signature(&rows);
                let rows_returned = rows.count();
                let attributed = ($query).execute_with_attribution();
                match attributed {
                    Ok((_result, attribution)) => focused_fluent_success_row(
                        $scenario_key,
                        $terminal,
                        selected_access.as_str(),
                        $canonicalization_result,
                        $raw_key_count,
                        $deduplicated_key_count,
                        result_signature,
                        rows_returned,
                        &attribution,
                        $explanation,
                    ),
                    Err(err) => focused_error_row(
                        $scenario_key,
                        $terminal,
                        selected_access.as_str(),
                        "execution_error",
                        Some(focused_error_code(&err)),
                        $canonicalization_result,
                        $raw_key_count,
                        $deduplicated_key_count,
                        $explanation,
                    ),
                }
            }
            Err(err) => {
                let err: icydb::Error = err.into();
                focused_error_row(
                    $scenario_key,
                    $terminal,
                    selected_access.as_str(),
                    "rejected",
                    Some(focused_error_code(&err)),
                    $canonicalization_result,
                    $raw_key_count,
                    $deduplicated_key_count,
                    $explanation,
                )
            }
        }
    }};
}

/// Capture one focused exact-key canonicalization scenario for the 0.197
/// closeout artifact. This is audit-only plumbing: each row either records a
/// measured PocketIC query execution or an explicit fail-closed/contract-only
/// boundary row, but it does not alter planner or executor behavior.
#[cfg(feature = "sql")]
#[allow(clippy::too_many_lines)]
#[update]
fn capture_pk_canonicalization_focused_scenario(
    scenario: String,
) -> Result<FocusedPkPerfRow, icydb::Error> {
    match scenario.as_str() {
        "pk.scalar.generated.filter.existing.try_one" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "try_one",
            "ByKey",
            1_u32,
            1_u32,
            "measured current exact primary-key filter over PerfAuditUser",
            db()?.load::<PerfAuditUser>().filter_eq("id", 1_i32)
        )),
        "pk.scalar.generated.filter.missing.try_one" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "try_one",
            "ByKey",
            1_u32,
            1_u32,
            "measured current missing exact primary-key filter over PerfAuditHeapUser",
            db()?
                .load::<PerfAuditHeapUser>()
                .filter_eq("id", 99_999_i32)
        )),
        "pk.scalar.generated.by_id.existing.try_one" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "try_one",
            "ByKey",
            1_u32,
            1_u32,
            "measured current explicit by_id baseline over PerfAuditUser",
            db()?.load::<PerfAuditUser>().by_id(Id::from_key(1_i32))
        )),
        "pk.scalar.external.filter.existing.try_one" => Ok(focused_contract_row(
            scenario.as_str(),
            "try_one",
            "ByKey",
            "ByKey",
            1,
            1,
            "contract-backed external Principal primary-key filter; core session tests provide measured semantic coverage",
        )),
        "pk.scalar.external.by_id.existing.try_one" => Ok(focused_contract_row(
            scenario.as_str(),
            "try_one",
            "ByKey",
            "ByKey",
            1,
            1,
            "contract-backed external Principal explicit by_id baseline; core session tests provide measured semantic coverage",
        )),
        "pk.sql.literal.generated.existing" => Ok(focused_sql_user_row(
            scenario.as_str(),
            "projection",
            "ByKey",
            1,
            1,
            "SELECT id, name FROM PerfAuditUser WHERE id = 1",
            "measured current SQL literal primary-key equality",
        )),
        "pk.sql.literal.generated.commuted" => Ok(focused_sql_user_row(
            scenario.as_str(),
            "projection",
            "ByKey",
            1,
            1,
            "SELECT id, name FROM PerfAuditUser WHERE 1 = id",
            "measured current SQL commuted literal primary-key equality",
        )),
        "pk.sql.parameter.unsupported" => Ok(focused_sql_user_row(
            scenario.as_str(),
            "projection",
            "UnsupportedByContract",
            0,
            0,
            "SELECT id, name FROM PerfAuditUser WHERE id = ?",
            "measured current SQL placeholder fail-closed boundary",
        )),
        "pk.sql.literal.generated.wrong_type" => Ok(focused_sql_user_row(
            scenario.as_str(),
            "projection",
            "ValidationFailure",
            1,
            0,
            "SELECT id, name FROM PerfAuditUser WHERE id = 'not-an-int'",
            "measured current SQL wrong-type primary-key literal failure",
        )),
        "pk.in.fluent.empty" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "Empty",
            0_u32,
            0_u32,
            "measured current empty primary-key IN filter",
            db()?
                .load::<PerfAuditUser>()
                .filter_in("id", Vec::<i32>::new())
        )),
        "pk.in.fluent.one" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKey",
            1_u32,
            1_u32,
            "measured current one-value primary-key IN filter",
            db()?.load::<PerfAuditUser>().filter_in("id", [1_i32])
        )),
        "pk.in.fluent.duplicates" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKeys",
            3_u32,
            2_u32,
            "measured current duplicate primary-key IN filter",
            db()?
                .load::<PerfAuditUser>()
                .filter_in("id", [2_i32, 1_i32, 2_i32])
        )),
        "pk.in.fluent.multiple_mixed" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKeys",
            3_u32,
            2_u32,
            "measured current mixed existing/missing primary-key IN filter",
            db()?
                .load::<PerfAuditUser>()
                .filter_in("id", [1_i32, 99_999_i32, 1_i32])
        )),
        "pk.in.fluent.raw_terms_over_budget" => Ok(focused_error_row(
            scenario.as_str(),
            "rows",
            "ByKeys",
            "public_policy_rejected_not_measured",
            Some("E204".to_string()),
            "ByKeys",
            1_025_u32,
            1_u32,
            "contract-backed public-read raw primary-key IN input cap; default execution lane is intentionally not used for this policy row",
        )),
        "pk.in.fluent.deduped_over_budget" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKeys",
            1_025_u32,
            1_025_u32,
            "measured current deduplicated primary-key IN public-read cap failure",
            db()?
                .load::<PerfAuditUser>()
                .filter_in("id", 10_000_i32..11_025_i32)
        )),
        "pk.in.fluent.by_ids.raw_terms_over_budget" => Ok(focused_error_row(
            scenario.as_str(),
            "rows",
            "ByKey",
            "public_policy_rejected_not_measured",
            Some("E204".to_string()),
            "ByKey",
            1_025_u32,
            1_u32,
            "contract-backed public-read duplicate-heavy by_ids raw input cap; default execution lane is intentionally not used for this policy row",
        )),
        "pk.in.sql.duplicates.order_asc" => Ok(focused_sql_user_row(
            scenario.as_str(),
            "projection",
            "ByKeys",
            3,
            2,
            "SELECT id FROM PerfAuditUser WHERE id IN (2, 1, 2) ORDER BY id ASC",
            "measured current SQL duplicate primary-key IN deterministic order",
        )),
        "pk.in.sql.payload_over_budget" => Ok(focused_error_row(
            scenario.as_str(),
            "projection",
            "ByKeys",
            "public_policy_rejected_not_measured",
            Some("E204".to_string()),
            "ByKeys",
            1_025,
            1_025,
            "contract-backed public-read SQL primary-key IN payload cap; default execution lane is intentionally not used for this policy row",
        )),
        "pk.residual.eq.true" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKey",
            1_u32,
            1_u32,
            "measured current primary-key equality with true residual",
            db()?.load::<PerfAuditUser>().filter(FilterExpr::and(vec![
                FieldRef::new("id").eq(1_i32),
                FieldRef::new("active").eq(true),
            ]))
        )),
        "pk.residual.eq.false" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKey",
            1_u32,
            1_u32,
            "measured current primary-key equality with false residual",
            db()?.load::<PerfAuditUser>().filter(FilterExpr::and(vec![
                FieldRef::new("id").eq(1_i32),
                FieldRef::new("active").eq(false),
            ]))
        )),
        "pk.residual.eq.invalid_existing" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ValidationFailure",
            1_u32,
            1_u32,
            "measured current invalid residual on existing primary key fails closed",
            db()?.load::<PerfAuditUser>().filter(FilterExpr::and(vec![
                FieldRef::new("id").eq(1_i32),
                FieldRef::new("missing").eq(1_i32),
            ]))
        )),
        "pk.residual.eq.invalid_missing" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ValidationFailure",
            1_u32,
            1_u32,
            "measured current invalid residual on missing primary key fails closed",
            db()?.load::<PerfAuditUser>().filter(FilterExpr::and(vec![
                FieldRef::new("id").eq(99_999_i32),
                FieldRef::new("missing").eq(1_i32),
            ]))
        )),
        "pk.empty.contradictory_eq" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "Empty",
            2_u32,
            0_u32,
            "measured current contradictory primary-key equality filter",
            db()?.load::<PerfAuditUser>().filter(FilterExpr::and(vec![
                FieldRef::new("id").eq(1_i32),
                FieldRef::new("id").eq(2_i32),
            ]))
        )),
        "pk.empty.eq_and_excluding_in" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "Empty",
            3_u32,
            0_u32,
            "measured current primary-key equality excluded by IN filter",
            db()?.load::<PerfAuditUser>().filter(FilterExpr::and(vec![
                FieldRef::new("id").eq(1_i32),
                FieldRef::new("id").in_list([2_i32, 3_i32]),
            ]))
        )),
        "pk.empty.count" => Ok(focused_empty_count_row(scenario.as_str())),
        "pk.empty.require_one" => Ok(focused_empty_require_one_row(scenario.as_str())),
        "pk.store.heap.existing" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKey",
            1_u32,
            1_u32,
            "measured current heap-store exact primary-key filter",
            db()?.load::<PerfAuditHeapUser>().filter_eq("id", 1_i32)
        )),
        "pk.store.journaled.existing" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "ByKey",
            1_u32,
            1_u32,
            "measured current journaled-store exact primary-key filter",
            db()?
                .load::<PerfAuditJournaledUser>()
                .filter_eq("id", 1_i32)
        )),
        "pk.store.heap.deleted" => {
            let _ = db()?
                .delete::<PerfAuditHeapUser>()
                .by_id(Id::from_key(2_i32))
                .execute();
            Ok(focused_fluent_row!(
                scenario.as_str(),
                "rows",
                "ByKey",
                1_u32,
                1_u32,
                "measured current heap-store deleted exact-key lookup",
                db()?.load::<PerfAuditHeapUser>().filter_eq("id", 2_i32)
            ))
        }
        "pk.store.journaled.deleted" => {
            let _ = db()?
                .delete::<PerfAuditJournaledUser>()
                .by_id(Id::from_key(2_i32))
                .execute();
            Ok(focused_fluent_row!(
                scenario.as_str(),
                "rows",
                "ByKey",
                1_u32,
                1_u32,
                "measured current journaled-store deleted exact-key lookup",
                db()?
                    .load::<PerfAuditJournaledUser>()
                    .filter_eq("id", 2_i32)
            ))
        }
        "pk.noncanonical.unique_secondary" => Ok(focused_fluent_row!(
            scenario.as_str(),
            "rows",
            "NotApplied",
            0_u32,
            0_u32,
            "measured current secondary-field equality remains off primary-key access",
            db()?.load::<PerfAuditUser>().filter_eq("name", "Alice")
        )),
        "pk.noncanonical.partial_composite" => Ok(focused_error_row(
            scenario.as_str(),
            "rows",
            "Unsupported",
            "unsupported_by_fixture",
            None,
            "NotApplied",
            0,
            0,
            "contract-only row: sql_perf fixture has no composite-primary-key entity",
        )),
        "pk.noncanonical.expression_wrapped" => Ok(focused_sql_user_row(
            scenario.as_str(),
            "projection",
            "NotApplied",
            0,
            0,
            "SELECT id FROM PerfAuditUser WHERE id + 0 = 1",
            "measured current expression-wrapped primary-key SQL boundary",
        )),
        _ => Err(query_validate_error()),
    }
}

#[cfg(feature = "sql")]
fn focused_access_label(node_type: &str) -> String {
    match node_type {
        "ByKeyLookup" => "ByKey",
        "ByKeysLookup" => "ByKeys",
        "FullScan" => "FullScan",
        "IndexPrefixScan" => "IndexPrefix",
        "IndexRangeScan" => "IndexRange",
        "IndexMultiLookup" => "IndexMultiLookup",
        "IndexBranchSet" => "IndexBranchSet",
        "Union" => "Union",
        "Intersection" => "Intersection",
        other => other,
    }
    .to_string()
}

#[cfg(feature = "sql")]
fn focused_error_code(err: &icydb::Error) -> String {
    format!("E{}", err.code().raw())
}

#[cfg(feature = "sql")]
fn focused_rows_signature<E>(rows: &icydb::db::response::Response<E>) -> String
where
    E: EntityFor<PerfAuditCanister>,
{
    let ids = rows
        .ids()
        .map(|id| format!("{id:?}"))
        .collect::<Vec<_>>()
        .join(",");

    format!("rows|{}|{}|{}", E::MODEL.name(), rows.count(), ids)
}

#[cfg(feature = "sql")]
#[expect(
    clippy::too_many_arguments,
    reason = "artifact row builder keeps the emitted schema explicit at call sites"
)]
fn focused_fluent_success_row(
    scenario_key: &str,
    terminal: &str,
    selected_access: &str,
    canonicalization_result: &str,
    raw_key_count: u32,
    deduplicated_key_count: u32,
    result_signature: String,
    rows_returned: u32,
    attribution: &QueryExecutionAttribution,
    explanation: &str,
) -> FocusedPkPerfRow {
    let direct_data_row = attribution.direct_data_row.unwrap_or_default();
    FocusedPkPerfRow {
        scenario_key: scenario_key.to_string(),
        terminal: terminal.to_string(),
        selected_access: selected_access.to_string(),
        admission_result: "admitted".to_string(),
        error_code: None,
        total_instructions: attribution.total_local_instructions,
        planner_instructions: attribution
            .compile_plan_build_local_instructions
            .saturating_add(attribution.plan_lookup_local_instructions)
            .saturating_add(attribution.route_plan_local_instructions),
        execute_instructions: attribution.execute_local_instructions,
        store_instructions: direct_data_row.store_get_local_instructions,
        data_store_get: attribution.store_get_calls,
        index_ranges: attribution.index_store_range_scan_calls,
        rows_decoded: attribution.store_get_calls,
        rows_returned: u64::from(rows_returned),
        result_signature,
        canonicalization_result: canonicalization_result.to_string(),
        raw_key_count,
        deduplicated_key_count,
        explanation: explanation.to_string(),
    }
}

#[cfg(feature = "sql")]
#[expect(
    clippy::too_many_arguments,
    reason = "artifact row builder keeps fail-closed scenario metadata explicit"
)]
fn focused_error_row(
    scenario_key: &str,
    terminal: &str,
    selected_access: &str,
    admission_result: &str,
    error_code: Option<String>,
    canonicalization_result: &str,
    raw_key_count: u32,
    deduplicated_key_count: u32,
    explanation: &str,
) -> FocusedPkPerfRow {
    FocusedPkPerfRow {
        scenario_key: scenario_key.to_string(),
        terminal: terminal.to_string(),
        selected_access: selected_access.to_string(),
        admission_result: admission_result.to_string(),
        error_code,
        total_instructions: 0,
        planner_instructions: 0,
        execute_instructions: 0,
        store_instructions: 0,
        data_store_get: 0,
        index_ranges: 0,
        rows_decoded: 0,
        rows_returned: 0,
        result_signature: "error".to_string(),
        canonicalization_result: canonicalization_result.to_string(),
        raw_key_count,
        deduplicated_key_count,
        explanation: explanation.to_string(),
    }
}

#[cfg(feature = "sql")]
fn focused_contract_row(
    scenario_key: &str,
    terminal: &str,
    selected_access: &str,
    canonicalization_result: &str,
    raw_key_count: u32,
    deduplicated_key_count: u32,
    explanation: &str,
) -> FocusedPkPerfRow {
    FocusedPkPerfRow {
        scenario_key: scenario_key.to_string(),
        terminal: terminal.to_string(),
        selected_access: selected_access.to_string(),
        admission_result: "contract_backed_not_measured".to_string(),
        error_code: None,
        total_instructions: 0,
        planner_instructions: 0,
        execute_instructions: 0,
        store_instructions: 0,
        data_store_get: 0,
        index_ranges: 0,
        rows_decoded: 0,
        rows_returned: 1,
        result_signature: "contract_backed_not_measured".to_string(),
        canonicalization_result: canonicalization_result.to_string(),
        raw_key_count,
        deduplicated_key_count,
        explanation: explanation.to_string(),
    }
}

#[cfg(feature = "sql")]
fn focused_sql_user_row(
    scenario_key: &str,
    terminal: &str,
    canonicalization_result: &str,
    raw_key_count: u32,
    deduplicated_key_count: u32,
    sql: &str,
    explanation: &str,
) -> FocusedPkPerfRow {
    let execution = db().map_err(icydb::Error::from).and_then(|session| {
        session.execute_trusted_sql_query_with_attribution::<PerfAuditUser>(sql)
    });

    match execution {
        Ok((result, attribution)) => FocusedPkPerfRow {
            scenario_key: scenario_key.to_string(),
            terminal: terminal.to_string(),
            selected_access: canonicalization_result.to_string(),
            admission_result: "admitted".to_string(),
            error_code: None,
            total_instructions: attribution.total_local_instructions,
            planner_instructions: attribution
                .execution
                .planner_local_instructions
                .saturating_add(attribution.execution.planner_plan_build_local_instructions),
            execute_instructions: attribution.execute_local_instructions,
            store_instructions: attribution.execution.store_local_instructions,
            data_store_get: attribution.store_get_calls,
            index_ranges: attribution.index_store_range_scan_calls,
            rows_decoded: attribution.store_get_calls,
            rows_returned: u64::from(focused_sql_row_count(&result)),
            result_signature: focused_sql_result_signature(&result),
            canonicalization_result: canonicalization_result.to_string(),
            raw_key_count,
            deduplicated_key_count,
            explanation: explanation.to_string(),
        },
        Err(err) => focused_error_row(
            scenario_key,
            terminal,
            canonicalization_result,
            "rejected",
            Some(focused_error_code(&err)),
            canonicalization_result,
            raw_key_count,
            deduplicated_key_count,
            explanation,
        ),
    }
}

#[cfg(feature = "sql")]
fn focused_sql_row_count(result: &SqlQueryResult) -> u32 {
    match result {
        SqlQueryResult::Count { row_count, .. } => *row_count,
        SqlQueryResult::Projection(rows) => rows.row_count,
        SqlQueryResult::Grouped(rows) => rows.row_count,
        SqlQueryResult::Explain { .. } | SqlQueryResult::Ddl { .. } => 1,
        SqlQueryResult::Describe(description) => {
            u32::try_from(description.fields().len()).unwrap_or(u32::MAX)
        }
        SqlQueryResult::ShowIndexes { indexes, .. } => {
            u32::try_from(indexes.len()).unwrap_or(u32::MAX)
        }
        SqlQueryResult::ShowColumns { columns, .. } => {
            u32::try_from(columns.len()).unwrap_or(u32::MAX)
        }
        SqlQueryResult::ShowEntities { entities, .. } => {
            u32::try_from(entities.len()).unwrap_or(u32::MAX)
        }
        SqlQueryResult::ShowStores { stores, .. } => {
            u32::try_from(stores.len()).unwrap_or(u32::MAX)
        }
        SqlQueryResult::ShowMemory { memory } => u32::try_from(memory.len()).unwrap_or(u32::MAX),
    }
}

#[cfg(feature = "sql")]
fn focused_sql_result_signature(result: &SqlQueryResult) -> String {
    match result {
        SqlQueryResult::Count { entity, row_count } => format!("count|{entity}|{row_count}"),
        SqlQueryResult::Projection(rows) => format!(
            "projection|{}|{}|{}",
            rows.entity,
            rows.row_count,
            rows.rendered_rows()
                .into_iter()
                .map(|row| row.join(","))
                .collect::<Vec<_>>()
                .join(";")
        ),
        other => format!("{:?}", other.render_lines()),
    }
}

#[cfg(feature = "sql")]
fn focused_empty_count_row(scenario_key: &str) -> FocusedPkPerfRow {
    let session = match db() {
        Ok(session) => session,
        Err(err) => {
            let err = icydb::Error::from(err);
            return focused_error_row(
                scenario_key,
                "count_exact",
                "Empty",
                "bootstrap_error",
                Some(focused_error_code(&err)),
                "Empty",
                0,
                0,
                "database memory bootstrap failed",
            );
        }
    };
    let query = session
        .load::<PerfAuditUser>()
        .filter_in("id", Vec::<i32>::new());
    match query.count_exact_with_attribution() {
        Ok((count, attribution)) => FocusedPkPerfRow {
            scenario_key: scenario_key.to_string(),
            terminal: "count_exact".to_string(),
            selected_access: "Empty".to_string(),
            admission_result: "admitted".to_string(),
            error_code: None,
            total_instructions: attribution.total_local_instructions,
            planner_instructions: attribution.compile_local_instructions,
            execute_instructions: attribution.execute_local_instructions,
            store_instructions: 0,
            data_store_get: attribution.store_get_calls,
            index_ranges: attribution.index_store_range_scan_calls,
            rows_decoded: attribution.store_get_calls,
            rows_returned: 0,
            result_signature: format!("count_exact|PerfAuditUser|{count}"),
            canonicalization_result: "Empty".to_string(),
            raw_key_count: 0,
            deduplicated_key_count: 0,
            explanation: "measured current empty primary-key count terminal".to_string(),
        },
        Err(err) => focused_error_row(
            scenario_key,
            "count_exact",
            "Empty",
            "rejected",
            Some(focused_error_code(&err)),
            "Empty",
            0,
            0,
            "empty primary-key count terminal failed",
        ),
    }
}

#[cfg(feature = "sql")]
fn focused_empty_require_one_row(scenario_key: &str) -> FocusedPkPerfRow {
    let session = match db() {
        Ok(session) => session,
        Err(err) => {
            let err = icydb::Error::from(err);
            return focused_error_row(
                scenario_key,
                "require_one",
                "Empty",
                "bootstrap_error",
                Some(focused_error_code(&err)),
                "Empty",
                0,
                0,
                "database memory bootstrap failed",
            );
        }
    };
    let query = session
        .load::<PerfAuditUser>()
        .filter_in("id", Vec::<i32>::new());
    let attributed = query.execute_with_attribution();
    let mut row = match attributed {
        Ok((_result, attribution)) => focused_fluent_success_row(
            scenario_key,
            "require_one",
            "Empty",
            "Empty",
            0,
            0,
            "rows|PerfAuditUser|0|".to_string(),
            0,
            &attribution,
            "measured current empty primary-key require_one terminal",
        ),
        Err(err) => focused_error_row(
            scenario_key,
            "require_one",
            "Empty",
            "execution_error",
            Some(focused_error_code(&err)),
            "Empty",
            0,
            0,
            "empty primary-key require_one attribution failed",
        ),
    };
    let terminal_result = session
        .load::<PerfAuditUser>()
        .filter_in("id", Vec::<i32>::new())
        .execute_rows()
        .and_then(|rows| rows.require_one());
    if let Err(err) = terminal_result {
        row.admission_result = "not_found".to_string();
        row.error_code = Some(focused_error_code(&err));
        row.result_signature = "not_found|PerfAuditUser".to_string();
    }
    row
}

#[cfg(feature = "sql")]
fn scale_fixture_facts(
    surface: &str,
    requested_rows: u32,
    actual_rows: usize,
    zero_match_rows: usize,
    one_match_rows: usize,
    quarter_match_rows: usize,
    payload_profile: ScalePayloadProfile,
) -> Result<ScaleFixtureFacts, icydb::Error> {
    let actual_rows = u32::try_from(actual_rows).map_err(|_| query_validate_error())?;
    let zero_match_rows = u32::try_from(zero_match_rows).map_err(|_| query_validate_error())?;
    let one_match_rows = u32::try_from(one_match_rows).map_err(|_| query_validate_error())?;
    let quarter_match_rows =
        u32::try_from(quarter_match_rows).map_err(|_| query_validate_error())?;
    if actual_rows != requested_rows
        || zero_match_rows != 0
        || one_match_rows != 1
        || quarter_match_rows != requested_rows / 4
    {
        return Err(query_validate_error());
    }

    Ok(ScaleFixtureFacts {
        profile_version: SCALE_FIXTURE_PROFILE_VERSION,
        surface: surface.to_string(),
        fixture_rows: actual_rows,
        zero_match_rows,
        one_match_rows,
        quarter_match_rows,
        all_match_rows: actual_rows,
        payload_profile,
    })
}

#[cfg(feature = "sql")]
fn scale_user_mirror_fixture_facts(
    surface: &str,
    requested_rows: u32,
    rows: &[PerfAuditHeapUser],
) -> Result<ScaleFixtureFacts, icydb::Error> {
    scale_fixture_facts(
        surface,
        requested_rows,
        rows.len(),
        rows.iter().filter(|row| row.name.starts_with('A')).count(),
        rows.iter().filter(|row| row.id == 1).count(),
        rows.iter()
            .filter(|row| row.age >= 24 && row.age < 40)
            .count(),
        ScalePayloadProfile::NotApplicable,
    )
}

#[cfg(feature = "sql")]
fn scale_journaled_user_fixture_facts(
    requested_rows: u32,
    rows: &[PerfAuditJournaledUser],
) -> Result<ScaleFixtureFacts, icydb::Error> {
    scale_fixture_facts(
        "journaled_user",
        requested_rows,
        rows.len(),
        rows.iter().filter(|row| row.name.starts_with('A')).count(),
        rows.iter().filter(|row| row.id == 1).count(),
        rows.iter()
            .filter(|row| row.age >= 24 && row.age < 40)
            .count(),
        ScalePayloadProfile::NotApplicable,
    )
}

#[cfg(feature = "sql")]
fn perf_scale_users(row_count: i32) -> Vec<PerfAuditUser> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            let quarter_match = id <= quarter_rows;
            let age = if quarter_match { 31 } else { 43 };
            PerfAuditUser {
                id,
                name: format!("scale-user-{id:04}"),
                age,
                age_nat: if quarter_match { 31 } else { 43 },
                rank: age - 2,
                active: quarter_match,
                created_at: Timestamp::default(),
                updated_at: Timestamp::default(),
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_accounts(row_count: i32) -> Vec<PerfAuditAccount> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            let quarter_match = id <= quarter_rows;
            PerfAuditAccount {
                id,
                handle: format!("scale-account-{id:04}"),
                tier: if quarter_match { "gold" } else { "bronze" }.to_string(),
                active: quarter_match,
                score: 40 + (id % 60),
                created_at: Timestamp::default(),
                updated_at: Timestamp::default(),
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_blobs(row_count: i32) -> Vec<PerfAuditBlob> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            let (thumbnail_len, chunk_len) = match id % 4 {
                0 => (32, 256),
                1 => (64, 512),
                2 => (128, 1_024),
                _ => (256, 2_048),
            };
            // The low byte deliberately repeats a deterministic payload-byte
            // seed without affecting the separately declared length profile.
            PerfAuditBlob {
                id,
                label: format!("scale-payload-{id:04}"),
                bucket: if id <= quarter_rows { 10 } else { 20 },
                thumbnail: perf_blob(id.to_le_bytes()[0], thumbnail_len),
                chunk: perf_blob(id.wrapping_add(31).to_le_bytes()[0], chunk_len),
                created_at: Timestamp::default(),
                updated_at: Timestamp::default(),
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_heap_users(row_count: i32) -> Vec<PerfAuditHeapUser> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            build_perf_audit_heap_user(
                id,
                &format!("scale-heap-user-{id:04}"),
                if id <= quarter_rows { 31 } else { 43 },
            )
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_journaled_users(row_count: i32) -> Vec<PerfAuditJournaledUser> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            build_perf_audit_journaled_user(
                id,
                &format!("scale-journaled-user-{id:04}"),
                if id <= quarter_rows { 31 } else { 43 },
            )
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_scale_tokens(row_count: i32) -> Vec<PerfAuditToken> {
    let quarter_rows = row_count / 4;
    (1..=row_count)
        .map(|id| {
            let quarter_match = id <= quarter_rows;
            let stage = if id % 2 == 0 { "Draft" } else { "Review" };
            perf_audit_token(
                20_000 + u128::from(id.unsigned_abs()),
                if quarter_match {
                    TOKEN_TARGET_COLLECTION
                } else {
                    TOKEN_OTHER_COLLECTION
                },
                stage,
                &format!("scale-token-{id:04}"),
            )
        })
        .collect()
}

/// Build the deterministic user fixture batch used by the perf audit.
fn perf_audit_users() -> Vec<PerfAuditUser> {
    vec![
        PerfAuditUser {
            id: 1,
            name: "Alice".to_string(),
            age: 31,
            age_nat: 31,
            rank: 28,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 2,
            name: "bob".to_string(),
            age: 24,
            age_nat: 24,
            rank: 25,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 3,
            name: "Charlie".to_string(),
            age: 43,
            age_nat: 43,
            rank: 43,
            active: false,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 4,
            name: "amber".to_string(),
            age: 27,
            age_nat: 26,
            rank: 29,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 5,
            name: "Andrew".to_string(),
            age: 31,
            age_nat: 30,
            rank: 30,
            active: true,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditUser {
            id: 6,
            name: "Zelda".to_string(),
            age: 19,
            age_nat: 19,
            rank: 17,
            active: false,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

fn build_perf_audit_heap_user(id: i32, name: &str, age: i32) -> PerfAuditHeapUser {
    PerfAuditHeapUser {
        id,
        name: name.to_string(),
        age,
        created_at: Timestamp::default(),
        updated_at: Timestamp::default(),
    }
}

/// Build a larger deterministic heap fixture window used by the bounded-query
/// instruction regression guard.
fn perf_audit_heap_users() -> Vec<PerfAuditHeapUser> {
    (1..=512)
        .map(|id| build_perf_audit_heap_user(id, &format!("heap-user-{id:04}"), 18 + (id % 47)))
        .collect()
}

fn build_perf_audit_journaled_user(id: i32, name: &str, age: i32) -> PerfAuditJournaledUser {
    PerfAuditJournaledUser {
        id,
        name: name.to_string(),
        age,
        created_at: Timestamp::default(),
        updated_at: Timestamp::default(),
    }
}

/// Build a larger deterministic journaled fixture window used by the
/// bounded-query instruction regression guard.
fn perf_audit_journaled_users() -> Vec<PerfAuditJournaledUser> {
    (1..=512)
        .map(|id| {
            build_perf_audit_journaled_user(id, &format!("journaled-user-{id:04}"), 18 + (id % 47))
        })
        .collect()
}

fn perf_audit_journaled_reentry_probe_users() -> Vec<PerfAuditJournaledUser> {
    (1..=JOURNALED_REENTRY_PROBE_ROWS)
        .map(|id| {
            build_perf_audit_journaled_user(
                id,
                &format!("journaled-reentry-{id:04}"),
                18 + (id % 13),
            )
        })
        .collect()
}

/// Build one deterministic blob payload for perf fixture rows.
fn perf_blob(seed: u8, len: usize) -> Blob {
    Blob::from(
        (0u8..=250)
            .cycle()
            .take(len)
            .map(|offset| seed.wrapping_add(offset))
            .collect::<Vec<_>>(),
    )
}

/// Build the deterministic blob fixture batch used by SQL perf audit queries.
fn perf_audit_blobs() -> Vec<PerfAuditBlob> {
    vec![
        PerfAuditBlob {
            id: 1,
            label: "avatar-a".to_string(),
            bucket: 10,
            thumbnail: perf_blob(11, 1_024),
            chunk: perf_blob(31, 16_384),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 2,
            label: "avatar-b".to_string(),
            bucket: 10,
            thumbnail: perf_blob(12, 2_048),
            chunk: perf_blob(32, 32_768),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 3,
            label: "avatar-c".to_string(),
            bucket: 10,
            thumbnail: perf_blob(13, 4_096),
            chunk: perf_blob(33, 65_536),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 4,
            label: "archive-a".to_string(),
            bucket: 20,
            thumbnail: perf_blob(14, 1_024),
            chunk: perf_blob(34, 16_384),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 5,
            label: "archive-b".to_string(),
            bucket: 20,
            thumbnail: perf_blob(15, 2_048),
            chunk: perf_blob(35, 32_768),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditBlob {
            id: 6,
            label: "archive-c".to_string(),
            bucket: 30,
            thumbnail: perf_blob(16, 4_096),
            chunk: perf_blob(36, 65_536),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

/// Build the deterministic account fixture batch used by the perf audit.
fn perf_audit_accounts() -> Vec<PerfAuditAccount> {
    vec![
        PerfAuditAccount {
            id: 1,
            handle: "Bravo".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 91,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 2,
            handle: "alpha".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 75,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 3,
            handle: "bravo".to_string(),
            tier: "silver".to_string(),
            active: true,
            score: 78,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 4,
            handle: "Delta".to_string(),
            tier: "silver".to_string(),
            active: false,
            score: 66,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 5,
            handle: "brick".to_string(),
            tier: "gold".to_string(),
            active: true,
            score: 88,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        PerfAuditAccount {
            id: 6,
            handle: "azure".to_string(),
            tier: "bronze".to_string(),
            active: true,
            score: 63,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

fn perf_audit_token(id: u128, collection_id: &str, stage: &str, title: &str) -> PerfAuditToken {
    PerfAuditToken {
        id: Ulid::from_bytes(id.to_be_bytes()),
        collection_id: collection_id.to_string(),
        stage: stage.to_string(),
        title: title.to_string(),
        created_at: Timestamp::default(),
        updated_at: Timestamp::default(),
    }
}

/// Build the deterministic token fixture batch used by the branch-set perf
/// audit query.
fn perf_audit_tokens() -> Vec<PerfAuditToken> {
    let mut tokens = vec![
        perf_audit_token(9_090, TOKEN_TARGET_COLLECTION, "Draft", "draft-090"),
        perf_audit_token(9_095, TOKEN_TARGET_COLLECTION, "Review", "review-095"),
        perf_audit_token(9_100, TOKEN_TARGET_COLLECTION, "Review", "review-100"),
        perf_audit_token(9_105, TOKEN_TARGET_COLLECTION, "Draft", "draft-105"),
        perf_audit_token(9_110, TOKEN_TARGET_COLLECTION, "Published", "published-110"),
        perf_audit_token(9_115, TOKEN_OTHER_COLLECTION, "Draft", "other-draft-115"),
        perf_audit_token(9_120, TOKEN_TARGET_COLLECTION, "Draft", "draft-120"),
        perf_audit_token(9_125, TOKEN_TARGET_COLLECTION, "Review", "review-125"),
        perf_audit_token(9_130, TOKEN_TARGET_COLLECTION, "Draft", "draft-130"),
        perf_audit_token(9_135, TOKEN_TARGET_COLLECTION, "Review", "review-135"),
        perf_audit_token(9_140, TOKEN_TARGET_COLLECTION, "Queued", "queued-140"),
        perf_audit_token(9_145, TOKEN_OTHER_COLLECTION, "Review", "other-review-145"),
        perf_audit_token(9_150, TOKEN_TARGET_COLLECTION, "Draft", "draft-150"),
        perf_audit_token(9_155, TOKEN_TARGET_COLLECTION, "Review", "review-155"),
        perf_audit_token(9_160, TOKEN_TARGET_COLLECTION, "Archived", "archived-160"),
        perf_audit_token(9_165, TOKEN_OTHER_COLLECTION, "Draft", "other-draft-165"),
        perf_audit_token(9_170, TOKEN_TARGET_COLLECTION, "Draft", "draft-170"),
        perf_audit_token(9_175, TOKEN_TARGET_COLLECTION, "Review", "review-175"),
        perf_audit_token(9_180, TOKEN_TARGET_COLLECTION, "Rejected", "rejected-180"),
        perf_audit_token(9_185, TOKEN_OTHER_COLLECTION, "Review", "other-review-185"),
    ];

    for offset in 0..240u128 {
        let stage = match offset % 4 {
            0 => "Draft",
            1 => "Queued",
            2 => "Review",
            _ => "Published",
        };
        let title = format!("{}-pressure-{offset:03}", stage.to_ascii_lowercase());
        tokens.push(perf_audit_token(
            10_000 + offset,
            TOKEN_TARGET_COLLECTION,
            stage,
            title.as_str(),
        ));
    }

    tokens
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
