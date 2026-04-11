//!
//! Test-only SQL perf harness for broad sql_parity integration sampling.
//!

use crate::{core_db, db, sql_dispatch};
use candid::{CandidType, Deserialize};
#[cfg(feature = "perf-attribution")]
use icydb::db::{LoweredSqlDispatchExecutorAttribution, SqlProjectionTextExecutorAttribution};
use icydb::{
    Error,
    db::{
        EntityAuthority, ExplainExecutionNodeDescriptor, GroupedCountFoldMetrics, PersistedRow,
        RowCheckMetrics, SqlProjectionMaterializationMetrics, SqlStatementRoute,
        StructuralReadMetrics, identifiers_tail_match,
        query::Predicate,
        response::{
            PagedGroupedResponse, PagedResponse, Response, WriteBatchResponse, WriteResponse,
        },
        sql::SqlQueryResult,
        with_grouped_count_fold_metrics, with_row_check_metrics,
        with_sql_projection_materialization_metrics, with_structural_read_metrics,
    },
    error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
    traits::{EntitySchema, EntityValue},
    value::Value,
};
use icydb_testing_test_sql_parity_fixtures::schema::{
    Customer, CustomerAccount, CustomerOrder, SqlParityCanister, SqlWriteProbe,
};

const MAX_REPEAT_COUNT: u32 = 100;

//
// SqlPerfSurface
//
// One measured SQL surface owned by the sql_parity canister perf harness.
// This stays intentionally narrow so the harness can compare generated SQL
// dispatch against representative typed session surfaces without pretending to
// cover every possible query front in one first pass.
//

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlPerfSurface {
    GeneratedDispatch,
    TypedDispatchCustomer,
    TypedDispatchCustomerOrder,
    TypedDispatchCustomerAccount,
    TypedDispatchSqlWriteProbe,
    TypedQueryFromSqlCustomerExecute,
    TypedExecuteSqlCustomer,
    TypedInsertCustomer,
    TypedInsertManyAtomicCustomer10,
    TypedInsertManyAtomicCustomer100,
    TypedInsertManyAtomicCustomer1000,
    TypedInsertManyNonAtomicCustomer10,
    TypedInsertManyNonAtomicCustomer100,
    TypedInsertManyNonAtomicCustomer1000,
    TypedUpdateCustomer,
    FluentDeleteCustomerByIdLimit1Count,
    FluentDeletePerfCustomerCount,
    TypedExecuteSqlGroupedCustomer,
    TypedExecuteSqlGroupedCustomerSecondPage,
    TypedExecuteSqlAggregateCustomer,
    FluentExplainCustomerExists,
    FluentExplainCustomerMin,
    FluentExplainCustomerLast,
    FluentExplainCustomerSumByAge,
    FluentExplainCustomerAvgDistinctByAge,
    FluentExplainCustomerCountDistinctByAge,
    FluentExplainCustomerLastValueByAge,
    FluentLoadCustomerByIdLimit2,
    FluentLoadCustomerNameEqLimit1,
    FluentPagedCustomerByIdLimit2FirstPage,
    FluentPagedCustomerByIdLimit2SecondPage,
    FluentPagedCustomerByIdLimit2InvalidCursor,
}

//
// SqlPerfRequest
//
// One perf-harness request for one SQL surface and one query shape.
// `repeat_count` runs happen inside one wasm call so the sample can report
// both the first execution cost and the warmed repeated-run range.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfRequest {
    pub surface: SqlPerfSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
    pub repeat_count: u32,
}

//
// SqlPerfOutcome
//
// Compact result summary for one measured SQL surface.
// The audit only needs stable payload shape, cardinality, and failure class
// signals here; full query payload rendering stays outside the perf harness.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfOutcome {
    pub success: bool,
    pub result_kind: String,
    pub entity: Option<String>,
    pub row_count: Option<u32>,
    pub detail_count: Option<u32>,
    pub has_cursor: Option<bool>,
    pub rendered_value: Option<String>,
    pub error_kind: Option<String>,
    pub error_origin: Option<String>,
    pub error_message: Option<String>,
    pub structural_read_metrics: Option<SqlPerfStructuralReadMetrics>,
    pub projection_materialization_metrics: Option<SqlPerfProjectionMaterializationMetrics>,
    pub row_check_metrics: Option<SqlPerfRowCheckMetrics>,
    pub grouped_count_fold_metrics: Option<SqlPerfGroupedCountFoldMetrics>,
}

//
// SqlPerfStructuralReadMetrics
//
// Compact structural-read metrics mirror attached to one perf outcome.
// This keeps the canister perf harness focused on the specific row-open
// validation/materialization counters needed for the current measurement pass.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfStructuralReadMetrics {
    pub rows_opened: u64,
    pub declared_slots_validated: u64,
    pub validated_non_scalar_slots: u64,
    pub materialized_non_scalar_slots: u64,
    pub rows_without_lazy_non_scalar_materializations: u64,
}

impl From<StructuralReadMetrics> for SqlPerfStructuralReadMetrics {
    fn from(metrics: StructuralReadMetrics) -> Self {
        Self {
            rows_opened: metrics.rows_opened,
            declared_slots_validated: metrics.declared_slots_validated,
            validated_non_scalar_slots: metrics.validated_non_scalar_slots,
            materialized_non_scalar_slots: metrics.materialized_non_scalar_slots,
            rows_without_lazy_non_scalar_materializations: metrics
                .rows_without_lazy_non_scalar_materializations,
        }
    }
}

//
// SqlPerfProjectionMaterializationMetrics
//
// Compact row-backed SQL projection metrics mirror attached to one perf
// outcome.
// This keeps the current probe focused on which SQL projection path executed
// and what the `data_rows` fallback actually touched.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfProjectionMaterializationMetrics {
    pub projected_rows_path_hits: u64,
    pub slot_rows_path_hits: u64,
    pub data_rows_path_hits: u64,
    pub data_rows_scalar_fallback_hits: u64,
    pub data_rows_generic_fallback_hits: u64,
    pub data_rows_projected_slot_accesses: u64,
    pub data_rows_non_projected_slot_accesses: u64,
    pub full_row_decode_materializations: u64,
}

impl From<SqlProjectionMaterializationMetrics> for SqlPerfProjectionMaterializationMetrics {
    fn from(metrics: SqlProjectionMaterializationMetrics) -> Self {
        Self {
            projected_rows_path_hits: metrics.projected_rows_path_hits,
            slot_rows_path_hits: metrics.slot_rows_path_hits,
            data_rows_path_hits: metrics.data_rows_path_hits,
            data_rows_scalar_fallback_hits: metrics.data_rows_scalar_fallback_hits,
            data_rows_generic_fallback_hits: metrics.data_rows_generic_fallback_hits,
            data_rows_projected_slot_accesses: metrics.data_rows_projected_slot_accesses,
            data_rows_non_projected_slot_accesses: metrics.data_rows_non_projected_slot_accesses,
            full_row_decode_materializations: metrics.full_row_decode_materializations,
        }
    }
}

//
// SqlPerfRowCheckMetrics
//
// Compact executor-owned row-check metrics mirror attached to one perf
// outcome.
// This keeps the current authority-contract probe focused on secondary scan
// traversal, membership decode, and authoritative row-presence checks.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfRowCheckMetrics {
    pub index_entries_scanned: u64,
    pub index_membership_single_key_entries: u64,
    pub index_membership_multi_key_entries: u64,
    pub index_membership_keys_decoded: u64,
    pub row_check_covering_candidates_seen: u64,
    pub row_check_rows_emitted: u64,
    pub row_presence_probe_count: u64,
    pub row_presence_probe_hits: u64,
    pub row_presence_probe_misses: u64,
    pub row_presence_probe_borrowed_data_store_count: u64,
    pub row_presence_probe_store_handle_count: u64,
    pub row_presence_key_to_raw_encodes: u64,
}

//
// SqlPerfGroupedCountFoldMetrics
//
// Compact dedicated grouped `COUNT(*)` fold metrics mirror attached to one
// perf outcome. This keeps grouped perf triage focused on the executor-owned
// grouped fold path without exposing unrelated runtime internals.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfGroupedCountFoldMetrics {
    pub fold_stage_runs: u64,
    pub rows_folded: u64,
    pub borrowed_probe_rows: u64,
    pub borrowed_hash_computations: u64,
    pub owned_group_fallback_rows: u64,
    pub owned_key_materializations: u64,
    pub bucket_candidate_checks: u64,
    pub existing_group_hits: u64,
    pub new_group_inserts: u64,
    pub finalize_stage_runs: u64,
    pub finalized_group_count: u64,
    pub window_rows_considered: u64,
    pub having_rows_rejected: u64,
    pub resume_boundary_rows_rejected: u64,
    pub candidate_rows_qualified: u64,
    pub bounded_selection_candidates_seen: u64,
    pub bounded_selection_heap_replacements: u64,
    pub bounded_selection_rows_sorted: u64,
    pub unbounded_selection_rows_sorted: u64,
    pub page_rows_skipped_for_offset: u64,
    pub projection_rows_input: u64,
    pub page_rows_emitted: u64,
    pub cursor_construction_attempts: u64,
    pub next_cursor_emitted: u64,
}

impl From<GroupedCountFoldMetrics> for SqlPerfGroupedCountFoldMetrics {
    fn from(metrics: GroupedCountFoldMetrics) -> Self {
        Self {
            fold_stage_runs: metrics.fold_stage_runs,
            rows_folded: metrics.rows_folded,
            borrowed_probe_rows: metrics.borrowed_probe_rows,
            borrowed_hash_computations: metrics.borrowed_hash_computations,
            owned_group_fallback_rows: metrics.owned_group_fallback_rows,
            owned_key_materializations: metrics.owned_key_materializations,
            bucket_candidate_checks: metrics.bucket_candidate_checks,
            existing_group_hits: metrics.existing_group_hits,
            new_group_inserts: metrics.new_group_inserts,
            finalize_stage_runs: metrics.finalize_stage_runs,
            finalized_group_count: metrics.finalized_group_count,
            window_rows_considered: metrics.window_rows_considered,
            having_rows_rejected: metrics.having_rows_rejected,
            resume_boundary_rows_rejected: metrics.resume_boundary_rows_rejected,
            candidate_rows_qualified: metrics.candidate_rows_qualified,
            bounded_selection_candidates_seen: metrics.bounded_selection_candidates_seen,
            bounded_selection_heap_replacements: metrics.bounded_selection_heap_replacements,
            bounded_selection_rows_sorted: metrics.bounded_selection_rows_sorted,
            unbounded_selection_rows_sorted: metrics.unbounded_selection_rows_sorted,
            page_rows_skipped_for_offset: metrics.page_rows_skipped_for_offset,
            projection_rows_input: metrics.projection_rows_input,
            page_rows_emitted: metrics.page_rows_emitted,
            cursor_construction_attempts: metrics.cursor_construction_attempts,
            next_cursor_emitted: metrics.next_cursor_emitted,
        }
    }
}

impl From<RowCheckMetrics> for SqlPerfRowCheckMetrics {
    fn from(metrics: RowCheckMetrics) -> Self {
        Self {
            index_entries_scanned: metrics.index_entries_scanned,
            index_membership_single_key_entries: metrics.index_membership_single_key_entries,
            index_membership_multi_key_entries: metrics.index_membership_multi_key_entries,
            index_membership_keys_decoded: metrics.index_membership_keys_decoded,
            row_check_covering_candidates_seen: metrics.row_check_covering_candidates_seen,
            row_check_rows_emitted: metrics.row_check_rows_emitted,
            row_presence_probe_count: metrics.row_presence_probe_count,
            row_presence_probe_hits: metrics.row_presence_probe_hits,
            row_presence_probe_misses: metrics.row_presence_probe_misses,
            row_presence_probe_borrowed_data_store_count: metrics
                .row_presence_probe_borrowed_data_store_count,
            row_presence_probe_store_handle_count: metrics.row_presence_probe_store_handle_count,
            row_presence_key_to_raw_encodes: metrics.row_presence_key_to_raw_encodes,
        }
    }
}

//
// SqlPerfSample
//
// One repeated wasm-side instruction sample for one SQL surface.
// This reports first/min/max/avg/total local instruction deltas so the audit
// can see cold-vs-warm behavior without relying on host-side zeroed counters.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfSample {
    pub surface: SqlPerfSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
    pub repeat_count: u32,
    pub first_local_instructions: u64,
    pub min_local_instructions: u64,
    pub max_local_instructions: u64,
    pub total_local_instructions: u64,
    pub avg_local_instructions: u64,
    pub outcome_stable: bool,
    pub outcome: SqlPerfOutcome,
}

//
// SqlPerfAttributionSurface
//
// Representative SQL query surfaces used for fixed-cost phase attribution.
// This stays intentionally narrow so attribution can isolate the shared read
// stack for representative scalar and grouped SELECT shapes.
//

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlPerfAttributionSurface {
    GeneratedDispatch,
    TypedDispatchCustomer,
    TypedDispatchCustomerOrder,
    TypedDispatchCustomerAccount,
    TypedGroupedCustomer,
    TypedGroupedCustomerSecondPage,
}

//
// SqlPerfAttributionRequest
//
// One phase-attribution request for one representative SQL surface.
// This measures one single execution and breaks the total into parse, route,
// lower, core-dispatch overhead, executor, and outer-wrapper costs.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfAttributionRequest {
    pub surface: SqlPerfAttributionSurface,
    pub sql: String,
    pub cursor_token: Option<String>,
}

//
// SqlPerfAttributionSample
//
// One fixed-cost SQL query attribution sample measured inside wasm.
// `dispatch_local_instructions` captures the core path between lowering and
// executor entry, while `wrapper_local_instructions` captures the remaining
// surface work above the attributed core path.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfAttributionSample {
    pub surface: SqlPerfAttributionSurface,
    pub sql: String,
    pub parse_local_instructions: u64,
    pub route_local_instructions: u64,
    pub lower_local_instructions: u64,
    pub dispatch_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub executor_breakdown: Option<SqlPerfExecutorAttribution>,
    pub wrapper_local_instructions: u64,
    pub total_local_instructions: u64,
    pub outcome: SqlPerfOutcome,
}

//
// SqlPerfExecutorAttribution
//
// Nested execute-phase attribution for lowered SQL dispatch.
// This keeps the existing top-level execute bucket stable while exposing the
// inner bind/plan/projection phases needed for runtime hot-path triage.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfExecutorAttribution {
    pub bind_local_instructions: u64,
    pub visible_indexes_local_instructions: u64,
    pub build_plan_local_instructions: u64,
    pub projection_labels_local_instructions: u64,
    pub projection_executor: SqlPerfProjectionTextExecutorAttribution,
    pub dispatch_result_local_instructions: u64,
    pub total_local_instructions: u64,
}

//
// SqlPerfProjectionTextExecutorAttribution
//
// Nested projection executor attribution for rendered SQL row execution.
// This separates structural prepare, scalar runtime, projection
// materialization, and final row packaging inside the execute phase.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlPerfProjectionTextExecutorAttribution {
    pub prepare_projection: u64,
    pub scalar_runtime: u64,
    pub materialize_projection: u64,
    pub result_rows: u64,
    pub total: u64,
}

#[cfg(feature = "perf-attribution")]
impl From<SqlProjectionTextExecutorAttribution> for SqlPerfProjectionTextExecutorAttribution {
    fn from(attribution: SqlProjectionTextExecutorAttribution) -> Self {
        Self {
            prepare_projection: attribution.prepare_projection,
            scalar_runtime: attribution.scalar_runtime,
            materialize_projection: attribution.materialize_projection,
            result_rows: attribution.result_rows,
            total: attribution.total,
        }
    }
}

#[cfg(feature = "perf-attribution")]
impl From<LoweredSqlDispatchExecutorAttribution> for SqlPerfExecutorAttribution {
    fn from(attribution: LoweredSqlDispatchExecutorAttribution) -> Self {
        Self {
            bind_local_instructions: attribution.bind_local_instructions,
            visible_indexes_local_instructions: attribution.visible_indexes_local_instructions,
            build_plan_local_instructions: attribution.build_plan_local_instructions,
            projection_labels_local_instructions: attribution.projection_labels_local_instructions,
            projection_executor: attribution.projection_executor.into(),
            dispatch_result_local_instructions: attribution.dispatch_result_local_instructions,
            total_local_instructions: attribution.total_local_instructions,
        }
    }
}

// Measure one SQL surface request inside the running canister.
pub fn sample_sql_surface(request: SqlPerfRequest) -> Result<SqlPerfSample, Error> {
    validate_perf_request(&request)?;

    let sql = normalize_perf_sql_input(request.sql.as_str())?.to_string();
    let repeat_count = request.repeat_count;

    // Measure each execution independently so the sample retains first-run and
    // warmed-run spread instead of only one merged total.
    let mut first_local_instructions = 0_u64;
    let mut min_local_instructions = u64::MAX;
    let mut max_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut last_outcome = None;
    let mut outcome_stable = true;

    for iteration in 0..repeat_count {
        let (delta, outcome) = measure_once(
            request.surface,
            sql.as_str(),
            request.cursor_token.as_deref(),
        );

        if iteration == 0 {
            first_local_instructions = delta;
        } else if last_outcome.as_ref() != Some(&outcome) {
            outcome_stable = false;
        }

        min_local_instructions = min_local_instructions.min(delta);
        max_local_instructions = max_local_instructions.max(delta);
        total_local_instructions = total_local_instructions.saturating_add(delta);
        last_outcome = Some(outcome);
    }

    let avg_local_instructions = total_local_instructions / u64::from(repeat_count);
    let outcome = last_outcome.expect("repeat_count validation guarantees at least one run");

    Ok(SqlPerfSample {
        surface: request.surface,
        sql,
        cursor_token: request.cursor_token,
        repeat_count,
        first_local_instructions,
        min_local_instructions,
        max_local_instructions,
        total_local_instructions,
        avg_local_instructions,
        outcome_stable,
        outcome,
    })
}

// Attribute one representative SQL query surface into fixed-cost wasm phases.
pub fn attribute_sql_surface(
    request: SqlPerfAttributionRequest,
) -> Result<SqlPerfAttributionSample, Error> {
    let sql = normalize_perf_sql_input(request.sql.as_str())?.to_string();

    match request.surface {
        SqlPerfAttributionSurface::GeneratedDispatch => {
            attribute_generated_dispatch_surface(sql.as_str())
        }
        SqlPerfAttributionSurface::TypedDispatchCustomer => {
            attribute_typed_dispatch_surface::<Customer>(
                sql.as_str(),
                SqlPerfAttributionSurface::TypedDispatchCustomer,
            )
        }
        SqlPerfAttributionSurface::TypedDispatchCustomerOrder => {
            attribute_typed_dispatch_surface::<CustomerOrder>(
                sql.as_str(),
                SqlPerfAttributionSurface::TypedDispatchCustomerOrder,
            )
        }
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount => {
            attribute_typed_dispatch_surface::<CustomerAccount>(
                sql.as_str(),
                SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
            )
        }
        SqlPerfAttributionSurface::TypedGroupedCustomer => {
            attribute_typed_grouped_surface(sql.as_str(), request.cursor_token.as_deref())
        }
        SqlPerfAttributionSurface::TypedGroupedCustomerSecondPage => {
            attribute_typed_grouped_second_page_surface(sql.as_str())
        }
    }
}

// Keep perf-harness input validation local so the public `icydb` SQL facade
// does not need to retain generated query-surface adapter helpers.
fn normalize_perf_sql_input(sql: &str) -> Result<&str, Error> {
    let sql_trimmed = sql.trim();
    if sql_trimmed.is_empty() {
        return Err(Error::new(
            ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            ErrorOrigin::Query,
            "query endpoint requires a non-empty SQL string",
        ));
    }

    Ok(sql_trimmed)
}

fn validate_perf_request(request: &SqlPerfRequest) -> Result<(), Error> {
    if request.repeat_count == 0 {
        return Err(invalid_perf_request_error(
            "sql_perf repeat_count must be at least 1",
        ));
    }

    if request.repeat_count > MAX_REPEAT_COUNT {
        return Err(invalid_perf_request_error(format!(
            "sql_perf repeat_count must be <= {MAX_REPEAT_COUNT}"
        )));
    }

    Ok(())
}

fn invalid_perf_request_error(message: impl Into<String>) -> Error {
    Error::new(
        ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        ErrorOrigin::Query,
        message,
    )
}

#[cfg(target_arch = "wasm32")]
fn read_local_instruction_counter() -> u64 {
    canic_cdk::api::performance_counter(1)
}

#[cfg(not(target_arch = "wasm32"))]
const fn read_local_instruction_counter() -> u64 {
    0
}

fn missing_continuation_sample(message: &'static str) -> (u64, SqlPerfOutcome) {
    (0, outcome_from_error(invalid_perf_request_error(message)))
}

fn measure_result<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

const fn attributed_total(
    parse_local_instructions: u64,
    route_local_instructions: u64,
    lower_local_instructions: u64,
    dispatch_local_instructions: u64,
    execute_local_instructions: u64,
    wrapper_local_instructions: u64,
) -> u64 {
    parse_local_instructions
        .saturating_add(route_local_instructions)
        .saturating_add(lower_local_instructions)
        .saturating_add(dispatch_local_instructions)
        .saturating_add(execute_local_instructions)
        .saturating_add(wrapper_local_instructions)
}

fn generated_query_authority(
    route: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<EntityAuthority, Error> {
    if matches!(route, SqlStatementRoute::ShowEntities) {
        return Err(invalid_perf_request_error(
            "sql attribution requires entity-scoped generated SQL route",
        ));
    }

    let sql_entity = route.entity();
    for authority in authorities {
        if identifiers_tail_match(sql_entity, authority.model().name()) {
            return Ok(*authority);
        }
    }

    Err(invalid_perf_request_error(format!(
        "sql attribution unsupported entity '{sql_entity}'"
    )))
}

fn attribute_generated_dispatch_surface(sql: &str) -> Result<SqlPerfAttributionSample, Error> {
    let core = core_db();
    let authorities = sql_dispatch::authorities();

    // Phase 1: attribute the core generated-query path with parse kept
    // separate from authority resolution, lowering, and shared dispatch.
    let (parse_local_instructions, parsed_result) =
        measure_result(|| core.parse_sql_statement(sql).map_err(Error::from));
    let parsed = parsed_result?;

    let (route_local_instructions, authority_result) =
        measure_result(|| generated_query_authority(parsed.route(), authorities));
    let authority = authority_result?;

    let (core_dispatch_total, core_dispatch_result) = measure_result(|| {
        core.execute_generated_query_surface_dispatch_for_authority(&parsed, authority)
            .map_err(Error::from)
    });
    let _core_dispatch_result = core_dispatch_result?;

    let (lower_local_instructions, lowered_result) = measure_result(|| {
        parsed
            .lower_query_lane_for_entity(
                authority.model().name(),
                authority.model().primary_key().name(),
            )
            .map_err(Error::from)
    });
    let lowered = lowered_result?;

    #[cfg(feature = "perf-attribution")]
    let (execute_local_instructions, executor_breakdown) = {
        let execute_breakdown = core
            .attribute_lowered_sql_dispatch_query_for_authority(&lowered, authority)
            .map_err(Error::from)?;

        (
            execute_breakdown.total_local_instructions,
            Some(execute_breakdown.into()),
        )
    };
    #[cfg(not(feature = "perf-attribution"))]
    let (execute_local_instructions, executor_breakdown) = {
        let (execute_local_instructions, execute_result) = measure_result(|| {
            core.execute_lowered_sql_dispatch_query_for_authority(lowered, authority)
                .map_err(Error::from)
        });
        let _execute_result = execute_result?;

        (execute_local_instructions, None)
    };

    let dispatch_local_instructions = core_dispatch_total
        .saturating_sub(lower_local_instructions.saturating_add(execute_local_instructions));

    // Phase 2: measure the real generated surface total and assign the
    // remaining cost to the outer wrapper above the attributed core path.
    let (total_local_instructions, outcome) = measure_surface_call(|| {
        sql_dispatch::query(sql).map_or_else(outcome_from_error, outcome_from_sql_query_result)
    });
    let attributed_core = parse_local_instructions
        .saturating_add(route_local_instructions)
        .saturating_add(core_dispatch_total);
    let wrapper_local_instructions = total_local_instructions.saturating_sub(attributed_core);

    Ok(SqlPerfAttributionSample {
        surface: SqlPerfAttributionSurface::GeneratedDispatch,
        sql: sql.to_string(),
        parse_local_instructions,
        route_local_instructions,
        lower_local_instructions,
        dispatch_local_instructions,
        execute_local_instructions,
        executor_breakdown,
        wrapper_local_instructions,
        total_local_instructions: attributed_total(
            parse_local_instructions,
            route_local_instructions,
            lower_local_instructions,
            dispatch_local_instructions,
            execute_local_instructions,
            wrapper_local_instructions,
        ),
        outcome,
    })
}

// Attribute one typed `execute_sql_dispatch::<E>` surface through the same
// core phases used by the generated lane while keeping the entity binding
// explicit at the typed facade boundary.
fn attribute_typed_dispatch_surface<E>(
    sql: &str,
    surface: SqlPerfAttributionSurface,
) -> Result<SqlPerfAttributionSample, Error>
where
    E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
{
    let core = core_db();
    let authority = EntityAuthority::for_type::<E>();

    // Phase 1: parse once and attribute the core typed dispatch path after
    // parse so the remaining outer facade wrapper can be measured cleanly.
    let (parse_local_instructions, parsed_result) =
        measure_result(|| core.parse_sql_statement(sql).map_err(Error::from));
    let parsed = parsed_result?;

    let (core_dispatch_total, core_dispatch_result) = measure_result(|| {
        core.execute_sql_dispatch_parsed::<E>(&parsed)
            .map_err(Error::from)
    });
    let _core_dispatch_result = core_dispatch_result?;

    let (lower_local_instructions, lowered_result) = measure_result(|| {
        parsed
            .lower_query_lane_for_entity(E::MODEL.name(), E::MODEL.primary_key().name())
            .map_err(Error::from)
    });
    let lowered = lowered_result?;

    #[cfg(feature = "perf-attribution")]
    let (execute_local_instructions, executor_breakdown) = {
        let execute_breakdown = core
            .attribute_lowered_sql_dispatch_query_for_authority(&lowered, authority)
            .map_err(Error::from)?;

        (
            execute_breakdown.total_local_instructions,
            Some(execute_breakdown.into()),
        )
    };
    #[cfg(not(feature = "perf-attribution"))]
    let (execute_local_instructions, executor_breakdown) = {
        let (execute_local_instructions, execute_result) = measure_result(|| {
            core.execute_lowered_sql_dispatch_query_for_authority(lowered, authority)
                .map_err(Error::from)
        });
        let _execute_result = execute_result?;

        (execute_local_instructions, None)
    };

    let dispatch_local_instructions = core_dispatch_total
        .saturating_sub(lower_local_instructions.saturating_add(execute_local_instructions));

    // Phase 2: measure the full facade surface total and assign the remaining
    // cost above the attributed core path to the outer wrapper.
    let (total_local_instructions, outcome) = measure_surface_call(|| {
        db().execute_sql_dispatch::<E>(sql)
            .map_or_else(outcome_from_error, outcome_from_sql_query_result)
    });
    let attributed_core = parse_local_instructions.saturating_add(core_dispatch_total);
    let wrapper_local_instructions = total_local_instructions.saturating_sub(attributed_core);

    Ok(SqlPerfAttributionSample {
        surface,
        sql: sql.to_string(),
        parse_local_instructions,
        route_local_instructions: 0,
        lower_local_instructions,
        dispatch_local_instructions,
        execute_local_instructions,
        executor_breakdown,
        wrapper_local_instructions,
        total_local_instructions: attributed_total(
            parse_local_instructions,
            0,
            lower_local_instructions,
            dispatch_local_instructions,
            execute_local_instructions,
            wrapper_local_instructions,
        ),
        outcome,
    })
}

fn attribute_typed_grouped_surface(
    sql: &str,
    cursor_token: Option<&str>,
) -> Result<SqlPerfAttributionSample, Error> {
    let core = core_db();

    // Phase 1: parse once so grouped attribution can isolate the typed
    // query-intent build from the shared grouped execute path.
    let (parse_local_instructions, parsed_result) =
        measure_result(|| core.parse_sql_statement(sql).map_err(Error::from));
    let parsed = parsed_result?;

    let (core_grouped_total, query_result) =
        measure_result(|| core.query_from_sql::<Customer>(sql).map_err(Error::from));
    let query = query_result?;

    let (lower_local_instructions, lowered_result) = measure_result(|| {
        parsed
            .lower_query_lane_for_entity(
                Customer::MODEL.name(),
                Customer::MODEL.primary_key().name(),
            )
            .map_err(Error::from)
    });
    let _lowered = lowered_result?;

    let dispatch_local_instructions = core_grouped_total
        .saturating_sub(parse_local_instructions.saturating_add(lower_local_instructions));

    // Phase 2: execute the already-built grouped intent so the remaining
    // facade work can be attributed as one outer wrapper cost.
    let (execute_local_instructions, execute_result) = measure_result(|| {
        core.execute_grouped(&query, cursor_token)
            .map_err(Error::from)
    });
    let _execute_result = execute_result?;

    let (total_local_instructions, outcome) = measure_surface_call(|| {
        db().execute_sql_grouped::<Customer>(sql, cursor_token)
            .map_or_else(outcome_from_error, outcome_from_grouped_response)
    });
    let attributed_core = core_grouped_total.saturating_add(execute_local_instructions);
    let wrapper_local_instructions = total_local_instructions.saturating_sub(attributed_core);

    Ok(SqlPerfAttributionSample {
        surface: SqlPerfAttributionSurface::TypedGroupedCustomer,
        sql: sql.to_string(),
        parse_local_instructions,
        route_local_instructions: 0,
        lower_local_instructions,
        dispatch_local_instructions,
        execute_local_instructions,
        executor_breakdown: None,
        wrapper_local_instructions,
        total_local_instructions: attributed_total(
            parse_local_instructions,
            0,
            lower_local_instructions,
            dispatch_local_instructions,
            execute_local_instructions,
            wrapper_local_instructions,
        ),
        outcome,
    })
}

// Bootstrap one grouped query cursor token from the typed grouped facade so
// second-page attribution can isolate resumed grouped execution separately
// from first-page cursor emission.
fn bootstrap_typed_grouped_cursor_token(sql: &str) -> Result<String, Error> {
    let first_page = db().execute_sql_grouped::<Customer>(sql, None)?;

    first_page.next_cursor().map(str::to_string).ok_or_else(|| {
        Error::new(
            ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            ErrorOrigin::Query,
            "grouped second-page attribution requires a continuation cursor",
        )
    })
}

fn attribute_typed_grouped_second_page_surface(
    sql: &str,
) -> Result<SqlPerfAttributionSample, Error> {
    // Phase 1: bootstrap one continuation cursor outside the measured resumed
    // call so attribution stays focused on the second-page grouped request.
    let cursor_token = bootstrap_typed_grouped_cursor_token(sql)?;

    let mut sample = attribute_typed_grouped_surface(sql, Some(cursor_token.as_str()))?;
    sample.surface = SqlPerfAttributionSurface::TypedGroupedCustomerSecondPage;

    Ok(sample)
}

fn measure_typed_grouped_second_page(sql: &str) -> (u64, SqlPerfOutcome) {
    let cursor_token = match bootstrap_typed_grouped_cursor_token(sql) {
        Ok(cursor_token) => cursor_token,
        Err(err) => return (0, outcome_from_error(err)),
    };

    measure_surface_call(|| {
        db().execute_sql_grouped::<Customer>(sql, Some(cursor_token.as_str()))
            .map_or_else(outcome_from_error, outcome_from_grouped_response)
    })
}

fn measure_fluent_paged_second_page() -> (u64, SqlPerfOutcome) {
    let bootstrap = db()
        .load::<Customer>()
        .order_by("id")
        .limit(2)
        .execute_paged();
    let outcome = match bootstrap {
        Ok(first_page) => {
            let Some(cursor_token) = first_page.next_cursor() else {
                return missing_continuation_sample(
                    "fluent paged second-page sample requires a continuation cursor",
                );
            };

            return measure_surface_call(|| {
                db().load::<Customer>()
                    .order_by("id")
                    .limit(2)
                    .cursor(cursor_token.to_string())
                    .execute_paged()
                    .map_or_else(outcome_from_error, outcome_from_paged_response)
            });
        }
        Err(err) => outcome_from_error(err),
    };

    (0, outcome)
}

fn perf_insert_customer() -> Customer {
    Customer {
        name: "perf-insert-customer".to_string(),
        age: 29,
        ..Default::default()
    }
}

fn perf_insert_customer_for_batch(batch_size: u32, offset: u32) -> Customer {
    let age_offset = i32::try_from(offset % 50).expect("offset modulo 50 must fit in i32");

    Customer {
        name: format!("perf-insert-customer-{batch_size}-{offset}"),
        age: 20 + age_offset,
        ..Default::default()
    }
}

fn perf_insert_customer_batch(batch_size: u32) -> Vec<Customer> {
    (0..batch_size)
        .map(|offset| perf_insert_customer_for_batch(batch_size, offset))
        .collect()
}

fn perf_update_customers() -> (Customer, Customer) {
    let base = Customer {
        name: "perf-update-customer-before".to_string(),
        age: 33,
        ..Default::default()
    };
    let inserted = base.clone();
    let updated = Customer {
        name: "perf-update-customer-after".to_string(),
        age: 34,
        ..base
    };

    (inserted, updated)
}

fn perf_delete_customer() -> Customer {
    Customer {
        name: "perf-delete-customer".to_string(),
        age: 35,
        ..Default::default()
    }
}

fn measure_typed_update_customer() -> (u64, SqlPerfOutcome) {
    let (inserted, updated) = perf_update_customers();
    let outcome = match db().insert(inserted) {
        Ok(_) => {
            return measure_surface_call(|| {
                db().update(updated)
                    .map_or_else(outcome_from_error, outcome_from_write_response)
            });
        }
        Err(err) => outcome_from_error(err),
    };

    (0, outcome)
}

fn measure_fluent_delete_perf_customer_count() -> (u64, SqlPerfOutcome) {
    let inserted = perf_delete_customer();
    let outcome = match db().insert(inserted) {
        Ok(_) => {
            return measure_surface_call(|| {
                db().delete::<Customer>()
                    .filter(Predicate::eq(
                        "name".to_string(),
                        "perf-delete-customer".into(),
                    ))
                    .order_by("id")
                    .limit(1)
                    .execute_count_only()
                    .map_or_else(outcome_from_error, outcome_from_delete_count)
            });
        }
        Err(err) => outcome_from_error(err),
    };

    (0, outcome)
}

fn measure_typed_insert_many_atomic_customer(batch_size: u32) -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().insert_many_atomic(perf_insert_customer_batch(batch_size))
            .map_or_else(outcome_from_error, outcome_from_write_batch_response)
    })
}

fn measure_typed_insert_many_non_atomic_customer(batch_size: u32) -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().insert_many_non_atomic(perf_insert_customer_batch(batch_size))
            .map_or_else(outcome_from_error, outcome_from_write_batch_response)
    })
}

// Measure one typed `execute_sql_dispatch::<E>` projection surface while
// keeping the entity binding explicit in the checked-in perf harness.
fn measure_typed_dispatch_surface<E>(sql: &str) -> (u64, SqlPerfOutcome)
where
    E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
{
    measure_surface_call(|| {
        db().execute_sql_dispatch::<E>(sql)
            .map_or_else(outcome_from_error, outcome_from_sql_query_result)
    })
}

// Keep fluent aggregate explain perf probes on one canonical Customer load
// window so the checked-in harness measures the new public explain surfaces
// without inventing extra fixture-only query shapes.
fn outcome_from_explain_execution_descriptor(
    entity: &str,
    descriptor: ExplainExecutionNodeDescriptor,
) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "explain".to_string(),
        entity: Some(entity.to_string()),
        row_count: None,
        detail_count: Some(checked_perf_count(
            descriptor.render_text_tree().lines().count(),
            "fluent explain line count",
        )),
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

// Route the new public fluent aggregate explain probes through the same
// stable Customer load window so PocketIC and canister perf runs measure the
// surface cost rather than fixture setup differences.
fn measure_fluent_customer_exists_explain() -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().load::<Customer>()
            .order_by("id")
            .explain_exists()
            .map_or_else(outcome_from_error, |plan| {
                outcome_from_explain_execution_descriptor(
                    Customer::MODEL.name(),
                    plan.execution_node_descriptor(),
                )
            })
    })
}

fn measure_fluent_customer_min_explain() -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().load::<Customer>()
            .order_by("id")
            .explain_min()
            .map_or_else(outcome_from_error, |plan| {
                outcome_from_explain_execution_descriptor(
                    Customer::MODEL.name(),
                    plan.execution_node_descriptor(),
                )
            })
    })
}

fn measure_fluent_customer_last_explain() -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().load::<Customer>()
            .order_by("id")
            .explain_last()
            .map_or_else(outcome_from_error, |plan| {
                outcome_from_explain_execution_descriptor(
                    Customer::MODEL.name(),
                    plan.execution_node_descriptor(),
                )
            })
    })
}

fn measure_fluent_customer_sum_by_age_explain() -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().load::<Customer>()
            .order_by("id")
            .explain_sum_by("age")
            .map_or_else(outcome_from_error, |plan| {
                outcome_from_explain_execution_descriptor(
                    Customer::MODEL.name(),
                    plan.execution_node_descriptor(),
                )
            })
    })
}

fn measure_fluent_customer_avg_distinct_by_age_explain() -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().load::<Customer>()
            .order_by("id")
            .explain_avg_distinct_by("age")
            .map_or_else(outcome_from_error, |plan| {
                outcome_from_explain_execution_descriptor(
                    Customer::MODEL.name(),
                    plan.execution_node_descriptor(),
                )
            })
    })
}

fn measure_fluent_customer_count_distinct_by_age_explain() -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().load::<Customer>()
            .order_by("id")
            .explain_count_distinct_by("age")
            .map_or_else(outcome_from_error, |descriptor| {
                outcome_from_explain_execution_descriptor(Customer::MODEL.name(), descriptor)
            })
    })
}

fn measure_fluent_customer_last_value_by_age_explain() -> (u64, SqlPerfOutcome) {
    measure_surface_call(|| {
        db().load::<Customer>()
            .order_by("id")
            .explain_last_value_by("age")
            .map_or_else(outcome_from_error, |descriptor| {
                outcome_from_explain_execution_descriptor(Customer::MODEL.name(), descriptor)
            })
    })
}

// Keep the perf harness surface table exhaustive in one match so new public
// SQL and fluent probe variants stay auditable from one dispatch boundary.
#[expect(clippy::too_many_lines)]
fn measure_once(
    surface: SqlPerfSurface,
    sql: &str,
    cursor_token: Option<&str>,
) -> (u64, SqlPerfOutcome) {
    match surface {
        SqlPerfSurface::GeneratedDispatch => measure_surface_call(|| {
            sql_dispatch::query(sql).map_or_else(outcome_from_error, outcome_from_sql_query_result)
        }),
        SqlPerfSurface::TypedDispatchCustomer => measure_typed_dispatch_surface::<Customer>(sql),
        SqlPerfSurface::TypedDispatchCustomerOrder => {
            measure_typed_dispatch_surface::<CustomerOrder>(sql)
        }
        SqlPerfSurface::TypedDispatchCustomerAccount => {
            measure_typed_dispatch_surface::<CustomerAccount>(sql)
        }
        SqlPerfSurface::TypedDispatchSqlWriteProbe => {
            measure_typed_dispatch_surface::<SqlWriteProbe>(sql)
        }
        SqlPerfSurface::TypedQueryFromSqlCustomerExecute => measure_surface_call(|| {
            let session = db();
            session
                .query_from_sql::<Customer>(sql)
                .and_then(|query| session.execute_query(&query))
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::TypedExecuteSqlCustomer => measure_surface_call(|| {
            db().execute_sql::<Customer>(sql)
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::TypedInsertCustomer => measure_surface_call(|| {
            db().insert(perf_insert_customer())
                .map_or_else(outcome_from_error, outcome_from_write_response)
        }),
        SqlPerfSurface::TypedInsertManyAtomicCustomer10 => {
            measure_typed_insert_many_atomic_customer(10)
        }
        SqlPerfSurface::TypedInsertManyAtomicCustomer100 => {
            measure_typed_insert_many_atomic_customer(100)
        }
        SqlPerfSurface::TypedInsertManyAtomicCustomer1000 => {
            measure_typed_insert_many_atomic_customer(1000)
        }
        SqlPerfSurface::TypedInsertManyNonAtomicCustomer10 => {
            measure_typed_insert_many_non_atomic_customer(10)
        }
        SqlPerfSurface::TypedInsertManyNonAtomicCustomer100 => {
            measure_typed_insert_many_non_atomic_customer(100)
        }
        SqlPerfSurface::TypedInsertManyNonAtomicCustomer1000 => {
            measure_typed_insert_many_non_atomic_customer(1000)
        }
        SqlPerfSurface::TypedUpdateCustomer => measure_typed_update_customer(),
        SqlPerfSurface::FluentDeleteCustomerByIdLimit1Count => measure_surface_call(|| {
            db().delete::<Customer>()
                .order_by("id")
                .limit(1)
                .execute_count_only()
                .map_or_else(outcome_from_error, outcome_from_delete_count)
        }),
        SqlPerfSurface::FluentDeletePerfCustomerCount => {
            measure_fluent_delete_perf_customer_count()
        }
        SqlPerfSurface::TypedExecuteSqlGroupedCustomer => measure_grouped_surface_call(|| {
            db().execute_sql_grouped::<Customer>(sql, cursor_token)
                .map_or_else(outcome_from_error, outcome_from_grouped_response)
        }),
        SqlPerfSurface::TypedExecuteSqlGroupedCustomerSecondPage => {
            measure_typed_grouped_second_page(sql)
        }
        SqlPerfSurface::TypedExecuteSqlAggregateCustomer => measure_surface_call(|| {
            db().execute_sql_aggregate::<Customer>(sql)
                .map_or_else(outcome_from_error, outcome_from_value)
        }),
        SqlPerfSurface::FluentExplainCustomerExists => measure_fluent_customer_exists_explain(),
        SqlPerfSurface::FluentExplainCustomerMin => measure_fluent_customer_min_explain(),
        SqlPerfSurface::FluentExplainCustomerLast => measure_fluent_customer_last_explain(),
        SqlPerfSurface::FluentExplainCustomerSumByAge => {
            measure_fluent_customer_sum_by_age_explain()
        }
        SqlPerfSurface::FluentExplainCustomerAvgDistinctByAge => {
            measure_fluent_customer_avg_distinct_by_age_explain()
        }
        SqlPerfSurface::FluentExplainCustomerCountDistinctByAge => {
            measure_fluent_customer_count_distinct_by_age_explain()
        }
        SqlPerfSurface::FluentExplainCustomerLastValueByAge => {
            measure_fluent_customer_last_value_by_age_explain()
        }
        SqlPerfSurface::FluentLoadCustomerByIdLimit2 => measure_surface_call(|| {
            db().load::<Customer>()
                .order_by("id")
                .limit(2)
                .execute()
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::FluentLoadCustomerNameEqLimit1 => measure_surface_call(|| {
            db().load::<Customer>()
                .filter(Predicate::eq("name".to_string(), "alice".into()))
                .order_by("id")
                .limit(1)
                .execute()
                .map_or_else(outcome_from_error, outcome_from_response)
        }),
        SqlPerfSurface::FluentPagedCustomerByIdLimit2FirstPage => measure_surface_call(|| {
            db().load::<Customer>()
                .order_by("id")
                .limit(2)
                .execute_paged()
                .map_or_else(outcome_from_error, outcome_from_paged_response)
        }),
        SqlPerfSurface::FluentPagedCustomerByIdLimit2SecondPage => {
            measure_fluent_paged_second_page()
        }
        SqlPerfSurface::FluentPagedCustomerByIdLimit2InvalidCursor => measure_surface_call(|| {
            db().load::<Customer>()
                .order_by("id")
                .limit(2)
                .cursor(cursor_token.unwrap_or("zz").to_string())
                .execute_paged()
                .map_or_else(outcome_from_error, outcome_from_paged_response)
        }),
    }
}

fn measure_surface_call(run: impl FnOnce() -> SqlPerfOutcome) -> (u64, SqlPerfOutcome) {
    let start = read_local_instruction_counter();
    let (((outcome, structural_metrics), projection_metrics), row_check_metrics) =
        with_row_check_metrics(|| {
            with_sql_projection_materialization_metrics(|| with_structural_read_metrics(run))
        });
    let delta = read_local_instruction_counter().saturating_sub(start);

    let outcome = attach_structural_read_metrics(outcome, structural_metrics);
    let outcome = attach_projection_materialization_metrics(outcome, projection_metrics);
    let outcome = attach_row_check_metrics(outcome, row_check_metrics);

    (delta, outcome)
}

fn measure_grouped_surface_call(run: impl FnOnce() -> SqlPerfOutcome) -> (u64, SqlPerfOutcome) {
    let start = read_local_instruction_counter();
    let ((((outcome, structural_metrics), projection_metrics), row_check_metrics), grouped_metrics) =
        with_grouped_count_fold_metrics(|| {
            with_row_check_metrics(|| {
                with_sql_projection_materialization_metrics(|| with_structural_read_metrics(run))
            })
        });
    let delta = read_local_instruction_counter().saturating_sub(start);

    let outcome = attach_structural_read_metrics(outcome, structural_metrics);
    let outcome = attach_projection_materialization_metrics(outcome, projection_metrics);
    let outcome = attach_row_check_metrics(outcome, row_check_metrics);
    let outcome = attach_grouped_count_fold_metrics(outcome, grouped_metrics);

    (delta, outcome)
}

fn attach_structural_read_metrics(
    mut outcome: SqlPerfOutcome,
    metrics: StructuralReadMetrics,
) -> SqlPerfOutcome {
    outcome.structural_read_metrics = Some(metrics.into());

    outcome
}

fn attach_projection_materialization_metrics(
    mut outcome: SqlPerfOutcome,
    metrics: SqlProjectionMaterializationMetrics,
) -> SqlPerfOutcome {
    outcome.projection_materialization_metrics = Some(metrics.into());

    outcome
}

fn attach_row_check_metrics(
    mut outcome: SqlPerfOutcome,
    metrics: RowCheckMetrics,
) -> SqlPerfOutcome {
    outcome.row_check_metrics = Some(metrics.into());

    outcome
}

fn attach_grouped_count_fold_metrics(
    mut outcome: SqlPerfOutcome,
    metrics: GroupedCountFoldMetrics,
) -> SqlPerfOutcome {
    // Keep grouped fold observability absent on non-grouped surfaces so the
    // perf harness does not pay a fixed wire-shaping cost for a metrics object
    // that carries only zeroes.
    if metrics == GroupedCountFoldMetrics::default() {
        return outcome;
    }

    outcome.grouped_count_fold_metrics = Some(metrics.into());

    outcome
}

// Keep perf outcome counters on the stable `u32` wire type without silently
// truncating host-side `usize` lengths if a future harness shape grows.
fn checked_perf_count(count: usize, label: &str) -> u32 {
    u32::try_from(count).unwrap_or_else(|_| panic!("perf harness {label} exceeds u32"))
}

// Keep SQL query-result shaping concentrated in one helper so the perf
// harness does not scatter stable wire-shape decisions across many callers.
#[allow(clippy::too_many_lines)]
fn outcome_from_sql_query_result(result: SqlQueryResult) -> SqlPerfOutcome {
    match result {
        SqlQueryResult::Projection(rows) => SqlPerfOutcome {
            success: true,
            result_kind: "projection".to_string(),
            entity: Some(rows.entity),
            row_count: Some(rows.row_count),
            detail_count: Some(checked_perf_count(
                rows.columns.len(),
                "projection column count",
            )),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
            structural_read_metrics: None,
            projection_materialization_metrics: None,
            row_check_metrics: None,
            grouped_count_fold_metrics: None,
        },
        SqlQueryResult::Grouped(rows) => SqlPerfOutcome {
            success: true,
            result_kind: "grouped".to_string(),
            entity: Some(rows.entity),
            row_count: Some(rows.row_count),
            detail_count: Some(checked_perf_count(
                rows.columns.len(),
                "grouped column count",
            )),
            has_cursor: Some(rows.next_cursor.is_some()),
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
            structural_read_metrics: None,
            projection_materialization_metrics: None,
            row_check_metrics: None,
            grouped_count_fold_metrics: None,
        },
        SqlQueryResult::Explain { entity, explain } => SqlPerfOutcome {
            success: true,
            result_kind: "explain".to_string(),
            entity: Some(entity),
            row_count: None,
            detail_count: Some(checked_perf_count(
                explain.lines().count(),
                "explain line count",
            )),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
            structural_read_metrics: None,
            projection_materialization_metrics: None,
            row_check_metrics: None,
            grouped_count_fold_metrics: None,
        },
        SqlQueryResult::Describe(description) => SqlPerfOutcome {
            success: true,
            result_kind: "describe".to_string(),
            entity: Some(description.entity_name().to_string()),
            row_count: None,
            detail_count: Some(checked_perf_count(
                description.fields().len(),
                "describe field count",
            )),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
            structural_read_metrics: None,
            projection_materialization_metrics: None,
            row_check_metrics: None,
            grouped_count_fold_metrics: None,
        },
        SqlQueryResult::ShowIndexes { entity, indexes } => SqlPerfOutcome {
            success: true,
            result_kind: "show_indexes".to_string(),
            entity: Some(entity),
            row_count: None,
            detail_count: Some(checked_perf_count(indexes.len(), "show indexes count")),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
            structural_read_metrics: None,
            projection_materialization_metrics: None,
            row_check_metrics: None,
            grouped_count_fold_metrics: None,
        },
        SqlQueryResult::ShowColumns { entity, columns } => SqlPerfOutcome {
            success: true,
            result_kind: "show_columns".to_string(),
            entity: Some(entity),
            row_count: None,
            detail_count: Some(checked_perf_count(columns.len(), "show columns count")),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
            structural_read_metrics: None,
            projection_materialization_metrics: None,
            row_check_metrics: None,
            grouped_count_fold_metrics: None,
        },
        SqlQueryResult::ShowEntities { entities } => SqlPerfOutcome {
            success: true,
            result_kind: "show_entities".to_string(),
            entity: None,
            row_count: None,
            detail_count: Some(checked_perf_count(entities.len(), "show entities count")),
            has_cursor: None,
            rendered_value: None,
            error_kind: None,
            error_origin: None,
            error_message: None,
            structural_read_metrics: None,
            projection_materialization_metrics: None,
            row_check_metrics: None,
            grouped_count_fold_metrics: None,
        },
    }
}

fn outcome_from_response(result: Response<Customer>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "typed_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(result.count()),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

fn outcome_from_paged_response(result: PagedResponse<Customer>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "paged_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(checked_perf_count(
            result.items().len(),
            "paged response row count",
        )),
        detail_count: None,
        has_cursor: Some(result.next_cursor().is_some()),
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

fn outcome_from_grouped_response(result: PagedGroupedResponse) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "grouped_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(checked_perf_count(
            result.items().len(),
            "grouped response row count",
        )),
        detail_count: None,
        has_cursor: Some(result.next_cursor().is_some()),
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

fn outcome_from_value(result: Value) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "aggregate_value".to_string(),
        entity: Some("Customer".to_string()),
        row_count: None,
        detail_count: None,
        has_cursor: None,
        rendered_value: Some(format!("{result:?}")),
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

fn outcome_from_write_response(result: WriteResponse<Customer>) -> SqlPerfOutcome {
    let _ = result.id();

    SqlPerfOutcome {
        success: true,
        result_kind: "write_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(1),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

fn outcome_from_write_batch_response(result: WriteBatchResponse<Customer>) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "write_batch_response".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(checked_perf_count(result.len(), "write batch row count")),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

fn outcome_from_delete_count(row_count: u32) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: true,
        result_kind: "delete_count".to_string(),
        entity: Some("Customer".to_string()),
        row_count: Some(row_count),
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: None,
        error_origin: None,
        error_message: None,
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}

fn outcome_from_error(err: Error) -> SqlPerfOutcome {
    SqlPerfOutcome {
        success: false,
        result_kind: "error".to_string(),
        entity: None,
        row_count: None,
        detail_count: None,
        has_cursor: None,
        rendered_value: None,
        error_kind: Some(format!("{:?}", err.kind())),
        error_origin: Some(format!("{:?}", err.origin())),
        error_message: Some(err.message().to_string()),
        structural_read_metrics: None,
        projection_materialization_metrics: None,
        row_check_metrics: None,
        grouped_count_fold_metrics: None,
    }
}
