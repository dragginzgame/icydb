//!
//! Dedicated SQL perf-audit canister used only for instruction-sampling and
//! access-shape coverage.
//!

#[cfg(feature = "sql")]
use candid::{CandidType, Deserialize};
#[cfg(feature = "sql")]
use ic_cdk::query;
#[cfg(feature = "sql")]
use ic_cdk::update;
#[cfg(feature = "sql")]
use icydb::types::{Blob, Timestamp, Ulid};
#[cfg(feature = "sql")]
use icydb::{
    ErrorCode, ErrorOrigin,
    db::{
        DynamicQuery, EntitySchemaDescription, ExhaustiveQueryPageOutput, ExhaustiveReadError,
        GroupedCountAttribution, GroupedExecutionAttribution, IntegrityCheckError,
        IntegrityCheckResult, IntegrityJobOwner, LiveQueryPageOutput, ReadSetRevisionError,
        ReadSetRevisionProof, SqlCompileAttribution, SqlExecutionAttribution, SqlIntegrityError,
        SqlPureCoveringAttribution, SqlQueryCacheAttribution, SqlQueryExecutionAttribution,
        SqlStructuralWorkAttribution, StructuralMutation, StructuralPatch, WriteCell,
        query::{FieldRef, asc},
        sql::SqlQueryResult,
    },
    value::InputValue,
};
#[cfg(feature = "sql")]
use icydb_testing_audit_sql_perf_fixtures::sql_perf::{
    PerfAuditAccount, PerfAuditBlob, PerfAuditHeapUser, PerfAuditJournaledUser,
    PerfAuditRelationSource, PerfAuditRelationTarget, PerfAuditStreamingCompoundRow,
    PerfAuditStreamingRow, PerfAuditToken, PerfAuditUser,
};

icydb::start!();

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_reset;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_load(handler = load_perf_fixtures);
}

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
struct ReadTotalOnlyPerfResult {
    row_count: u32,
    instructions: u64,
}

/// Exact schema-application work observed inside one IC message.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(all(feature = "sql", feature = "test-admin-api"))]
struct SchemaApplicationPerfResult {
    local_instructions: u64,
    reconcile_checks: u64,
    first_create: u64,
    exact_match: u64,
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

/// Exact deterministic source facts for the 0.222 streaming executor fixture.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct StreamingExecutionFixtureFacts {
    profile_version: u32,
    seed: u64,
    fixture_rows: u32,
    lane_a_zero_rows: u32,
    lane_b_zero_rows: u32,
    sparse_overlap_rows: u32,
    empty_overlap_rows: u32,
    group_count: u32,
    wide_payload_bytes: Vec<u32>,
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
struct ConstraintActivationPerfResult {
    no_check: StorageWritePerfResult,
    add_check_local_instructions: u64,
    add_check_rows_scanned: u64,
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct SqlWriteMaterializationPerfResult {
    local_instructions: [u64; 4],
    rows: [u32; 4],
}

/// Focused trusted resumable-update instruction and progress evidence.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct ResumableUpdatePerfResult {
    prepare_local_instructions: u64,
    forward_local_instructions: Vec<u64>,
    verify_local_instructions: Vec<u64>,
    forward_keys_scanned: u32,
    verify_keys_scanned: u32,
    rows_updated: u32,
}

/// One public integrity result plus its canister-local execution cost.
#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
struct IntegritySqlPerfResult {
    result: IntegrityCheckResult,
    local_instructions: u64,
}

#[cfg(feature = "sql")]
const STORAGE_WRITE_MATRIX_RUNS: u32 = 10;
#[cfg(feature = "sql")]
const SQL_WRITE_MATERIALIZATION_ROWS: i32 = 32;
#[cfg(feature = "sql")]
const INTEGRITY_JOURNAL_TAIL_BATCHES: i32 = 6;
#[cfg(feature = "sql")]
const JOURNALED_REENTRY_PROBE_ROWS: i32 = 32;
#[cfg(feature = "sql")]
const TOKEN_TARGET_COLLECTION: &str = "01KV5N439P0000000000000000";
#[cfg(feature = "sql")]
const TOKEN_OTHER_COLLECTION: &str = "01KV5N439P1111111111111111";
#[cfg(feature = "sql")]
const SCALE_FIXTURE_PROFILE_VERSION: u32 = 1;
#[cfg(feature = "sql")]
const SCALE_FIXTURE_ROW_CARDINALITIES: &[u32] = &[16, 256, 2_048];
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_PROFILE_VERSION: u32 = 1;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_SEED: u64 = 3;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_SEED_I32: i32 = 3;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_FIXTURE_ROWS: i32 = 2_048;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_CONTINUATION_ROWS: i32 = 10_001;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_CONTINUATION_LOAD_BATCH_ROWS: i32 = 4_096;
#[cfg(feature = "sql")]
const STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES: &[usize] = &[300 * 1_024, 150 * 1_024, 40 * 1_024];

#[derive(CandidType, Debug, Deserialize)]
#[cfg(feature = "sql")]
enum StreamingExhaustivePageError {
    Database(icydb::Error),
    Revision(ReadSetRevisionError),
}

#[cfg(feature = "sql")]
impl From<ExhaustiveReadError> for StreamingExhaustivePageError {
    fn from(error: ExhaustiveReadError) -> Self {
        match error {
            ExhaustiveReadError::Database(error) => Self::Database(error),
            ExhaustiveReadError::Revision(error) => Self::Revision(error),
        }
    }
}

#[cfg(feature = "sql")]
trait StructuralFixtureRow {
    const ENTITY: &'static str;

    fn into_structural_patch(self) -> StructuralPatch;
}

#[cfg(feature = "sql")]
trait StorageWriteFixtureRow: StructuralFixtureRow {
    fn primary_key_input(&self) -> InputValue;
}

#[cfg(feature = "sql")]
fn authored(value: impl Into<InputValue>) -> WriteCell<InputValue> {
    WriteCell::Value(value.into())
}

#[cfg(feature = "sql")]
fn insert_fixture_rows<R>(rows: Vec<R>) -> Result<(), icydb::Error>
where
    R: StructuralFixtureRow,
{
    if rows.is_empty() {
        return Ok(());
    }
    let expected = u32::try_from(rows.len()).map_err(|_| query_validate_error())?;
    let patches = rows
        .into_iter()
        .map(StructuralFixtureRow::into_structural_patch)
        .collect();
    let result = db()?.execute_trusted_structural_insert_batch(R::ENTITY, patches)?;
    if result.affected_rows != expected {
        return Err(query_validate_error());
    }
    Ok(())
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditUser {
    const ENTITY: &'static str = "PerfAuditUser";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("name", authored(self.name))
            .field("age", authored(self.age))
            .field("age_nat", authored(self.age_nat))
            .field("rank", authored(self.rank))
            .field("active", authored(self.active))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditHeapUser {
    const ENTITY: &'static str = "PerfAuditHeapUser";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("name", authored(self.name))
            .field("age", authored(self.age))
    }
}

#[cfg(feature = "sql")]
impl StorageWriteFixtureRow for PerfAuditHeapUser {
    fn primary_key_input(&self) -> InputValue {
        self.id.into()
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditJournaledUser {
    const ENTITY: &'static str = "PerfAuditJournaledUser";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("name", authored(self.name))
            .field("age", authored(self.age))
    }
}

#[cfg(feature = "sql")]
impl StorageWriteFixtureRow for PerfAuditJournaledUser {
    fn primary_key_input(&self) -> InputValue {
        self.id.into()
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditRelationTarget {
    const ENTITY: &'static str = "PerfAuditRelationTarget";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new().field("id", authored(self.id))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditRelationSource {
    const ENTITY: &'static str = "PerfAuditRelationSource";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("target_id", authored(self.target_id))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditBlob {
    const ENTITY: &'static str = "PerfAuditBlob";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("label", authored(self.label))
            .field("bucket", authored(self.bucket))
            .field("thumbnail", authored(self.thumbnail))
            .field("chunk", authored(self.chunk))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditAccount {
    const ENTITY: &'static str = "PerfAuditAccount";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("handle", authored(self.handle))
            .field("tier", authored(self.tier))
            .field("active", authored(self.active))
            .field("score", authored(self.score))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditToken {
    const ENTITY: &'static str = "PerfAuditToken";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("collection_id", authored(self.collection_id))
            .field("stage", authored(self.stage))
            .field("title", authored(self.title))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditStreamingRow {
    const ENTITY: &'static str = "PerfAuditStreamingRow";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("lane_a", authored(self.lane_a))
            .field("lane_b", authored(self.lane_b))
            .field("group_key", authored(self.group_key))
            .field("sort_key", authored(self.sort_key))
            .field("label", authored(self.label))
            .field("payload", authored(self.payload))
    }
}

#[cfg(feature = "sql")]
impl StructuralFixtureRow for PerfAuditStreamingCompoundRow {
    const ENTITY: &'static str = "PerfAuditStreamingCompoundRow";

    fn into_structural_patch(self) -> StructuralPatch {
        StructuralPatch::new()
            .field("id", authored(self.id))
            .field("lane_a", authored(self.lane_a))
            .field("lane_b", authored(self.lane_b))
            .field("group_key", authored(self.group_key))
            .field("sort_key", authored(self.sort_key))
            .field("label", authored(self.label))
            .field("payload", authored(self.payload))
    }
}

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

///
/// GroupedRuntimeTotals
///
/// Accumulates executor-owned grouped runtime facts across repeated perf runs.
/// Average work counters and maximum live-state peaks are projected into the
/// final sample without making the audit canister a second runtime authority.
///

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupedRuntimeTotals {
    rows_scanned: u64,
    groups_observed: u64,
    groups_finalized: u64,
    max_peak_live_groups: u64,
    max_peak_live_aggregate_states: u64,
    max_peak_live_distinct_values: u64,
    early_scan_stop_runs: u64,
}

#[cfg(feature = "sql")]
impl GroupedRuntimeTotals {
    fn record(&mut self, grouped: GroupedExecutionAttribution) {
        self.rows_scanned = self.rows_scanned.saturating_add(grouped.rows_scanned);
        self.groups_observed = self.groups_observed.saturating_add(grouped.groups_observed);
        self.groups_finalized = self
            .groups_finalized
            .saturating_add(grouped.groups_finalized);
        self.max_peak_live_groups = self.max_peak_live_groups.max(grouped.peak_live_groups);
        self.max_peak_live_aggregate_states = self
            .max_peak_live_aggregate_states
            .max(grouped.peak_live_aggregate_states);
        self.max_peak_live_distinct_values = self
            .max_peak_live_distinct_values
            .max(grouped.peak_live_distinct_values);
        self.early_scan_stop_runs = self
            .early_scan_stop_runs
            .saturating_add(u64::from(grouped.early_scan_stop));
    }

    const fn apply_average(
        self,
        attribution: &mut GroupedExecutionAttribution,
        repeated_run_count: u64,
    ) {
        attribution.rows_scanned = self.rows_scanned / repeated_run_count;
        attribution.groups_observed = self.groups_observed / repeated_run_count;
        attribution.groups_finalized = self.groups_finalized / repeated_run_count;
        attribution.peak_live_groups = self.max_peak_live_groups;
        attribution.peak_live_aggregate_states = self.max_peak_live_aggregate_states;
        attribution.peak_live_distinct_values = self.max_peak_live_distinct_values;
        attribution.early_scan_stop = self.early_scan_stop_runs == repeated_run_count;
    }
}

#[cfg(feature = "sql")]
const fn record_structural_work(
    total: &mut SqlStructuralWorkAttribution,
    current: SqlStructuralWorkAttribution,
) {
    total.range_conjunctions_examined = total
        .range_conjunctions_examined
        .saturating_add(current.range_conjunctions_examined);
    total.range_lower_bounds_extracted = total
        .range_lower_bounds_extracted
        .saturating_add(current.range_lower_bounds_extracted);
    total.range_upper_bounds_extracted = total
        .range_upper_bounds_extracted
        .saturating_add(current.range_upper_bounds_extracted);
    total.range_physical_children_emitted = total
        .range_physical_children_emitted
        .saturating_add(current.range_physical_children_emitted);
    total.residual_predicate_evaluations = total
        .residual_predicate_evaluations
        .saturating_add(current.residual_predicate_evaluations);
    total.membership_authored_members = total
        .membership_authored_members
        .saturating_add(current.membership_authored_members);
    total.membership_normalized_members = total
        .membership_normalized_members
        .saturating_add(current.membership_normalized_members);
    total.membership_distinct_members = total
        .membership_distinct_members
        .saturating_add(current.membership_distinct_members);
    total.membership_null_members = total
        .membership_null_members
        .saturating_add(current.membership_null_members);
    total.membership_canonicalization_passes = total
        .membership_canonicalization_passes
        .saturating_add(current.membership_canonicalization_passes);
    total.membership_members_revisited = total
        .membership_members_revisited
        .saturating_add(current.membership_members_revisited);
    total.prefix_branches_before_deduplication = total
        .prefix_branches_before_deduplication
        .saturating_add(current.prefix_branches_before_deduplication);
    total.prefix_branches_after_deduplication = total
        .prefix_branches_after_deduplication
        .saturating_add(current.prefix_branches_after_deduplication);
    total.prefix_exclusions_tested = total
        .prefix_exclusions_tested
        .saturating_add(current.prefix_exclusions_tested);
    total.prefix_exclusions_pruned = total
        .prefix_exclusions_pruned
        .saturating_add(current.prefix_exclusions_pruned);
    total.prefix_branch_cap_admissions = total
        .prefix_branch_cap_admissions
        .saturating_add(current.prefix_branch_cap_admissions);
    total.prefix_branch_cap_rejections = total
        .prefix_branch_cap_rejections
        .saturating_add(current.prefix_branch_cap_rejections);
}

#[cfg(feature = "sql")]
const fn average_structural_work(
    total: SqlStructuralWorkAttribution,
    divisor: u64,
) -> SqlStructuralWorkAttribution {
    SqlStructuralWorkAttribution {
        range_conjunctions_examined: total.range_conjunctions_examined / divisor,
        range_lower_bounds_extracted: total.range_lower_bounds_extracted / divisor,
        range_upper_bounds_extracted: total.range_upper_bounds_extracted / divisor,
        range_physical_children_emitted: total.range_physical_children_emitted / divisor,
        residual_predicate_evaluations: total.residual_predicate_evaluations / divisor,
        membership_authored_members: total.membership_authored_members / divisor,
        membership_normalized_members: total.membership_normalized_members / divisor,
        membership_distinct_members: total.membership_distinct_members / divisor,
        membership_null_members: total.membership_null_members / divisor,
        membership_canonicalization_passes: total.membership_canonicalization_passes / divisor,
        membership_members_revisited: total.membership_members_revisited / divisor,
        prefix_branches_before_deduplication: total.prefix_branches_before_deduplication / divisor,
        prefix_branches_after_deduplication: total.prefix_branches_after_deduplication / divisor,
        prefix_exclusions_tested: total.prefix_exclusions_tested / divisor,
        prefix_exclusions_pruned: total.prefix_exclusions_pruned / divisor,
        prefix_branch_cap_admissions: total.prefix_branch_cap_admissions / divisor,
        prefix_branch_cap_rejections: total.prefix_branch_cap_rejections / divisor,
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
    grouped_runtime_totals: GroupedRuntimeTotals,
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
    total_structural_work: SqlStructuralWorkAttribution,
    total_response_decode_local_instructions: u64,
    total_execute_local_instructions: u64,
    total_local_instructions: u64,
    total_sql_compiled_command_cache_hits: u64,
    total_sql_compiled_command_cache_misses: u64,
    total_shared_query_plan_cache_hits: u64,
    total_shared_query_plan_cache_misses: u64,
    total_shared_query_plan_cache_insertions: u64,
    total_shared_query_plan_cache_evictions: u64,
    total_shared_query_plan_cache_rejected_oversize: u64,
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
        let mut grouped = GroupedExecutionAttribution {
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
            ..GroupedExecutionAttribution::default()
        };
        grouped_runtime_totals.apply_average(&mut grouped, divisor);
        attribution.grouped = Some(grouped);
    }
    attribution.store_get_calls = total_store_get_calls / divisor;
    attribution.index_store_get_calls = total_index_store_get_calls / divisor;
    attribution.index_store_range_scan_calls = total_index_store_range_scan_calls / divisor;
    attribution.index_store_entry_reads = total_index_store_entry_reads / divisor;
    attribution.structural_work = average_structural_work(total_structural_work, divisor);
    attribution.response_decode_local_instructions =
        total_response_decode_local_instructions / divisor;
    attribution.execute_local_instructions = total_execute_local_instructions / divisor;
    attribution.total_local_instructions = total_local_instructions / divisor;
    attribution.cache = SqlQueryCacheAttribution {
        sql_compiled_command_hits: total_sql_compiled_command_cache_hits,
        sql_compiled_command_misses: total_sql_compiled_command_cache_misses,
        shared_query_plan_hits: total_shared_query_plan_cache_hits,
        shared_query_plan_misses: total_shared_query_plan_cache_misses,
        shared_query_plan_insertions: total_shared_query_plan_cache_insertions,
        shared_query_plan_evictions: total_shared_query_plan_cache_evictions,
        shared_query_plan_rejected_oversize: total_shared_query_plan_cache_rejected_oversize,
    };

    attribution
}
#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
fn query_entity_with_perf_loop(sql: &str, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    if runs == 0 {
        return Err(invalid_perf_loop_runs_error());
    }

    let session = icydb::db!()?;
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
    let mut grouped_runtime_totals = GroupedRuntimeTotals::default();
    let mut grouped_count_totals = GroupedCountTotals::default();
    let mut total_store_get_calls = 0_u64;
    let mut total_index_store_get_calls = 0_u64;
    let mut total_index_store_range_scan_calls = 0_u64;
    let mut total_index_store_entry_reads = 0_u64;
    let mut total_structural_work = SqlStructuralWorkAttribution::default();
    let mut total_response_decode_local_instructions = 0_u64;
    let mut total_execute_local_instructions = 0_u64;
    let mut total_local_instructions = 0_u64;
    let mut total_sql_compiled_command_cache_hits = 0_u64;
    let mut total_sql_compiled_command_cache_misses = 0_u64;
    let mut total_shared_query_plan_cache_hits = 0_u64;
    let mut total_shared_query_plan_cache_misses = 0_u64;
    let mut total_shared_query_plan_cache_insertions = 0_u64;
    let mut total_shared_query_plan_cache_evictions = 0_u64;
    let mut total_shared_query_plan_cache_rejected_oversize = 0_u64;
    let mut saw_pure_covering = false;
    let mut saw_grouped = false;

    // Execute the same SQL through one session repeatedly so a real
    // session-local compiled-command cache can move the compile side honestly.
    for _ in 0..runs {
        let (result, attribution) = session.execute_trusted_sql_query_with_attribution(sql)?;
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
            grouped_runtime_totals.record(grouped);
            grouped_count_totals.record_grouped_count(grouped.count);
        }
        total_store_get_calls = total_store_get_calls.saturating_add(attribution.store_get_calls);
        total_index_store_get_calls =
            total_index_store_get_calls.saturating_add(attribution.index_store_get_calls);
        total_index_store_range_scan_calls = total_index_store_range_scan_calls
            .saturating_add(attribution.index_store_range_scan_calls);
        total_index_store_entry_reads =
            total_index_store_entry_reads.saturating_add(attribution.index_store_entry_reads);
        record_structural_work(&mut total_structural_work, attribution.structural_work);
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
        total_shared_query_plan_cache_insertions = total_shared_query_plan_cache_insertions
            .saturating_add(attribution.cache.shared_query_plan_insertions);
        total_shared_query_plan_cache_evictions = total_shared_query_plan_cache_evictions
            .saturating_add(attribution.cache.shared_query_plan_evictions);
        total_shared_query_plan_cache_rejected_oversize =
            total_shared_query_plan_cache_rejected_oversize
                .saturating_add(attribution.cache.shared_query_plan_rejected_oversize);
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
            grouped_runtime_totals,
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
            total_structural_work,
            total_response_decode_local_instructions,
            total_execute_local_instructions,
            total_local_instructions,
            total_sql_compiled_command_cache_hits,
            total_sql_compiled_command_cache_misses,
            total_shared_query_plan_cache_hits,
            total_shared_query_plan_cache_misses,
            total_shared_query_plan_cache_insertions,
            total_shared_query_plan_cache_evictions,
            total_shared_query_plan_cache_rejected_oversize,
            saw_pure_covering,
            saw_grouped,
            runs,
        ),
    })
}
/// Clear all dedicated perf fixture rows from this canister.
#[cfg(feature = "sql")]
fn reset_perf_fixtures() -> Result<(), icydb::Error> {
    let session = db()?;
    for entity in [
        "PerfAuditRelationSource",
        "PerfAuditAccount",
        "PerfAuditBlob",
        "PerfAuditHeapUser",
        "PerfAuditJournaledUser",
        "PerfAuditRelationTarget",
        "PerfAuditStreamingCompoundRow",
        "PerfAuditStreamingRow",
        "PerfAuditToken",
        "PerfAuditUser",
    ] {
        let _ = session.execute_trusted_sql_mutation(&format!("DELETE FROM {entity}"))?;
    }

    Ok(())
}

/// Load one deterministic fixture batch tuned for SQL perf audit queries.
#[cfg(feature = "test-admin-api")]
fn load_perf_fixtures() -> Result<(), icydb::Error> {
    insert_fixture_rows(perf_audit_users())?;
    insert_fixture_rows(perf_audit_heap_users())?;
    insert_fixture_rows(perf_audit_journaled_users())?;
    insert_fixture_rows(perf_audit_blobs())?;
    insert_fixture_rows(perf_audit_accounts())?;
    insert_fixture_rows(perf_audit_tokens())?;

    Ok(())
}

/// Load the fixed 0.222 key-stream/materialization baseline fixture.
#[cfg(feature = "sql")]
#[update]
fn load_streaming_execution_fixture() -> Result<StreamingExecutionFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let rows = perf_streaming_execution_rows();
        let facts = streaming_execution_fixture_facts(rows.as_slice())?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;
        insert_fixture_rows(perf_streaming_execution_compound_rows())?;

        Ok(facts)
    })
}

/// Load the frozen 10,001-row continuation fixture without attempting to
/// process it in the same message. The bounded insert batches are setup work;
/// live and exhaustive traversal happens through separate query calls below.
#[cfg(feature = "sql")]
#[update]
fn load_streaming_execution_continuation_fixture() -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        let mut first = 1;
        while first <= STREAMING_EXECUTION_CONTINUATION_ROWS {
            let last = first
                .saturating_add(STREAMING_EXECUTION_CONTINUATION_LOAD_BATCH_ROWS - 1)
                .min(STREAMING_EXECUTION_CONTINUATION_ROWS);
            insert_fixture_rows(perf_streaming_execution_rows_range(first, last))?;
            first = last.saturating_add(1);
        }

        u32::try_from(STREAMING_EXECUTION_CONTINUATION_ROWS).map_err(|_| query_validate_error())
    })
}

/// Execute one revision-tolerant page of the frozen 10,001-row fixture.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_live_page(
    continuation: Option<String>,
) -> Result<LiveQueryPageOutput, icydb::Error> {
    icydb::db::with_request_execution(|| {
        db()?.execute_trusted_live_page(
            &streaming_execution_continuation_query(),
            continuation.as_deref(),
        )
    })
}

/// Execute one revision-strict page of the frozen 10,001-row fixture.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_exhaustive_page(
    continuation: Option<String>,
    proof: Option<ReadSetRevisionProof>,
) -> Result<ExhaustiveQueryPageOutput, StreamingExhaustivePageError> {
    icydb::db::with_request_execution(|| {
        let session = db().map_err(StreamingExhaustivePageError::Database)?;
        session
            .execute_trusted_exhaustive_page(
                &streaming_execution_continuation_query(),
                continuation.as_deref(),
                proof.as_ref(),
            )
            .map_err(Into::into)
    })
}

#[cfg(feature = "sql")]
fn streaming_execution_continuation_query() -> DynamicQuery {
    DynamicQuery::new("PerfAuditStreamingRow")
        .filter(FieldRef::new("lane_a").gte(0_i32))
        .order_by(asc("id"))
        .select(["id"])
}

/// Load only the deterministic user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
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
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic account scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_account_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
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
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic blob scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_blob_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
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
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic heap-user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_heap_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_heap_users(validated_rows);
        let facts = scale_user_mirror_fixture_facts("heap_user", row_count, &rows)?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic journaled-user scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_journaled_user_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let validated_rows = validate_scale_fixture_rows(row_count)?;
        let rows = perf_scale_journaled_users(validated_rows);
        let facts = scale_journaled_user_fixture_facts(row_count, &rows)?;
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Load only the deterministic token scale surface at one reviewed cardinality.
#[cfg(feature = "sql")]
#[update]
fn load_token_scale_fixture(row_count: u32) -> Result<ScaleFixtureFacts, icydb::Error> {
    icydb::db::with_request_execution(|| {
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
        reset_perf_fixtures()?;
        insert_fixture_rows(rows)?;

        Ok(facts)
    })
}

/// Return accepted runtime schema descriptions in stable audit-surface order.
#[cfg(feature = "sql")]
#[query]
fn accepted_schema_descriptions() -> Result<Vec<EntitySchemaDescription>, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let session = db()?;

        Ok(vec![
            session.try_describe_entity_by_name("PerfAuditAccount")?,
            session.try_describe_entity_by_name("PerfAuditBlob")?,
            session.try_describe_entity_by_name("PerfAuditHeapUser")?,
            session.try_describe_entity_by_name("PerfAuditJournaledUser")?,
            session.try_describe_entity_by_name("PerfAuditRelationSource")?,
            session.try_describe_entity_by_name("PerfAuditRelationTarget")?,
            session.try_describe_entity_by_name("PerfAuditToken")?,
            session.try_describe_entity_by_name("PerfAuditUser")?,
        ])
    })
}

#[cfg(all(feature = "sql", feature = "test-admin-api"))]
fn measure_schema_application() -> Result<SchemaApplicationPerfResult, icydb::Error> {
    let session = icydb::db::DbSession::new(crate::__icydb_generated::core_db()?);
    let target = session.schema_application_target()?;
    let (first_create, exact_match) = match target.accepted_head() {
        icydb::db::ExpectedAcceptedHead::Empty => (1, 0),
        icydb::db::ExpectedAcceptedHead::Exact { .. } => (0, 1),
    };
    let start = ic_cdk::api::performance_counter(1);
    session.apply_generated_schema_fragment(
        crate::__icydb_generated::ICYDB_SCHEMA_FRAGMENT,
        crate::__icydb_generated::ICYDB_SCHEMA_MIGRATION_PLAN,
        crate::__icydb_generated::ICYDB_SCHEMA_SUBMISSION_KEY,
        crate::__icydb_generated::ICYDB_SCHEMA_ENTITY_STORES,
    )?;
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(SchemaApplicationPerfResult {
        local_instructions,
        reconcile_checks: 1,
        first_create,
        exact_match,
    })
}

/// Measure schema application inside a rollback-scoped query message.
#[cfg(all(feature = "sql", feature = "test-admin-api"))]
#[query]
fn measure_schema_application_query() -> Result<SchemaApplicationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(measure_schema_application)
}

/// Measure and persist schema application through an update message.
#[cfg(all(feature = "sql", feature = "test-admin-api"))]
#[update]
fn measure_schema_application_update() -> Result<SchemaApplicationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(measure_schema_application)
}

/// Load a small journaled-only fixture for same-WASM upgrade/reentry
/// instruction probes. The full SQL perf corpus intentionally remains larger
/// than this audit budget.
#[cfg(feature = "sql")]
#[update]
fn load_journaled_reentry_probe_fixture() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        insert_fixture_rows(perf_audit_journaled_reentry_probe_users())?;

        Ok(())
    })
}

/// Load one row per commit so Deep integrity must resume within a live journal
/// tail rather than merely observe an empty or single-batch tail.
#[cfg(feature = "sql")]
#[update]
fn load_journal_tail_integrity_fixture() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        for id in 1..=INTEGRITY_JOURNAL_TAIL_BATCHES {
            insert_fixture_rows(vec![build_perf_audit_journaled_user(
                id,
                &format!("integrity-journal-tail-{id:04}"),
                18 + id,
            )])?;
        }

        Ok(())
    })
}

/// Load the deterministic relation pair used by bounded integrity evidence.
#[cfg(feature = "sql")]
#[update]
fn load_relation_integrity_fixture() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        reset_perf_fixtures()?;
        insert_fixture_rows(perf_audit_relation_targets())?;
        insert_fixture_rows(perf_audit_relation_sources())?;

        Ok(())
    })
}

/// Execute one PerfAuditUser-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_user(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditUser-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            icydb::db!()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditUser-only SQL query through the fully attributed path
/// while measuring the same outer canister-local boundary as the total-only
/// calibration endpoint.
#[cfg(feature = "sql")]
#[query]
fn query_user_attributed_total_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let (result, _attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute one PerfAuditUser-only SQL query through the normal non-attributed
/// path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_user_total_only_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_trusted_sql_query(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute one PerfAuditUser-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditUser-only SQL query repeatedly inside one canister
/// query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_user_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one fixed 0.222 streaming-fixture query with full attribution.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Warm one fixed 0.222 streaming-fixture query under update instructions.
#[cfg(feature = "sql")]
#[update]
fn warm_streaming_execution_query_with_perf(
    sql: String,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one fixed 0.222 streaming-fixture query repeatedly in one request.
#[cfg(feature = "sql")]
#[query]
fn query_streaming_execution_loop_with_perf(
    sql: String,
    runs: u32,
) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
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
    E: StorageWriteFixtureRow,
    B: Fn(i32, &str, i32) -> E + Copy,
{
    let session = db()?;
    let first_row = build(base_id, "first-insert", 41);
    let start = ic_cdk::api::performance_counter(1);
    session.execute_trusted_structural_mutation(StructuralMutation::Insert {
        entity: E::ENTITY.to_string(),
        patch: first_row.into_structural_patch(),
    })?;
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
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: E::ENTITY.to_string(),
            patch: row.into_structural_patch(),
        })?;
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
        let key = row.primary_key_input();
        let start = ic_cdk::api::performance_counter(1);
        session.execute_trusted_structural_mutation(StructuralMutation::Update {
            entity: E::ENTITY.to_string(),
            key,
            patch: row.into_structural_patch(),
        })?;
        steady_update_total =
            steady_update_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
    }

    let mut steady_delete_total = 0_u64;
    for offset in 0..STORAGE_WRITE_MATRIX_RUNS {
        let id = base_id + 100 + i32::try_from(offset).unwrap_or(i32::MAX);
        let start = ic_cdk::api::performance_counter(1);
        let deleted = session
            .execute_trusted_structural_mutation(StructuralMutation::Delete {
                entity: E::ENTITY.to_string(),
                key: id.into(),
            })?
            .affected_rows;
        steady_delete_total =
            steady_delete_total.saturating_add(ic_cdk::api::performance_counter(1) - start);
        if deleted != 1 {
            return Err(unexpected_write_perf_count_error(storage_label, 1, deleted));
        }
    }

    let read_back_id = base_id + 10_000;
    let read_back_row = build(read_back_id, "write-read-back", 73);
    let start = ic_cdk::api::performance_counter(1);
    session.execute_trusted_structural_mutation(StructuralMutation::Insert {
        entity: E::ENTITY.to_string(),
        patch: read_back_row.into_structural_patch(),
    })?;
    let response = session.execute_trusted_sql_query(&format!(
        "SELECT id FROM {} WHERE id = {read_back_id} LIMIT 1",
        E::ENTITY
    ))?;
    let write_then_read_back_local_instructions =
        ic_cdk::api::performance_counter(1).saturating_sub(start);
    let read_back_rows = sql_write_result_row_count(&response).ok_or_else(query_validate_error)?;
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
fn measure_sql_write_statement(
    label: &str,
    sql: &str,
    expected_rows: u32,
) -> Result<(u64, u32), icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_mutation(sql)?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let row_count = ensure_sql_write_row_count(label, &result, expected_rows)?;

    Ok((instructions, row_count))
}

#[cfg(feature = "sql")]
fn measure_sql_exact_update_statement(
    label: &str,
    sql: &str,
    expected_rows: u32,
) -> Result<(u64, u32), icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    let result = db()?.execute_trusted_sql_exact_update(sql, expected_rows)?;
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
    E: StructuralFixtureRow,
    B: Fn(i32, &str, i32) -> E + Copy,
{
    let expected_rows = u32::try_from(SQL_WRITE_MATERIALIZATION_ROWS).unwrap_or(u32::MAX);
    let update_count_start = base_id + 2_000;
    let update_returning_start = base_id + 3_000;
    let delete_count_start = base_id + 4_000;
    let delete_returning_start = base_id + 5_000;

    insert_fixture_rows(sql_write_window_rows(
        update_count_start,
        "update-count",
        41,
        build,
    ))?;
    insert_fixture_rows(sql_write_window_rows(
        update_returning_start,
        "update-returning",
        51,
        build,
    ))?;
    insert_fixture_rows(sql_write_window_rows(
        delete_count_start,
        "delete-count",
        61,
        build,
    ))?;
    insert_fixture_rows(sql_write_window_rows(
        delete_returning_start,
        "delete-returning",
        71,
        build,
    ))?;

    let update_count_end = update_count_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let update_returning_end = update_returning_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let delete_count_end = delete_count_start + SQL_WRITE_MATERIALIZATION_ROWS;
    let delete_returning_end = delete_returning_start + SQL_WRITE_MATERIALIZATION_ROWS;

    let update_count = measure_sql_exact_update_statement(
        "SQL write materialization UPDATE count",
        &format!(
            "UPDATE {entity_name} SET age = 77 \
             WHERE id >= {update_count_start} AND id < {update_count_end}"
        ),
        expected_rows,
    )?;
    let update_returning = measure_sql_exact_update_statement(
        "SQL write materialization UPDATE RETURNING",
        &format!(
            "UPDATE {entity_name} SET age = 78 \
             WHERE id >= {update_returning_start} AND id < {update_returning_end} \
             RETURNING id"
        ),
        expected_rows,
    )?;
    let delete_count = measure_sql_write_statement(
        "SQL write materialization DELETE count",
        &format!(
            "DELETE FROM {entity_name} \
             WHERE id >= {delete_count_start} AND id < {delete_count_end}"
        ),
        expected_rows,
    )?;
    let delete_returning = measure_sql_write_statement(
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
    icydb::db::with_request_execution(|| {
        measure_storage_write_matrix::<PerfAuditHeapUser, _>(
            "heap write matrix",
            30_000,
            build_perf_audit_heap_user,
        )
    })
}

/// Measure the journaled typed write path.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_write_matrix_perf() -> Result<StorageWritePerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_storage_write_matrix::<PerfAuditJournaledUser, _>(
            "journaled write matrix",
            40_000,
            build_perf_audit_journaled_user,
        )
    })
}

/// Measure the matched journaled typed-write path before and after one simple
/// accepted check, including the exact bounded publication scan.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_constraint_write_perf()
-> Result<ConstraintActivationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let no_check = measure_storage_write_matrix::<PerfAuditJournaledUser, _>(
            "journaled no-check write matrix",
            70_000,
            build_perf_audit_journaled_user,
        )?;

        let start = ic_cdk::api::performance_counter(1);
        let add_result = db()?.execute_admin_sql_ddl(
            "ALTER TABLE PerfAuditJournaledUser ADD CONSTRAINT \
         perf_audit_age_nonnegative CHECK (age >= 0) NOT VALID \
         EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
        )?;
        let add_check_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);
        let SqlQueryResult::Ddl {
            rows_scanned: add_check_rows_scanned,
            ..
        } = add_result
        else {
            return Err(query_validate_error());
        };

        Ok(ConstraintActivationPerfResult {
            no_check,
            add_check_local_instructions,
            add_check_rows_scanned,
        })
    })
}

/// Measure the journaled typed-write path after the preceding audit call has
/// published its simple accepted check.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_checked_write_perf() -> Result<StorageWritePerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_storage_write_matrix::<PerfAuditJournaledUser, _>(
            "journaled checked write matrix",
            90_000,
            build_perf_audit_journaled_user,
        )
    })
}

/// Advance the audit-only journaled check activation to accepted authority.
#[cfg(feature = "sql")]
#[update]
fn validate_journaled_user_perf_check() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        const MAX_VALIDATION_STEPS: usize = 4;

        for _ in 0..MAX_VALIDATION_STEPS {
            let result = db()?.execute_admin_sql_ddl(
                "ALTER TABLE PerfAuditJournaledUser \
             VALIDATE CONSTRAINT perf_audit_age_nonnegative",
            )?;
            if matches!(
                result,
                SqlQueryResult::Ddl {
                    constraint_validation: Some(ref validation),
                    ..
                } if validation.complete
            ) {
                return Ok(());
            }
        }

        Err(query_validate_error())
    })
}

/// Measure broad SQL write materialization shapes against heap storage.
#[cfg(feature = "sql")]
#[update]
fn measure_heap_user_sql_write_materialization_perf()
-> Result<SqlWriteMaterializationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_sql_write_materialization_matrix::<PerfAuditHeapUser, _>(
            "PerfAuditHeapUser",
            50_000,
            build_perf_audit_heap_user,
        )
    })
}

/// Measure broad SQL write materialization shapes against journaled storage.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_sql_write_materialization_perf()
-> Result<SqlWriteMaterializationPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        measure_sql_write_materialization_matrix::<PerfAuditJournaledUser, _>(
            "PerfAuditJournaledUser",
            60_000,
            build_perf_audit_journaled_user,
        )
    })
}

/// Measure one complete trusted resumable convergence operation without
/// exposing its proof-bearing continuation across the canister boundary.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_user_resumable_update_perf() -> Result<ResumableUpdatePerfResult, icydb::Error>
{
    icydb::db::with_request_execution(|| {
        const MAX_STEPS: usize = 16;

        let session = db()?;
        let sql = "UPDATE PerfAuditJournaledUser SET name = 'resumable-measured' WHERE age >= 0";
        let operation_id = Ulid::from_bytes(0x210_0000_0000_0001_u128.to_be_bytes());
        let prepare_start = ic_cdk::api::performance_counter(1);
        let mut continuation = session.prepare_trusted_sql_resumable_update(operation_id, sql)?;
        let prepare_local_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(prepare_start);
        let mut phase = icydb::db::TrustedResumableUpdatePhase::Forward;
        let mut forward_local_instructions = Vec::new();
        let mut verify_local_instructions = Vec::new();
        let mut forward_keys_scanned = 0_u32;
        let mut verify_keys_scanned = 0_u32;
        let mut rows_updated = 0_u32;

        for _ in 0..MAX_STEPS {
            let start = ic_cdk::api::performance_counter(1);
            let receipt =
                session.resume_trusted_sql_resumable_update(operation_id, sql, &continuation)?;
            let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
            match phase {
                icydb::db::TrustedResumableUpdatePhase::Forward => {
                    forward_local_instructions.push(instructions);
                    forward_keys_scanned =
                        forward_keys_scanned.saturating_add(receipt.keys_scanned());
                }
                icydb::db::TrustedResumableUpdatePhase::Verify => {
                    verify_local_instructions.push(instructions);
                    verify_keys_scanned =
                        verify_keys_scanned.saturating_add(receipt.keys_scanned());
                }
            }
            rows_updated = rows_updated.saturating_add(receipt.rows_updated());
            phase = receipt.phase();
            if receipt.complete() {
                return Ok(ResumableUpdatePerfResult {
                    prepare_local_instructions,
                    forward_local_instructions,
                    verify_local_instructions,
                    forward_keys_scanned,
                    verify_keys_scanned,
                    rows_updated,
                });
            }
            continuation = receipt
                .into_continuation()
                .ok_or_else(query_validate_error)?;
        }

        Err(query_validate_error())
    })
}

/// Measure one canonical administrative integrity SQL operation.
#[cfg(feature = "sql")]
#[update]
// This audit endpoint deliberately exposes the canonical typed integrity
// error. Boxing it would change the generated Candid response contract.
#[allow(clippy::result_large_err)]
fn measure_integrity_sql_perf(sql: String) -> Result<IntegritySqlPerfResult, SqlIntegrityError> {
    icydb::db::with_request_execution(|| {
        let session = db().map_err(SqlIntegrityError::Sql)?;
        let owner = IntegrityJobOwner::new("audit::sql-perf")
            .map_err(IntegrityCheckError::Job)
            .map_err(SqlIntegrityError::Integrity)?;
        let start = ic_cdk::api::performance_counter(1);
        let result = session.execute_admin_integrity_sql(sql.as_str(), owner)?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(IntegritySqlPerfResult {
            result,
            local_instructions,
        })
    })
}

/// Execute one PerfAuditHeapUser-only SQL query and attach one local
/// instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditHeapUser-only SQL query through the normal
/// non-attributed path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_heap_user_total_only_perf(sql: String) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_trusted_sql_query(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute one PerfAuditHeapUser-only SQL query through the update surface so
/// the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_heap_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
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
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditJournaledUser-only SQL query and attach one local
/// instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditJournaledUser-only SQL query through the normal
/// non-attributed path and measure only the top-level canister-local delta.
#[cfg(feature = "sql")]
#[query]
fn query_journaled_user_total_only_perf(
    sql: String,
) -> Result<SqlTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = db()?.execute_trusted_sql_query(sql.as_str())?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(SqlTotalOnlyPerfResult {
            result,
            instructions,
        })
    })
}

/// Execute the journaled LIMIT 1 shape through an update call. After a
/// same-WASM upgrade this gives the integration harness one normal guarded
/// reentry probe that includes any required recovery/rebuild work.
#[cfg(feature = "sql")]
#[update]
fn measure_journaled_reentry_perf() -> Result<ReadTotalOnlyPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let response = db()?.execute_trusted_sql_query(
            "SELECT id FROM PerfAuditJournaledUser ORDER BY id LIMIT 1",
        )?;
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let row_count = sql_write_result_row_count(&response).ok_or_else(query_validate_error)?;

        Ok(ReadTotalOnlyPerfResult {
            row_count,
            instructions,
        })
    })
}

/// Execute one PerfAuditJournaledUser-only SQL query through the update surface
/// so the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_journaled_user_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
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
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditAccount-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_account(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditAccount-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_account_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditAccount-only SQL query through the update surface so
/// the canister can persist any warmed in-heap query caches for later query
/// calls.
#[cfg(feature = "sql")]
#[update]
fn warm_account_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
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
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditBlob-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_blob(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditBlob-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditBlob-only SQL query through the update surface so the
/// canister can persist any warmed in-heap query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_blob_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditBlob-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_blob_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
}

/// Execute one PerfAuditToken-only SQL query.
#[cfg(feature = "sql")]
#[query]
fn query_token(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

/// Execute one PerfAuditToken-only SQL query and attach one local instruction
/// sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute one PerfAuditToken-only SQL query through the update surface so the
/// canister can persist warmed query caches for later query calls.
#[cfg(feature = "sql")]
#[update]
fn warm_token_query_with_perf(sql: String) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let (result, attribution) =
            db()?.execute_trusted_sql_query_with_attribution(sql.as_str())?;

        Ok(SqlQueryPerfResult {
            result,
            attribution,
        })
    })
}

/// Execute the same PerfAuditToken-only SQL query repeatedly inside one
/// canister query call and report the per-run average instruction sample.
#[cfg(feature = "sql")]
#[query]
fn query_token_loop_with_perf(sql: String, runs: u32) -> Result<SqlQueryPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| query_entity_with_perf_loop(sql.as_str(), runs))
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
    const MANY_GROUP_COUNT: i32 = 100;

    let quarter_rows = row_count / 4;
    let grouped_age_rows = quarter_rows / 4;
    (1..=row_count)
        .map(|id| {
            let quarter_match = id <= quarter_rows;
            let age = if id <= grouped_age_rows {
                31
            } else if id <= grouped_age_rows * 2 {
                32
            } else if id <= grouped_age_rows * 3 {
                33
            } else if quarter_match {
                34
            } else {
                43
            };
            PerfAuditUser {
                id,
                name: format!("scale-group-{:03}", ((id - 1) % MANY_GROUP_COUNT) + 1),
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
fn perf_streaming_execution_rows() -> Vec<PerfAuditStreamingRow> {
    perf_streaming_execution_rows_range(1, STREAMING_EXECUTION_FIXTURE_ROWS)
}

#[cfg(feature = "sql")]
fn perf_streaming_execution_rows_range(first: i32, last: i32) -> Vec<PerfAuditStreamingRow> {
    (first..=last)
        .map(|id| PerfAuditStreamingRow {
            id,
            lane_a: streaming_lane_a(id),
            lane_b: streaming_lane_b(id),
            group_key: streaming_group_key(id),
            sort_key: streaming_sort_key(id),
            label: streaming_label(id).to_string(),
            payload: perf_blob(id.to_le_bytes()[0], streaming_payload_len(id)),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_streaming_execution_compound_rows() -> Vec<PerfAuditStreamingCompoundRow> {
    (1..=STREAMING_EXECUTION_FIXTURE_ROWS)
        .map(|id| PerfAuditStreamingCompoundRow {
            id,
            lane_a: streaming_lane_a(id),
            lane_b: streaming_lane_b(id),
            group_key: streaming_group_key(id),
            sort_key: streaming_sort_key(id),
            label: streaming_label(id).to_string(),
            payload: perf_blob(id.to_le_bytes()[0], 32),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
const fn streaming_lane_a(id: i32) -> i32 {
    (id * 17 + STREAMING_EXECUTION_FIXTURE_SEED_I32) % 97
}

#[cfg(feature = "sql")]
const fn streaming_lane_b(id: i32) -> i32 {
    (id * 29 + STREAMING_EXECUTION_FIXTURE_SEED_I32 + 2) % 101
}

#[cfg(feature = "sql")]
const fn streaming_group_key(id: i32) -> i32 {
    (id - 1) % 17
}

#[cfg(feature = "sql")]
const fn streaming_sort_key(id: i32) -> i32 {
    (id * 37 + STREAMING_EXECUTION_FIXTURE_SEED_I32) % STREAMING_EXECUTION_FIXTURE_ROWS
}

#[cfg(feature = "sql")]
const fn streaming_label(id: i32) -> &'static str {
    match id {
        1 => "early-wide",
        STREAMING_EXECUTION_FIXTURE_ROWS => "late-match",
        _ => "ordinary",
    }
}

#[cfg(feature = "sql")]
const fn streaming_payload_len(id: i32) -> usize {
    match id {
        1 => STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES[0],
        2 => STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES[1],
        3 => STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES[2],
        _ => 32,
    }
}

#[cfg(feature = "sql")]
fn streaming_execution_fixture_facts(
    rows: &[PerfAuditStreamingRow],
) -> Result<StreamingExecutionFixtureFacts, icydb::Error> {
    let fixture_rows = u32::try_from(rows.len()).map_err(|_| query_validate_error())?;
    let first_lane_matches = u32::try_from(rows.iter().filter(|row| row.lane_a == 0).count())
        .map_err(|_| query_validate_error())?;
    let second_lane_matches = u32::try_from(rows.iter().filter(|row| row.lane_b == 0).count())
        .map_err(|_| query_validate_error())?;
    let sparse_overlap_rows = u32::try_from(
        rows.iter()
            .filter(|row| row.lane_a == 0 && row.lane_b == 0)
            .count(),
    )
    .map_err(|_| query_validate_error())?;
    let empty_overlap_rows = u32::try_from(
        rows.iter()
            .filter(|row| row.lane_a == 0 && row.lane_b == 1)
            .count(),
    )
    .map_err(|_| query_validate_error())?;
    let wide_payload_bytes = STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES
        .iter()
        .copied()
        .map(u32::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| query_validate_error())?;

    Ok(StreamingExecutionFixtureFacts {
        profile_version: STREAMING_EXECUTION_FIXTURE_PROFILE_VERSION,
        seed: STREAMING_EXECUTION_FIXTURE_SEED,
        fixture_rows,
        lane_a_zero_rows: first_lane_matches,
        lane_b_zero_rows: second_lane_matches,
        sparse_overlap_rows,
        empty_overlap_rows,
        group_count: 17,
        wide_payload_bytes,
    })
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
#[cfg(feature = "test-admin-api")]
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

#[cfg(feature = "sql")]
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
#[cfg(feature = "test-admin-api")]
fn perf_audit_heap_users() -> Vec<PerfAuditHeapUser> {
    (1..=512)
        .map(|id| build_perf_audit_heap_user(id, &format!("heap-user-{id:04}"), 18 + (id % 47)))
        .collect()
}

#[cfg(feature = "sql")]
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
#[cfg(feature = "test-admin-api")]
fn perf_audit_journaled_users() -> Vec<PerfAuditJournaledUser> {
    (1..=512)
        .map(|id| {
            build_perf_audit_journaled_user(id, &format!("journaled-user-{id:04}"), 18 + (id % 47))
        })
        .collect()
}

#[cfg(feature = "sql")]
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

#[cfg(feature = "sql")]
fn perf_audit_relation_targets() -> Vec<PerfAuditRelationTarget> {
    (1..=16)
        .map(|id| PerfAuditRelationTarget {
            id,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

#[cfg(feature = "sql")]
fn perf_audit_relation_sources() -> Vec<PerfAuditRelationSource> {
    (1..=16)
        .map(|id| PerfAuditRelationSource {
            id,
            target_id: ((id - 1) % 8) + 1,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        })
        .collect()
}

/// Build one deterministic blob payload for perf fixture rows.
#[cfg(feature = "sql")]
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
#[cfg(feature = "test-admin-api")]
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
#[cfg(feature = "test-admin-api")]
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

#[cfg(feature = "sql")]
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
#[cfg(feature = "test-admin-api")]
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
