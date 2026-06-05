//! Module: metrics::sink::counters
//! Responsibility: map stable metrics events into global and per-entity counters.
//! Does not own: metrics sink dispatch, span lifetimes, or report/reset APIs.
//! Boundary: provides counter-bucket helpers used only by the concrete metrics sink.

use crate::{error::ErrorClass, metrics::state as metrics};

use super::{
    CacheKind, CacheMissReason, CacheOutcome, ExecKind, ExecOutcome, GroupedPlanExecutionMode,
    PlanChoiceReason, PlanKind, PreparedShapeFinalizationOutcome, SchemaReconcileOutcome,
    SchemaTransitionOutcome, SqlCompileRejectPhase, SqlWriteKind,
};

// Replace one entity-scoped gauge contribution inside an aggregate total. This
// keeps global footprint gauges current even when the same entity reports a
// newer observed size later in the window.
pub(super) const fn replace_gauge_total(total: &mut u64, previous: u64, current: u64) {
    *total = total.saturating_sub(previous).saturating_add(current);
}

// Start counters are used for ordinary spans and for load errors that return
// before the successful load finalizers create their normal span.
#[remain::check]
pub(super) const fn record_global_exec_start(ops: &mut metrics::EventOps, kind: ExecKind) {
    #[remain::sorted]
    match kind {
        ExecKind::Delete => {
            ops.delete_calls = ops.delete_calls.saturating_add(1);
        }
        ExecKind::Load => ops.load_calls = ops.load_calls.saturating_add(1),
        ExecKind::Save => ops.save_calls = ops.save_calls.saturating_add(1),
    }
}

// Mirror executor starts into entity summaries so attempts and outcomes can be
// read from the same per-entity row in the report.
#[remain::check]
pub(super) const fn record_entity_exec_start(ops: &mut metrics::EntityCounters, kind: ExecKind) {
    #[remain::sorted]
    match kind {
        ExecKind::Delete => {
            ops.delete_calls = ops.delete_calls.saturating_add(1);
        }
        ExecKind::Load => {
            ops.load_calls = ops.load_calls.saturating_add(1);
        }
        ExecKind::Save => {
            ops.save_calls = ops.save_calls.saturating_add(1);
        }
    }
}

// Outcome counters are shared by all executor kinds. Per-kind attempts still
// come from load/save/delete call counters, so this layer only tracks finish
// status and error taxonomy.
#[remain::check]
pub(super) const fn record_global_exec_outcome(ops: &mut metrics::EventOps, outcome: ExecOutcome) {
    #[remain::sorted]
    match outcome {
        ExecOutcome::Aborted => {
            ops.exec_aborted = ops.exec_aborted.saturating_add(1);
        }
        ExecOutcome::ErrorConflict => {
            ops.exec_error_conflict = ops.exec_error_conflict.saturating_add(1);
        }
        ExecOutcome::ErrorCorruption => {
            ops.exec_error_corruption = ops.exec_error_corruption.saturating_add(1);
        }
        ExecOutcome::ErrorIncompatiblePersistedFormat => {
            ops.exec_error_incompatible_persisted_format = ops
                .exec_error_incompatible_persisted_format
                .saturating_add(1);
        }
        ExecOutcome::ErrorInternal => {
            ops.exec_error_internal = ops.exec_error_internal.saturating_add(1);
        }
        ExecOutcome::ErrorInvariantViolation => {
            ops.exec_error_invariant_violation =
                ops.exec_error_invariant_violation.saturating_add(1);
        }
        ExecOutcome::ErrorNotFound => {
            ops.exec_error_not_found = ops.exec_error_not_found.saturating_add(1);
        }
        ExecOutcome::ErrorUnsupported => {
            ops.exec_error_unsupported = ops.exec_error_unsupported.saturating_add(1);
        }
        ExecOutcome::Success => {
            ops.exec_success = ops.exec_success.saturating_add(1);
        }
    }
}

// Mirror outcome attribution into entity summaries so failed operations can be
// correlated with the model that owned the executor span.
#[remain::check]
pub(super) const fn record_entity_exec_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: ExecOutcome,
) {
    #[remain::sorted]
    match outcome {
        ExecOutcome::Aborted => {
            ops.exec_aborted = ops.exec_aborted.saturating_add(1);
        }
        ExecOutcome::ErrorConflict => {
            ops.exec_error_conflict = ops.exec_error_conflict.saturating_add(1);
        }
        ExecOutcome::ErrorCorruption => {
            ops.exec_error_corruption = ops.exec_error_corruption.saturating_add(1);
        }
        ExecOutcome::ErrorIncompatiblePersistedFormat => {
            ops.exec_error_incompatible_persisted_format = ops
                .exec_error_incompatible_persisted_format
                .saturating_add(1);
        }
        ExecOutcome::ErrorInternal => {
            ops.exec_error_internal = ops.exec_error_internal.saturating_add(1);
        }
        ExecOutcome::ErrorInvariantViolation => {
            ops.exec_error_invariant_violation =
                ops.exec_error_invariant_violation.saturating_add(1);
        }
        ExecOutcome::ErrorNotFound => {
            ops.exec_error_not_found = ops.exec_error_not_found.saturating_add(1);
        }
        ExecOutcome::ErrorUnsupported => {
            ops.exec_error_unsupported = ops.exec_error_unsupported.saturating_add(1);
        }
        ExecOutcome::Success => {
            ops.exec_success = ops.exec_success.saturating_add(1);
        }
    }
}

#[remain::check]
pub(super) const fn record_global_sql_write_kind(ops: &mut metrics::EventOps, kind: SqlWriteKind) {
    #[remain::sorted]
    match kind {
        SqlWriteKind::Delete => {
            ops.sql_delete_calls = ops.sql_delete_calls.saturating_add(1);
        }
        SqlWriteKind::Insert => {
            ops.sql_insert_calls = ops.sql_insert_calls.saturating_add(1);
        }
        SqlWriteKind::InsertSelect => {
            ops.sql_insert_select_calls = ops.sql_insert_select_calls.saturating_add(1);
        }
        SqlWriteKind::Update => {
            ops.sql_update_calls = ops.sql_update_calls.saturating_add(1);
        }
    }
}

#[remain::check]
pub(super) const fn record_entity_sql_write_kind(
    ops: &mut metrics::EntityCounters,
    kind: SqlWriteKind,
) {
    #[remain::sorted]
    match kind {
        SqlWriteKind::Delete => {
            ops.sql_delete_calls = ops.sql_delete_calls.saturating_add(1);
        }
        SqlWriteKind::Insert => {
            ops.sql_insert_calls = ops.sql_insert_calls.saturating_add(1);
        }
        SqlWriteKind::InsertSelect => {
            ops.sql_insert_select_calls = ops.sql_insert_select_calls.saturating_add(1);
        }
        SqlWriteKind::Update => {
            ops.sql_update_calls = ops.sql_update_calls.saturating_add(1);
        }
    }
}

// Schema reconciliation is a startup/metadata trust boundary, not normal query
// execution. Count the check plus the stable outcome bucket so operators can
// distinguish expected first-contact writes from fail-closed drift.
#[remain::check]
pub(super) const fn record_global_schema_reconcile_outcome(
    ops: &mut metrics::EventOps,
    outcome: SchemaReconcileOutcome,
) {
    ops.schema_reconcile_checks = ops.schema_reconcile_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaReconcileOutcome::ExactMatch => {
            ops.schema_reconcile_exact_match = ops.schema_reconcile_exact_match.saturating_add(1);
        }
        SchemaReconcileOutcome::FirstCreate => {
            ops.schema_reconcile_first_create = ops.schema_reconcile_first_create.saturating_add(1);
        }
        SchemaReconcileOutcome::LatestSnapshotCorrupt => {
            ops.schema_reconcile_latest_snapshot_corrupt = ops
                .schema_reconcile_latest_snapshot_corrupt
                .saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedFieldSlot => {
            ops.schema_reconcile_rejected_field_slot =
                ops.schema_reconcile_rejected_field_slot.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedOther => {
            ops.schema_reconcile_rejected_other =
                ops.schema_reconcile_rejected_other.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedRowLayout => {
            ops.schema_reconcile_rejected_row_layout =
                ops.schema_reconcile_rejected_row_layout.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedSchemaVersion => {
            ops.schema_reconcile_rejected_schema_version = ops
                .schema_reconcile_rejected_schema_version
                .saturating_add(1);
        }
        SchemaReconcileOutcome::StoreWriteError => {
            ops.schema_reconcile_store_write_error =
                ops.schema_reconcile_store_write_error.saturating_add(1);
        }
    }
}

// Mirror schema reconciliation outcomes into the entity summary because one
// drifting entity schema should be visible without inspecting global totals.
#[remain::check]
pub(super) const fn record_entity_schema_reconcile_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: SchemaReconcileOutcome,
) {
    ops.schema_reconcile_checks = ops.schema_reconcile_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaReconcileOutcome::ExactMatch => {
            ops.schema_reconcile_exact_match = ops.schema_reconcile_exact_match.saturating_add(1);
        }
        SchemaReconcileOutcome::FirstCreate => {
            ops.schema_reconcile_first_create = ops.schema_reconcile_first_create.saturating_add(1);
        }
        SchemaReconcileOutcome::LatestSnapshotCorrupt => {
            ops.schema_reconcile_latest_snapshot_corrupt = ops
                .schema_reconcile_latest_snapshot_corrupt
                .saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedFieldSlot => {
            ops.schema_reconcile_rejected_field_slot =
                ops.schema_reconcile_rejected_field_slot.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedOther => {
            ops.schema_reconcile_rejected_other =
                ops.schema_reconcile_rejected_other.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedRowLayout => {
            ops.schema_reconcile_rejected_row_layout =
                ops.schema_reconcile_rejected_row_layout.saturating_add(1);
        }
        SchemaReconcileOutcome::RejectedSchemaVersion => {
            ops.schema_reconcile_rejected_schema_version = ops
                .schema_reconcile_rejected_schema_version
                .saturating_add(1);
        }
        SchemaReconcileOutcome::StoreWriteError => {
            ops.schema_reconcile_store_write_error =
                ops.schema_reconcile_store_write_error.saturating_add(1);
        }
    }
}

// Schema transition outcomes are narrower than reconciliation outcomes: they
// count only policy decisions for an existing accepted snapshot.
#[remain::check]
pub(super) const fn record_global_schema_transition_outcome(
    ops: &mut metrics::EventOps,
    outcome: SchemaTransitionOutcome,
) {
    ops.schema_transition_checks = ops.schema_transition_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaTransitionOutcome::AppendOnlyNullableFields => {
            ops.schema_transition_append_only_nullable_fields = ops
                .schema_transition_append_only_nullable_fields
                .saturating_add(1);
        }
        SchemaTransitionOutcome::ExactMatch => {
            ops.schema_transition_exact_match = ops.schema_transition_exact_match.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedEntityIdentity => {
            ops.schema_transition_rejected_entity_identity = ops
                .schema_transition_rejected_entity_identity
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldContract => {
            ops.schema_transition_rejected_field_contract = ops
                .schema_transition_rejected_field_contract
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldSlot => {
            ops.schema_transition_rejected_field_slot =
                ops.schema_transition_rejected_field_slot.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedRowLayout => {
            ops.schema_transition_rejected_row_layout =
                ops.schema_transition_rejected_row_layout.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSchemaVersion => {
            ops.schema_transition_rejected_schema_version = ops
                .schema_transition_rejected_schema_version
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSnapshot => {
            ops.schema_transition_rejected_snapshot =
                ops.schema_transition_rejected_snapshot.saturating_add(1);
        }
    }
}

// Mirror transition decisions into entity summaries so one drifting entity can
// be found without conflating policy rejection with store/recovery failures.
#[remain::check]
pub(super) const fn record_entity_schema_transition_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: SchemaTransitionOutcome,
) {
    ops.schema_transition_checks = ops.schema_transition_checks.saturating_add(1);

    #[remain::sorted]
    match outcome {
        SchemaTransitionOutcome::AppendOnlyNullableFields => {
            ops.schema_transition_append_only_nullable_fields = ops
                .schema_transition_append_only_nullable_fields
                .saturating_add(1);
        }
        SchemaTransitionOutcome::ExactMatch => {
            ops.schema_transition_exact_match = ops.schema_transition_exact_match.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedEntityIdentity => {
            ops.schema_transition_rejected_entity_identity = ops
                .schema_transition_rejected_entity_identity
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldContract => {
            ops.schema_transition_rejected_field_contract = ops
                .schema_transition_rejected_field_contract
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedFieldSlot => {
            ops.schema_transition_rejected_field_slot =
                ops.schema_transition_rejected_field_slot.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedRowLayout => {
            ops.schema_transition_rejected_row_layout =
                ops.schema_transition_rejected_row_layout.saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSchemaVersion => {
            ops.schema_transition_rejected_schema_version = ops
                .schema_transition_rejected_schema_version
                .saturating_add(1);
        }
        SchemaTransitionOutcome::RejectedSnapshot => {
            ops.schema_transition_rejected_snapshot =
                ops.schema_transition_rejected_snapshot.saturating_add(1);
        }
    }
}

// SQL compile rejects are counted before execution/write metrics because they
// represent SQL admission failures. The phase bucket intentionally stays broad
// to avoid leaking SQL text or semantic diagnostics through metrics.
#[remain::check]
pub(super) const fn record_global_sql_compile_reject_phase(
    ops: &mut metrics::EventOps,
    phase: SqlCompileRejectPhase,
) {
    ops.sql_compile_rejects = ops.sql_compile_rejects.saturating_add(1);

    #[remain::sorted]
    match phase {
        SqlCompileRejectPhase::CacheKey => {
            ops.sql_compile_reject_cache_key = ops.sql_compile_reject_cache_key.saturating_add(1);
        }
        SqlCompileRejectPhase::Parse => {
            ops.sql_compile_reject_parse = ops.sql_compile_reject_parse.saturating_add(1);
        }
        SqlCompileRejectPhase::Semantic => {
            ops.sql_compile_reject_semantic = ops.sql_compile_reject_semantic.saturating_add(1);
        }
    }
}

// Per-entity compile rejects make it possible to distinguish a global SQL
// client issue from one model whose schema or query surface rejects commands.
#[remain::check]
pub(super) const fn record_entity_sql_compile_reject_phase(
    ops: &mut metrics::EntityCounters,
    phase: SqlCompileRejectPhase,
) {
    ops.sql_compile_rejects = ops.sql_compile_rejects.saturating_add(1);

    #[remain::sorted]
    match phase {
        SqlCompileRejectPhase::CacheKey => {
            ops.sql_compile_reject_cache_key = ops.sql_compile_reject_cache_key.saturating_add(1);
        }
        SqlCompileRejectPhase::Parse => {
            ops.sql_compile_reject_parse = ops.sql_compile_reject_parse.saturating_add(1);
        }
        SqlCompileRejectPhase::Semantic => {
            ops.sql_compile_reject_semantic = ops.sql_compile_reject_semantic.saturating_add(1);
        }
    }
}

// SQL write errors are split by command kind so dashboards can distinguish
// invalid write shapes from failing statement families.
#[remain::check]
pub(super) const fn record_global_sql_write_error_kind(
    ops: &mut metrics::EventOps,
    kind: SqlWriteKind,
) {
    #[remain::sorted]
    match kind {
        SqlWriteKind::Delete => {
            ops.sql_write_error_delete = ops.sql_write_error_delete.saturating_add(1);
        }
        SqlWriteKind::Insert => {
            ops.sql_write_error_insert = ops.sql_write_error_insert.saturating_add(1);
        }
        SqlWriteKind::InsertSelect => {
            ops.sql_write_error_insert_select = ops.sql_write_error_insert_select.saturating_add(1);
        }
        SqlWriteKind::Update => {
            ops.sql_write_error_update = ops.sql_write_error_update.saturating_add(1);
        }
    }
}

// Keep the per-entity write-error command counters aligned with the global
// report so entity summaries can explain localized write rejection hotspots.
#[remain::check]
pub(super) const fn record_entity_sql_write_error_kind(
    ops: &mut metrics::EntityCounters,
    kind: SqlWriteKind,
) {
    #[remain::sorted]
    match kind {
        SqlWriteKind::Delete => {
            ops.sql_write_error_delete = ops.sql_write_error_delete.saturating_add(1);
        }
        SqlWriteKind::Insert => {
            ops.sql_write_error_insert = ops.sql_write_error_insert.saturating_add(1);
        }
        SqlWriteKind::InsertSelect => {
            ops.sql_write_error_insert_select = ops.sql_write_error_insert_select.saturating_add(1);
        }
        SqlWriteKind::Update => {
            ops.sql_write_error_update = ops.sql_write_error_update.saturating_add(1);
        }
    }
}

// Error-class counters retain the stable taxonomy without tying the metrics
// report to internal SQL planning, validation, or executor error types.
#[remain::check]
pub(super) const fn record_global_sql_write_error_class(
    ops: &mut metrics::EventOps,
    class: ErrorClass,
) {
    #[remain::sorted]
    match class {
        ErrorClass::Conflict => {
            ops.sql_write_error_conflict = ops.sql_write_error_conflict.saturating_add(1);
        }
        ErrorClass::Corruption => {
            ops.sql_write_error_corruption = ops.sql_write_error_corruption.saturating_add(1);
        }
        ErrorClass::IncompatiblePersistedFormat => {
            ops.sql_write_error_incompatible_persisted_format = ops
                .sql_write_error_incompatible_persisted_format
                .saturating_add(1);
        }
        ErrorClass::Internal => {
            ops.sql_write_error_internal = ops.sql_write_error_internal.saturating_add(1);
        }
        ErrorClass::InvariantViolation => {
            ops.sql_write_error_invariant_violation =
                ops.sql_write_error_invariant_violation.saturating_add(1);
        }
        ErrorClass::NotFound => {
            ops.sql_write_error_not_found = ops.sql_write_error_not_found.saturating_add(1);
        }
        ErrorClass::Unsupported => {
            ops.sql_write_error_unsupported = ops.sql_write_error_unsupported.saturating_add(1);
        }
    }
}

// Mirror error-class totals per entity so one noisy table does not disappear
// into the process-wide rejected-write totals.
#[remain::check]
pub(super) const fn record_entity_sql_write_error_class(
    ops: &mut metrics::EntityCounters,
    class: ErrorClass,
) {
    #[remain::sorted]
    match class {
        ErrorClass::Conflict => {
            ops.sql_write_error_conflict = ops.sql_write_error_conflict.saturating_add(1);
        }
        ErrorClass::Corruption => {
            ops.sql_write_error_corruption = ops.sql_write_error_corruption.saturating_add(1);
        }
        ErrorClass::IncompatiblePersistedFormat => {
            ops.sql_write_error_incompatible_persisted_format = ops
                .sql_write_error_incompatible_persisted_format
                .saturating_add(1);
        }
        ErrorClass::Internal => {
            ops.sql_write_error_internal = ops.sql_write_error_internal.saturating_add(1);
        }
        ErrorClass::InvariantViolation => {
            ops.sql_write_error_invariant_violation =
                ops.sql_write_error_invariant_violation.saturating_add(1);
        }
        ErrorClass::NotFound => {
            ops.sql_write_error_not_found = ops.sql_write_error_not_found.saturating_add(1);
        }
        ErrorClass::Unsupported => {
            ops.sql_write_error_unsupported = ops.sql_write_error_unsupported.saturating_add(1);
        }
    }
}

// Cache counters are intentionally cache-family specific and outcome specific
// so the report can distinguish a cold cache from a warmed cache that inserts
// successfully after misses.
#[remain::check]
pub(super) const fn record_global_cache_outcome(
    ops: &mut metrics::EventOps,
    kind: CacheKind,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_global_shared_query_plan_cache_outcome(ops, outcome);
        }
        CacheKind::SqlCompiledCommand => {
            record_global_sql_compiled_command_cache_outcome(ops, outcome);
        }
    }
}

// Shared query-plan cache outcomes update only the query-plan cache family so
// cache dashboards can distinguish planner reuse from SQL command reuse.
#[remain::check]
pub(super) const fn record_global_shared_query_plan_cache_outcome(
    ops: &mut metrics::EventOps,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_shared_query_plan_hits = ops.cache_shared_query_plan_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_shared_query_plan_inserts =
                ops.cache_shared_query_plan_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_shared_query_plan_misses =
                ops.cache_shared_query_plan_misses.saturating_add(1);
        }
    }
}

// SQL compiled-command cache outcomes update only the SQL cache family so the
// same hit/miss vocabulary remains separated by cache owner.
#[remain::check]
pub(super) const fn record_global_sql_compiled_command_cache_outcome(
    ops: &mut metrics::EventOps,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_sql_compiled_command_hits =
                ops.cache_sql_compiled_command_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_sql_compiled_command_inserts =
                ops.cache_sql_compiled_command_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_sql_compiled_command_misses =
                ops.cache_sql_compiled_command_misses.saturating_add(1);
        }
    }
}

// Cache size is a gauge for the current scope, not an event count. Cache owners
// refresh it after lookups and insertions so the metrics report can show memory
// pressure alongside reuse outcomes.
#[remain::check]
pub(super) const fn record_global_cache_entries(
    ops: &mut metrics::EventOps,
    kind: CacheKind,
    entries: u64,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            ops.cache_shared_query_plan_entries = entries;
        }
        CacheKind::SqlCompiledCommand => {
            ops.cache_sql_compiled_command_entries = entries;
        }
    }
}

// Cache miss reasons are scoped below the coarse miss counter. They explain
// whether misses are healthy first-contact behavior or drift in one identity
// dimension without expanding labels by query text or schema fingerprint.
#[remain::check]
pub(super) const fn record_global_cache_miss_reason(
    ops: &mut metrics::EventOps,
    kind: CacheKind,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_global_shared_query_plan_miss_reason(ops, reason);
        }
        CacheKind::SqlCompiledCommand => {
            record_global_sql_compiled_command_miss_reason(ops, reason);
        }
    }
}

// Shared query-plan cache misses cannot vary by SQL surface. If that impossible
// reason reaches this boundary, fold it into the distinct-key bucket rather than
// creating a nonsensical public counter.
#[remain::check]
pub(super) const fn record_global_shared_query_plan_miss_reason(
    ops: &mut metrics::EventOps,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_shared_query_plan_miss_cold =
                ops.cache_shared_query_plan_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Surface => {
            ops.cache_shared_query_plan_miss_distinct_key = ops
                .cache_shared_query_plan_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::MethodVersion => {
            ops.cache_shared_query_plan_miss_method_version = ops
                .cache_shared_query_plan_miss_method_version
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_shared_query_plan_miss_schema_fingerprint = ops
                .cache_shared_query_plan_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Visibility => {
            ops.cache_shared_query_plan_miss_visibility = ops
                .cache_shared_query_plan_miss_visibility
                .saturating_add(1);
        }
    }
}

// SQL compiled-command cache misses cannot vary by planner visibility. Fold
// that impossible reason into distinct-key so the public report stays aligned
// with the cache family's real identity dimensions.
#[remain::check]
pub(super) const fn record_global_sql_compiled_command_miss_reason(
    ops: &mut metrics::EventOps,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_sql_compiled_command_miss_cold =
                ops.cache_sql_compiled_command_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Visibility => {
            ops.cache_sql_compiled_command_miss_distinct_key = ops
                .cache_sql_compiled_command_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::MethodVersion => {
            ops.cache_sql_compiled_command_miss_method_version = ops
                .cache_sql_compiled_command_miss_method_version
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_sql_compiled_command_miss_schema_fingerprint = ops
                .cache_sql_compiled_command_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Surface => {
            ops.cache_sql_compiled_command_miss_surface = ops
                .cache_sql_compiled_command_miss_surface
                .saturating_add(1);
        }
    }
}

// Mirror cache activity to the owning entity so global cache movement can be
// traced back to the model whose schema/query identity produced it.
#[remain::check]
pub(super) const fn record_entity_cache_outcome(
    ops: &mut metrics::EntityCounters,
    kind: CacheKind,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_entity_shared_query_plan_cache_outcome(ops, outcome);
        }
        CacheKind::SqlCompiledCommand => {
            record_entity_sql_compiled_command_cache_outcome(ops, outcome);
        }
    }
}

// Entity-scoped query-plan cache outcomes mirror global counters so one model's
// planner cache churn can be isolated from aggregate cache totals.
#[remain::check]
pub(super) const fn record_entity_shared_query_plan_cache_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_shared_query_plan_hits = ops.cache_shared_query_plan_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_shared_query_plan_inserts =
                ops.cache_shared_query_plan_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_shared_query_plan_misses =
                ops.cache_shared_query_plan_misses.saturating_add(1);
        }
    }
}

// Entity-scoped SQL cache outcomes keep SQL command reuse attributable to the
// entity path that owns the compiled statement context.
#[remain::check]
pub(super) const fn record_entity_sql_compiled_command_cache_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_sql_compiled_command_hits =
                ops.cache_sql_compiled_command_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_sql_compiled_command_inserts =
                ops.cache_sql_compiled_command_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_sql_compiled_command_misses =
                ops.cache_sql_compiled_command_misses.saturating_add(1);
        }
    }
}

// Keep per-entity miss reason buckets aligned with the global cache report so
// one drifting entity can be found without reverse-engineering aggregate totals.
#[remain::check]
pub(super) const fn record_entity_cache_miss_reason(
    ops: &mut metrics::EntityCounters,
    kind: CacheKind,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_entity_shared_query_plan_miss_reason(ops, reason);
        }
        CacheKind::SqlCompiledCommand => {
            record_entity_sql_compiled_command_miss_reason(ops, reason);
        }
    }
}

#[remain::check]
pub(super) const fn record_entity_shared_query_plan_miss_reason(
    ops: &mut metrics::EntityCounters,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_shared_query_plan_miss_cold =
                ops.cache_shared_query_plan_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Surface => {
            ops.cache_shared_query_plan_miss_distinct_key = ops
                .cache_shared_query_plan_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::MethodVersion => {
            ops.cache_shared_query_plan_miss_method_version = ops
                .cache_shared_query_plan_miss_method_version
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_shared_query_plan_miss_schema_fingerprint = ops
                .cache_shared_query_plan_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Visibility => {
            ops.cache_shared_query_plan_miss_visibility = ops
                .cache_shared_query_plan_miss_visibility
                .saturating_add(1);
        }
    }
}

#[remain::check]
pub(super) const fn record_entity_sql_compiled_command_miss_reason(
    ops: &mut metrics::EntityCounters,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_sql_compiled_command_miss_cold =
                ops.cache_sql_compiled_command_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Visibility => {
            ops.cache_sql_compiled_command_miss_distinct_key = ops
                .cache_sql_compiled_command_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::MethodVersion => {
            ops.cache_sql_compiled_command_miss_method_version = ops
                .cache_sql_compiled_command_miss_method_version
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_sql_compiled_command_miss_schema_fingerprint = ops
                .cache_sql_compiled_command_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Surface => {
            ops.cache_sql_compiled_command_miss_surface = ops
                .cache_sql_compiled_command_miss_surface
                .saturating_add(1);
        }
    }
}

// Keep the legacy coarse global plan groups in lockstep with the detailed
// route counters so existing dashboards and newer diagnostics can agree.
#[remain::check]
pub(super) const fn record_global_plan_kind(ops: &mut metrics::EventOps, kind: PlanKind) {
    #[remain::sorted]
    match kind {
        PlanKind::ByKey => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_key = ops.plan_by_key.saturating_add(1);
        }
        PlanKind::ByKeys => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_keys = ops.plan_by_keys.saturating_add(1);
        }
        PlanKind::FullScan => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_explicit_full_scan = ops.plan_explicit_full_scan.saturating_add(1);
        }
        PlanKind::IndexMultiLookup => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_multi_lookup = ops.plan_index_multi_lookup.saturating_add(1);
        }
        PlanKind::IndexPrefix => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_prefix = ops.plan_index_prefix.saturating_add(1);
        }
        PlanKind::IndexRange => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_range = ops.plan_index_range.saturating_add(1);
        }
        PlanKind::Intersection => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_intersection = ops.plan_intersection.saturating_add(1);
        }
        PlanKind::KeyRange => {
            ops.plan_range = ops.plan_range.saturating_add(1);
            ops.plan_key_range = ops.plan_key_range.saturating_add(1);
        }
        PlanKind::Union => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_union = ops.plan_union.saturating_add(1);
        }
    }
}

// Plan choice reasons explain selected non-index and primary-key route families
// at execution time, complementing the coarse route kind counters.
#[remain::check]
pub(super) const fn record_global_plan_choice_reason(
    ops: &mut metrics::EventOps,
    reason: PlanChoiceReason,
) {
    #[remain::sorted]
    match reason {
        PlanChoiceReason::ConflictingPrimaryKeyChildrenAccessPreferred => {
            ops.plan_choice_conflicting_primary_key_children_access_preferred = ops
                .plan_choice_conflicting_primary_key_children_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::ConstantFalsePredicate => {
            ops.plan_choice_constant_false_predicate =
                ops.plan_choice_constant_false_predicate.saturating_add(1);
        }
        PlanChoiceReason::EmptyChildAccessPreferred => {
            ops.plan_choice_empty_child_access_preferred = ops
                .plan_choice_empty_child_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::FullScanAccess => {
            ops.plan_choice_full_scan_access = ops.plan_choice_full_scan_access.saturating_add(1);
        }
        PlanChoiceReason::IntentKeyAccessOverride => {
            ops.plan_choice_intent_key_access_override =
                ops.plan_choice_intent_key_access_override.saturating_add(1);
        }
        PlanChoiceReason::LimitZeroWindow => {
            ops.plan_choice_limit_zero_window = ops.plan_choice_limit_zero_window.saturating_add(1);
        }
        PlanChoiceReason::NonIndexAccess => {
            ops.plan_choice_non_index_access = ops.plan_choice_non_index_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerCompositeNonIndex => {
            ops.plan_choice_planner_composite_non_index = ops
                .plan_choice_planner_composite_non_index
                .saturating_add(1);
        }
        PlanChoiceReason::PlannerFullScanFallback => {
            ops.plan_choice_planner_full_scan_fallback =
                ops.plan_choice_planner_full_scan_fallback.saturating_add(1);
        }
        PlanChoiceReason::PlannerKeySetAccess => {
            ops.plan_choice_planner_key_set_access =
                ops.plan_choice_planner_key_set_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyLookup => {
            ops.plan_choice_planner_primary_key_lookup =
                ops.plan_choice_planner_primary_key_lookup.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyRange => {
            ops.plan_choice_planner_primary_key_range =
                ops.plan_choice_planner_primary_key_range.saturating_add(1);
        }
        PlanChoiceReason::RequiredOrderPrimaryKeyRangePreferred => {
            ops.plan_choice_required_order_primary_key_range_preferred = ops
                .plan_choice_required_order_primary_key_range_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::SingletonPrimaryKeyChildAccessPreferred => {
            ops.plan_choice_singleton_primary_key_child_access_preferred = ops
                .plan_choice_singleton_primary_key_child_access_preferred
                .saturating_add(1);
        }
    }
}

// Grouped plan modes are orthogonal to access shape, so count them beside the
// route counters instead of deriving them from a single access kind.
#[remain::check]
pub(super) const fn record_global_grouped_plan_mode(
    ops: &mut metrics::EventOps,
    grouped_execution_mode: Option<GroupedPlanExecutionMode>,
) {
    #[remain::sorted]
    match grouped_execution_mode {
        None => {}
        Some(GroupedPlanExecutionMode::HashMaterialized) => {
            ops.plan_grouped_hash_materialized =
                ops.plan_grouped_hash_materialized.saturating_add(1);
        }
        Some(GroupedPlanExecutionMode::OrderedMaterialized) => {
            ops.plan_grouped_ordered_materialized =
                ops.plan_grouped_ordered_materialized.saturating_add(1);
        }
    }
}

// Prepared shape finalization sits at the executor authority boundary. Count
// whether a plan arrived with schema-selected static metadata already frozen or
// needed the generated-model fallback at lowering time.
#[remain::check]
pub(super) const fn record_global_prepared_shape_finalization_outcome(
    ops: &mut metrics::EventOps,
    outcome: PreparedShapeFinalizationOutcome,
) {
    #[remain::sorted]
    match outcome {
        PreparedShapeFinalizationOutcome::AlreadyFinalized => {
            ops.prepared_shape_already_finalized =
                ops.prepared_shape_already_finalized.saturating_add(1);
        }
        PreparedShapeFinalizationOutcome::GeneratedFallback => {
            ops.prepared_shape_generated_fallback =
                ops.prepared_shape_generated_fallback.saturating_add(1);
        }
    }
}

// Mirror global plan attribution into the owning entity summary so operators
// can identify which model is causing full scans, unions, or expensive grouped
// routes without correlating separate counters.
#[remain::check]
pub(super) const fn record_entity_plan_kind(ops: &mut metrics::EntityCounters, kind: PlanKind) {
    #[remain::sorted]
    match kind {
        PlanKind::ByKey => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_key = ops.plan_by_key.saturating_add(1);
        }
        PlanKind::ByKeys => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_keys = ops.plan_by_keys.saturating_add(1);
        }
        PlanKind::FullScan => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_explicit_full_scan = ops.plan_explicit_full_scan.saturating_add(1);
        }
        PlanKind::IndexMultiLookup => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_multi_lookup = ops.plan_index_multi_lookup.saturating_add(1);
        }
        PlanKind::IndexPrefix => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_prefix = ops.plan_index_prefix.saturating_add(1);
        }
        PlanKind::IndexRange => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_range = ops.plan_index_range.saturating_add(1);
        }
        PlanKind::Intersection => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_intersection = ops.plan_intersection.saturating_add(1);
        }
        PlanKind::KeyRange => {
            ops.plan_range = ops.plan_range.saturating_add(1);
            ops.plan_key_range = ops.plan_key_range.saturating_add(1);
        }
        PlanKind::Union => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_union = ops.plan_union.saturating_add(1);
        }
    }
}

// Mirror selected route-choice reasons to per-entity summaries so one model's
// fallback behavior is visible without correlating global counters manually.
#[remain::check]
pub(super) const fn record_entity_plan_choice_reason(
    ops: &mut metrics::EntityCounters,
    reason: PlanChoiceReason,
) {
    #[remain::sorted]
    match reason {
        PlanChoiceReason::ConflictingPrimaryKeyChildrenAccessPreferred => {
            ops.plan_choice_conflicting_primary_key_children_access_preferred = ops
                .plan_choice_conflicting_primary_key_children_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::ConstantFalsePredicate => {
            ops.plan_choice_constant_false_predicate =
                ops.plan_choice_constant_false_predicate.saturating_add(1);
        }
        PlanChoiceReason::EmptyChildAccessPreferred => {
            ops.plan_choice_empty_child_access_preferred = ops
                .plan_choice_empty_child_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::FullScanAccess => {
            ops.plan_choice_full_scan_access = ops.plan_choice_full_scan_access.saturating_add(1);
        }
        PlanChoiceReason::IntentKeyAccessOverride => {
            ops.plan_choice_intent_key_access_override =
                ops.plan_choice_intent_key_access_override.saturating_add(1);
        }
        PlanChoiceReason::LimitZeroWindow => {
            ops.plan_choice_limit_zero_window = ops.plan_choice_limit_zero_window.saturating_add(1);
        }
        PlanChoiceReason::NonIndexAccess => {
            ops.plan_choice_non_index_access = ops.plan_choice_non_index_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerCompositeNonIndex => {
            ops.plan_choice_planner_composite_non_index = ops
                .plan_choice_planner_composite_non_index
                .saturating_add(1);
        }
        PlanChoiceReason::PlannerFullScanFallback => {
            ops.plan_choice_planner_full_scan_fallback =
                ops.plan_choice_planner_full_scan_fallback.saturating_add(1);
        }
        PlanChoiceReason::PlannerKeySetAccess => {
            ops.plan_choice_planner_key_set_access =
                ops.plan_choice_planner_key_set_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyLookup => {
            ops.plan_choice_planner_primary_key_lookup =
                ops.plan_choice_planner_primary_key_lookup.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyRange => {
            ops.plan_choice_planner_primary_key_range =
                ops.plan_choice_planner_primary_key_range.saturating_add(1);
        }
        PlanChoiceReason::RequiredOrderPrimaryKeyRangePreferred => {
            ops.plan_choice_required_order_primary_key_range_preferred = ops
                .plan_choice_required_order_primary_key_range_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::SingletonPrimaryKeyChildAccessPreferred => {
            ops.plan_choice_singleton_primary_key_child_access_preferred = ops
                .plan_choice_singleton_primary_key_child_access_preferred
                .saturating_add(1);
        }
    }
}

// Mirror prepared static execution-planning contract authority outcomes to entity summaries so one
// model still using generated fallback can be found from metrics alone.
#[remain::check]
pub(super) const fn record_entity_prepared_shape_finalization_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: PreparedShapeFinalizationOutcome,
) {
    #[remain::sorted]
    match outcome {
        PreparedShapeFinalizationOutcome::AlreadyFinalized => {
            ops.prepared_shape_already_finalized =
                ops.prepared_shape_already_finalized.saturating_add(1);
        }
        PreparedShapeFinalizationOutcome::GeneratedFallback => {
            ops.prepared_shape_generated_fallback =
                ops.prepared_shape_generated_fallback.saturating_add(1);
        }
    }
}

// Grouped execution counters stay per entity for the same reason as access
// route counters: global counts show shape drift, but entity counts show owner.
#[remain::check]
pub(super) const fn record_entity_grouped_plan_mode(
    ops: &mut metrics::EntityCounters,
    grouped_execution_mode: Option<GroupedPlanExecutionMode>,
) {
    #[remain::sorted]
    match grouped_execution_mode {
        None => {}
        Some(GroupedPlanExecutionMode::HashMaterialized) => {
            ops.plan_grouped_hash_materialized =
                ops.plan_grouped_hash_materialized.saturating_add(1);
        }
        Some(GroupedPlanExecutionMode::OrderedMaterialized) => {
            ops.plan_grouped_ordered_materialized =
                ops.plan_grouped_ordered_materialized.saturating_add(1);
        }
    }
}
