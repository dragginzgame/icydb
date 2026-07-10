//! SQL compile and write counter mutation helpers.
//! Does not own SQL policy, execution, or metrics event dispatch.

use crate::{
    error::ErrorClass,
    metrics::{
        sink::{SqlCompileRejectPhase, SqlWriteKind},
        state as metrics,
    },
};

#[remain::check]
pub(in crate::metrics::sink) const fn record_global_sql_write_kind(
    ops: &mut metrics::EventOps,
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

#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_sql_write_kind(
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

// SQL compile rejects are counted before execution/write metrics because they
// represent SQL admission failures. The phase bucket intentionally stays broad
// to avoid leaking SQL text or semantic diagnostics through metrics.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_sql_compile_reject_phase(
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
pub(in crate::metrics::sink) const fn record_entity_sql_compile_reject_phase(
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
pub(in crate::metrics::sink) const fn record_global_sql_write_error_kind(
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
pub(in crate::metrics::sink) const fn record_entity_sql_write_error_kind(
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
pub(in crate::metrics::sink) const fn record_global_sql_write_error_class(
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
pub(in crate::metrics::sink) const fn record_entity_sql_write_error_class(
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
