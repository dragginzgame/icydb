//! Executor metrics counter mutation helpers.
//! Does not own event dispatch or executor instrumentation lifetimes.

use crate::metrics::{
    sink::{ExecKind, ExecOutcome},
    state as metrics,
};

// Start counters are used for ordinary spans and for load errors that return
// before the successful load finalizers create their normal span.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_exec_start(
    ops: &mut metrics::EventOps,
    kind: ExecKind,
) {
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
pub(in crate::metrics::sink) const fn record_entity_exec_start(
    ops: &mut metrics::EntityCounters,
    kind: ExecKind,
) {
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
pub(in crate::metrics::sink) const fn record_global_exec_outcome(
    ops: &mut metrics::EventOps,
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

// Mirror outcome attribution into entity summaries so failed operations can be
// correlated with the model that owned the executor span.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_exec_outcome(
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
