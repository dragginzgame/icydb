use crate::{
    db::{
        CommitApplyGuard, CommitGuard, CommitRowOp, Db, PreparedRowCommitOp, finish_commit,
        prepare_row_commit_for_entity, rollback_prepared_row_ops_reverse, snapshot_row_rollback,
    },
    error::InternalError,
    obs::sink::{self, MetricsEvent},
    traits::{EntityKind, EntityValue},
};

///
/// PreparedRowOpDelta
///
/// Aggregated mutation deltas from preflight-prepared row operations.
/// Used by save/delete executors to emit consistent metrics without duplicating
/// per-field folding logic.
///
pub(super) struct PreparedRowOpDelta {
    pub(super) rows_touched: usize,
    pub(super) index_inserts: usize,
    pub(super) index_removes: usize,
    pub(super) reverse_index_inserts: usize,
    pub(super) reverse_index_removes: usize,
}

/// Aggregate index and reverse-index deltas across prepared row operations.
#[must_use]
pub(super) fn summarize_prepared_row_ops(
    prepared_row_ops: &[PreparedRowCommitOp],
) -> PreparedRowOpDelta {
    let mut summary = PreparedRowOpDelta {
        rows_touched: prepared_row_ops.len(),
        index_inserts: 0,
        index_removes: 0,
        reverse_index_inserts: 0,
        reverse_index_removes: 0,
    };

    for row_op in prepared_row_ops {
        summary.index_inserts = summary
            .index_inserts
            .saturating_add(row_op.index_insert_count);
        summary.index_removes = summary
            .index_removes
            .saturating_add(row_op.index_remove_count);
        summary.reverse_index_inserts = summary
            .reverse_index_inserts
            .saturating_add(row_op.reverse_index_insert_count);
        summary.reverse_index_removes = summary
            .reverse_index_removes
            .saturating_add(row_op.reverse_index_remove_count);
    }

    summary
}

/// Emit index and reverse-index metrics from one prepared-row delta aggregate.
pub(super) fn emit_prepared_row_op_delta_metrics<E: EntityKind>(delta: &PreparedRowOpDelta) {
    emit_index_delta_metrics::<E>(
        delta.index_inserts,
        delta.index_removes,
        delta.reverse_index_inserts,
        delta.reverse_index_removes,
    );
}

/// Emit index and reverse-index delta metrics with saturated diagnostics counts.
pub(super) fn emit_index_delta_metrics<E: EntityKind>(
    index_inserts: usize,
    index_removes: usize,
    reverse_index_inserts: usize,
    reverse_index_removes: usize,
) {
    sink::record(MetricsEvent::IndexDelta {
        entity_path: E::PATH,
        inserts: u64::try_from(index_inserts).unwrap_or(u64::MAX),
        removes: u64::try_from(index_removes).unwrap_or(u64::MAX),
    });

    sink::record(MetricsEvent::ReverseIndexDelta {
        entity_path: E::PATH,
        inserts: u64::try_from(reverse_index_inserts).unwrap_or(u64::MAX),
        removes: u64::try_from(reverse_index_removes).unwrap_or(u64::MAX),
    });
}

/// Prepare row ops for commit-time apply by simulating sequential execution.
///
/// This preflight ensures later row ops are prepared against the state produced
/// by earlier row ops, then restores the original state before returning.
pub(super) fn preflight_prepare_row_ops<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: &[CommitRowOp],
) -> Result<Vec<PreparedRowCommitOp>, InternalError> {
    let mut prepared = Vec::with_capacity(row_ops.len());
    let mut rollback = Vec::with_capacity(row_ops.len());

    for row_op in row_ops {
        let row = match prepare_row_commit_for_entity::<E>(db, row_op) {
            Ok(op) => op,
            Err(err) => {
                rollback_prepared_row_ops_reverse(rollback);
                return Err(err);
            }
        };
        rollback.push(snapshot_row_rollback(&row));
        row.clone().apply();
        prepared.push(row);
    }

    rollback_prepared_row_ops_reverse(rollback);
    Ok(prepared)
}

/// Apply prepared row ops under the shared commit-window guard.
pub(super) fn apply_prepared_row_ops(
    commit: CommitGuard,
    apply_phase: &'static str,
    prepared_row_ops: Vec<PreparedRowCommitOp>,
    on_index_applied: impl FnOnce(),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    finish_commit(commit, |guard| {
        let mut apply_guard = CommitApplyGuard::new(apply_phase);
        let _ = guard;

        let mut rollback = Vec::with_capacity(prepared_row_ops.len());
        for row_op in &prepared_row_ops {
            rollback.push(snapshot_row_rollback(row_op));
        }
        apply_guard.record_rollback(move || rollback_prepared_row_ops_reverse(rollback));

        for row_op in prepared_row_ops {
            row_op.apply();
        }
        on_index_applied();
        on_data_applied();
        apply_guard.finish()?;

        Ok(())
    })
}
