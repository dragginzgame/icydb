use crate::{
    db::{
        CommitApplyGuard, CommitGuard, CommitRowOp, Db, PreparedRowCommitOp, finish_commit,
        prepare_row_commit_for_entity, snapshot_row_rollback,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

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
                rollback_prepared_row_ops(rollback);
                return Err(err);
            }
        };
        rollback.push(snapshot_row_rollback(&row));
        row.clone().apply();
        prepared.push(row);
    }

    rollback_prepared_row_ops(rollback);
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
        apply_guard.record_rollback(move || rollback_prepared_row_ops(rollback));

        for row_op in prepared_row_ops {
            row_op.apply();
        }
        on_index_applied();
        on_data_applied();
        apply_guard.finish()?;

        Ok(())
    })
}

/// Apply row rollback operations in reverse write order.
fn rollback_prepared_row_ops(ops: Vec<PreparedRowCommitOp>) {
    for op in ops.into_iter().rev() {
        op.apply();
    }
}
