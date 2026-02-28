//! Module: commit::replay
//! Responsibility: replay persisted row operations in canonical marker order.
//! Does not own: marker persistence, secondary-index full rebuild, or mutation planning policy.
//! Boundary: commit::recovery -> commit::replay -> commit::{prepare,apply} (one-way).

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedRowCommitOp, rollback_prepared_row_ops_reverse,
            snapshot_row_only_rollback,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};

/// Replay marker row ops in order, rolling back on any preparation error.
///
/// Sequential replay is required for correctness when multiple row ops
/// touch the same data row in one marker.
///
/// Recovery replay applies row-store mutations only; secondary indexes are
/// rebuilt from authoritative rows in a separate phase.
pub(in crate::db) fn replay_commit_marker_row_ops(
    db: &Db<impl CanisterKind>,
    row_ops: &[CommitRowOp],
) -> Result<(), InternalError> {
    let mut rollbacks = Vec::<PreparedRowCommitOp>::with_capacity(row_ops.len());

    // Phase 1: prepare + apply row-only mutations sequentially so repeated
    // writes to one key in one marker replay in canonical marker order.
    for row_op in row_ops {
        let prepared = match db.prepare_row_commit_op(row_op) {
            Ok(op) => op,
            Err(err) => {
                // Phase 2: fail closed by restoring prior row-store values.
                rollback_prepared_row_ops_reverse(rollbacks);
                return Err(err);
            }
        };

        rollbacks.push(snapshot_row_only_rollback(&prepared));
        prepared.apply_row_only();
    }

    Ok(())
}
