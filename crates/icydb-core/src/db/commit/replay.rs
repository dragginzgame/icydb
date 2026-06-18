//! Module: db::commit::replay
//! Responsibility: replay persisted row operations in canonical marker order.
//! Does not own: marker persistence, secondary-index full rebuild, or mutation planning policy.
//! Boundary: commit::recovery -> commit::replay -> commit::{prepare,apply} (one-way).

use crate::{
    db::{Db, commit::CommitRowOp, registry::StoreRecoveryCapability},
    error::InternalError,
    traits::CanisterKind,
};

/// Reject obsolete marker row ops for durable stores.
///
/// Current durable stores recover through journal batches. Heap stores are
/// live-only and ignore durable marker row ops.
pub(in crate::db) fn replay_commit_marker_row_ops(
    db: &Db<impl CanisterKind>,
    row_ops: &[CommitRowOp],
) -> Result<(), InternalError> {
    for row_op in row_ops {
        match row_op_recovery_capability(db, row_op)? {
            StoreRecoveryCapability::StableBasePlusJournalReplay => {
                return Err(InternalError::store_unsupported());
            }
            StoreRecoveryCapability::None => {}
        }
    }

    Ok(())
}

fn row_op_recovery_capability(
    db: &Db<impl CanisterKind>,
    row_op: &CommitRowOp,
) -> Result<StoreRecoveryCapability, InternalError> {
    let hooks = db.runtime_hook_for_entity_path(row_op.entity_path.as_ref())?;
    let handle = db.store_handle(hooks.store_path)?;

    Ok(handle.storage_capabilities().recovery())
}
