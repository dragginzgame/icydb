//! Module: commit::rollback
//! Responsibility: capture and apply rollback snapshots for prepared row operations.
//! Does not own: commit-marker persistence, mutation planning, or recovery orchestration.
//! Boundary: commit::{prepare,replay,executor} -> commit::rollback -> commit::apply.

use crate::db::commit::{PreparedIndexDeltaKind, PreparedIndexMutation, PreparedRowCommitOp};

/// Capture the current store state needed to roll back one prepared row op.
///
/// The returned op writes the prior index/data values back when applied.
#[must_use]
pub(crate) fn snapshot_row_rollback(op: &PreparedRowCommitOp) -> PreparedRowCommitOp {
    // Phase 1: snapshot all index keys touched by the prepared operation.
    let mut index_ops = Vec::with_capacity(op.index_ops.len());
    for index_op in &op.index_ops {
        let existing = index_op.store.with_borrow(|store| store.get(&index_op.key));
        index_ops.push(PreparedIndexMutation {
            store: index_op.store,
            key: index_op.key.clone(),
            value: existing,
            delta_kind: PreparedIndexDeltaKind::None,
        });
    }

    // Phase 2: snapshot the row-store value for the target primary key.
    let data_value = op.data_store.with_borrow(|store| store.get(&op.data_key));

    PreparedRowCommitOp {
        index_ops,
        data_store: op.data_store,
        data_key: op.data_key,
        data_value,
    }
}

/// Capture only row-store state needed to roll back one prepared row op.
///
/// Recovery replay applies row mutations only and rebuilds indexes in a
/// dedicated phase, so replay rollback snapshots should remain row-scoped.
#[must_use]
pub(crate) fn snapshot_row_only_rollback(op: &PreparedRowCommitOp) -> PreparedRowCommitOp {
    // Recovery row-replay rollback does not touch index stores; rebuild owns those.
    let data_value = op.data_store.with_borrow(|store| store.get(&op.data_key));

    PreparedRowCommitOp {
        index_ops: Vec::new(),
        data_store: op.data_store,
        data_key: op.data_key,
        data_value,
    }
}

/// Apply prepared-row rollback operations in reverse write order.
///
/// This is shared by preflight/recovery paths so rollback ordering remains
/// mechanically consistent across commit-related execution phases.
pub(crate) fn rollback_prepared_row_ops_reverse(ops: Vec<PreparedRowCommitOp>) {
    // Reverse order mirrors forward apply order and preserves overwrite semantics.
    for op in ops.into_iter().rev() {
        op.apply();
    }
}
