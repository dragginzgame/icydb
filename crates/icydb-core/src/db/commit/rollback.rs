//! Module: commit::rollback
//! Responsibility: capture and apply rollback snapshots for prepared row operations.
//! Does not own: commit-marker persistence, mutation planning, or recovery orchestration.
//! Boundary: commit::{prepare,replay,executor} -> commit::rollback -> commit::apply.

use crate::db::{
    commit::{PreparedIndexMutation, PreparedRowCommitOp},
    data::canonical_row_from_stored_raw_row,
};

impl PreparedRowCommitOp {
    /// Capture the current store state needed to roll back this prepared row op.
    ///
    /// The returned op writes the prior index/data values back when applied.
    #[must_use]
    pub(crate) fn snapshot_rollback(&self) -> Self {
        // Phase 1: snapshot all index keys touched by the prepared operation.
        let mut index_ops = Vec::with_capacity(self.index_ops.len());
        for index_op in &self.index_ops {
            let existing = index_op
                .index_store
                .with_borrow(|store| store.get(&index_op.key));
            index_ops.push(PreparedIndexMutation::rollback_snapshot(
                index_op.index_store,
                index_op.key.clone(),
                existing,
            ));
        }

        // Phase 2: snapshot the row-store value for the target primary key.
        let data_value = self
            .data_store
            .with_borrow(|store| store.get(&self.data_key))
            .map(canonical_row_from_stored_raw_row);

        Self {
            index_ops,
            data_store: self.data_store,
            data_key: self.data_key,
            data_value,
        }
    }

    /// Capture only row-store state needed to roll back this prepared row op.
    ///
    /// Recovery replay applies row mutations only and rebuilds indexes in a
    /// dedicated phase, so replay rollback snapshots should remain row-scoped.
    #[must_use]
    pub(crate) fn snapshot_row_only_rollback(&self) -> Self {
        // Recovery row-replay rollback does not touch index stores; rebuild owns those.
        let data_value = self
            .data_store
            .with_borrow(|store| store.get(&self.data_key))
            .map(canonical_row_from_stored_raw_row);

        Self {
            index_ops: Vec::new(),
            data_store: self.data_store,
            data_key: self.data_key,
            data_value,
        }
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
