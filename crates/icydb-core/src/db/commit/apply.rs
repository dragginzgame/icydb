//! Module: commit::apply
//! Responsibility: apply precomputed row/index mutations to stores.
//! Does not own: mutation preparation, commit-marker durability, or recovery orchestration.
//! Boundary: commit::{prepared_op,prepare,rebuild,replay} -> commit::apply (one-way).

use crate::db::commit::PreparedRowCommitOp;

impl PreparedRowCommitOp {
    /// Apply the prepared row operation infallibly.
    pub(crate) fn apply(self) {
        // Phase 1: apply all index mutations first so rollback snapshots can
        // mirror this order exactly in reverse.
        for index_op in self.index_ops {
            index_op.store.with_borrow_mut(|store| {
                if let Some(value) = index_op.value {
                    store.insert(index_op.key, value);
                } else {
                    store.remove(&index_op.key);
                }
            });
        }

        // Phase 2: apply the authoritative row-store mutation.
        self.data_store.with_borrow_mut(|store| {
            if let Some(value) = self.data_value {
                store.insert(self.data_key, value);
            } else {
                store.remove(&self.data_key);
            }
        });
    }

    /// Apply only the prepared row-store mutation.
    ///
    /// Recovery replay uses this path so row-store durability remains
    /// authoritative while secondary indexes are rebuilt in a dedicated phase.
    pub(crate) fn apply_row_only(self) {
        self.data_store.with_borrow_mut(|store| {
            if let Some(value) = self.data_value {
                store.insert(self.data_key, value);
            } else {
                store.remove(&self.data_key);
            }
        });
    }
}
