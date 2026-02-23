use crate::db::{
    data::{DataStore, RawDataKey, RawRow},
    index::{IndexStore, RawIndexEntry, RawIndexKey},
};
use std::{cell::RefCell, thread::LocalKey};

///
/// PreparedIndexMutation
///
/// Mechanical index mutation derived from a row op.
///

#[derive(Clone)]
pub(crate) struct PreparedIndexMutation {
    pub(crate) store: &'static LocalKey<RefCell<IndexStore>>,
    pub(crate) key: RawIndexKey,
    pub(crate) value: Option<RawIndexEntry>,
}

///
/// PreparedRowCommitOp
///
/// Mechanical store mutation derived from one row op.
///

#[derive(Clone)]
pub(in crate::db) struct PreparedRowCommitOp {
    pub(crate) index_ops: Vec<PreparedIndexMutation>,
    pub(crate) data_store: &'static LocalKey<RefCell<DataStore>>,
    pub(crate) data_key: RawDataKey,
    pub(crate) data_value: Option<RawRow>,
    pub(crate) index_remove_count: usize,
    pub(crate) index_insert_count: usize,
    pub(crate) reverse_index_remove_count: usize,
    pub(crate) reverse_index_insert_count: usize,
}

impl PreparedRowCommitOp {
    /// Debug-only sanity checks for logical delta counters carried with this op.
    ///
    /// Counters represent logical insert/remove deltas, not raw write-op count.
    pub(crate) fn debug_assert_delta_count_bounds(&self) {
        let index_op_count = self.index_ops.len();
        let total_delta_count = self
            .index_remove_count
            .saturating_add(self.index_insert_count)
            .saturating_add(self.reverse_index_remove_count)
            .saturating_add(self.reverse_index_insert_count);

        debug_assert!(
            self.index_remove_count <= index_op_count,
            "index_remove_count exceeds prepared index-op count: removes={} ops={index_op_count}",
            self.index_remove_count
        );
        debug_assert!(
            self.index_insert_count <= index_op_count,
            "index_insert_count exceeds prepared index-op count: inserts={} ops={index_op_count}",
            self.index_insert_count
        );
        debug_assert!(
            self.reverse_index_remove_count <= index_op_count,
            "reverse_index_remove_count exceeds prepared index-op count: removes={} ops={index_op_count}",
            self.reverse_index_remove_count
        );
        debug_assert!(
            self.reverse_index_insert_count <= index_op_count,
            "reverse_index_insert_count exceeds prepared index-op count: inserts={} ops={index_op_count}",
            self.reverse_index_insert_count
        );
        debug_assert!(
            total_delta_count <= index_op_count,
            "logical delta counters exceed prepared index-op envelope: deltas={total_delta_count} ops={index_op_count}",
        );
    }

    /// Apply the prepared row operation infallibly.
    pub(crate) fn apply(self) {
        self.debug_assert_delta_count_bounds();

        for index_op in self.index_ops {
            index_op.store.with_borrow_mut(|store| {
                if let Some(value) = index_op.value {
                    store.insert(index_op.key, value);
                } else {
                    store.remove(&index_op.key);
                }
            });
        }

        self.data_store.with_borrow_mut(|store| {
            if let Some(value) = self.data_value {
                store.insert(self.data_key, value);
            } else {
                store.remove(&self.data_key);
            }
        });
    }
}
