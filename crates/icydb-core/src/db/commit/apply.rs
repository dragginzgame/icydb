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
    pub(crate) delta_kind: PreparedIndexDeltaKind,
}

///
/// PreparedIndexDeltaKind
///
/// Logical mutation-class annotation used for commit-window delta aggregation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedIndexDeltaKind {
    None,
    IndexInsert,
    IndexRemove,
    ReverseIndexInsert,
    ReverseIndexRemove,
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
}

impl PreparedRowCommitOp {
    /// Apply the prepared row operation infallibly.
    pub(crate) fn apply(self) {
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
