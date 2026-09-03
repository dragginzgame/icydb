//! Module: db::commit::prepared_op
//! Responsibility: define mechanical prepared commit mutation payloads.
//! Does not own: mutation planning, store apply sequencing, or recovery orchestration.
//! Boundary: commit::{prepare,relation,executor} -> commit::prepared_op -> commit::apply.

use crate::db::{
    data::{CanonicalRow, DataStore, RawDataStoreKey},
    index::{IndexEntryValue, IndexStore, RawIndexStoreKey},
};
use std::{cell::RefCell, thread::LocalKey};

///
/// PreparedIndexMutation
///
/// Mechanical index mutation derived from one prepared row operation.
/// Invariant: all payload bytes are already validated and ready for infallible apply.
///

#[derive(Clone)]
pub(crate) struct PreparedIndexMutation {
    pub(crate) index_store: &'static LocalKey<RefCell<IndexStore>>,
    pub(crate) key: RawIndexStoreKey,
    pub(crate) value: Option<IndexEntryValue>,
}

impl PreparedIndexMutation {
    /// Build one prepared index mutation.
    pub(crate) const fn new(
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: RawIndexStoreKey,
        value: Option<IndexEntryValue>,
    ) -> Self {
        Self {
            index_store,
            key,
            value,
        }
    }
}

///
/// PreparedRowCommitOp
///
/// Mechanical store mutation derived from one commit-marker row op.
/// Invariant: `index_ops` and `data_*` fields represent one atomic row transition.
///

#[derive(Clone)]
pub(in crate::db) struct PreparedRowCommitOp {
    pub(crate) index_ops: Vec<PreparedIndexMutation>,
    pub(crate) data_store: &'static LocalKey<RefCell<DataStore>>,
    pub(crate) data_index_store: &'static LocalKey<RefCell<IndexStore>>,
    pub(crate) data_key: RawDataStoreKey,
    pub(crate) data_value: Option<CanonicalRow>,
}
