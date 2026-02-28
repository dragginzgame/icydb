//! Module: commit::prepared_op
//! Responsibility: define mechanical prepared commit mutation payloads.
//! Does not own: mutation planning, store apply sequencing, or recovery orchestration.
//! Boundary: commit::{prepare,relation,executor} -> commit::prepared_op -> commit::apply.

use crate::db::{
    data::{DataStore, RawDataKey, RawRow},
    index::{IndexStore, RawIndexEntry, RawIndexKey},
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
    pub(crate) store: &'static LocalKey<RefCell<IndexStore>>,
    pub(crate) key: RawIndexKey,
    pub(crate) value: Option<RawIndexEntry>,
    pub(crate) delta_kind: PreparedIndexDeltaKind,
}

///
/// PreparedIndexDeltaKind
///
/// Logical mutation-class annotation used for commit-window delta aggregation.
/// This is observability metadata and must not affect mutation semantics.
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
/// Mechanical store mutation derived from one commit-marker row op.
/// Invariant: `index_ops` and `data_*` fields represent one atomic row transition.
///

#[derive(Clone)]
pub(in crate::db) struct PreparedRowCommitOp {
    pub(crate) index_ops: Vec<PreparedIndexMutation>,
    pub(crate) data_store: &'static LocalKey<RefCell<DataStore>>,
    pub(crate) data_key: RawDataKey,
    pub(crate) data_value: Option<RawRow>,
}
