//! Module: executor::stream
//! Responsibility: ordered key-stream primitives and physical access-stream boundaries.
//! Does not own: planning semantics or row materialization policy.
//! Boundary: shared key-stream infrastructure consumed by executor load routes.

pub(super) mod access;
mod flat_merge;
pub(super) mod key;
mod prefix_set;

pub(in crate::db::executor) use flat_merge::{
    FlatMergeOrderedChild, FlatMergeSiblingSet, FlatMergeStream,
};
pub(in crate::db::executor) use prefix_set::{PrefixSetExecutionShape, PrefixSetMergeSafety};
