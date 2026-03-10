//! Module: patch::merge
//! Responsibility: module-local ownership and contracts for patch::merge.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod error;
mod list;
mod map;
mod scalar;
mod set;

pub use error::MergePatchError;

pub(crate) use list::merge_vec;
pub(crate) use map::{merge_btree_map, merge_hash_map};
pub(crate) use scalar::{merge_atomic, merge_option};
pub(crate) use set::{merge_btree_set, merge_hash_set};
