mod error;
mod list;
mod map;
mod scalar;
mod set;

pub use crate::patch::merge::{
    error::MergePatchError,
    list::merge_vec,
    map::{merge_btree_map, merge_hash_map},
    scalar::{merge_atomic, merge_option},
    set::{merge_btree_set, merge_hash_set},
};
