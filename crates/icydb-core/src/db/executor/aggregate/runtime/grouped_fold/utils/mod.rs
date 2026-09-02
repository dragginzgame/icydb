//! Module: executor::aggregate::runtime::grouped_fold::utils
//! Responsibility: shared grouped-count hashing, equality, and boundary helpers.
//! Boundary: keeps hot-path primitives centralized for count and generic folds.

mod boundary;
mod bucket;
mod equality;
mod hashing;
mod path_key;

pub(super) use boundary::{
    compare_grouped_boundary_values, grouped_next_cursor_boundary,
    grouped_resume_boundary_allows_candidate,
};
pub(super) use bucket::GroupIndexBucket;
pub(super) use equality::{
    find_matching_group_index, find_matching_group_index_in_bucket,
    find_matching_single_group_value_index, group_key_matches_row_view,
    group_key_matches_single_group_value,
};
pub(super) use hashing::{stable_hash_group_values_from_row_view, stable_hash_single_group_value};
pub(super) use path_key::{
    group_key_matches_path_group_values, materialize_path_group_key, resolve_group_field_value,
    stable_hash_path_group_values,
};
