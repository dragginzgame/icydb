//! Module: executor::mutation
//! Responsibility: mutation execution preflight and shared commit-window entry helpers.
//! Does not own: relation semantics or logical-plan construction.
//! Boundary: write-path setup shared by save/delete executors.

pub(super) mod commit_window;
mod save_validation;

pub(in crate::db) use commit_window::{
    commit_delete_row_ops_with_window_for_path, commit_structural_save_row_ops_with_window_for_path,
};
pub(in crate::db) use save_validation::validate_structural_accepted_after_image;
