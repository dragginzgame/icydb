//! Module: executor::mutation
//! Responsibility: mutation execution preflight and shared commit-window entry helpers.
//! Does not own: relation semantics or logical-plan construction.
//! Boundary: write-path setup shared by save/delete executors.

pub(super) mod commit_window;
mod constraint_scheduler;

#[cfg(test)]
pub(in crate::db) use commit_window::{
    IdentityCommitInterruption, interrupt_next_identity_commit_for_tests,
};
pub(in crate::db) use commit_window::{
    commit_delete_row_ops_with_window_for_path, commit_structural_save_row_ops_with_window_for_path,
};
pub(in crate::db) use constraint_scheduler::AcceptedMutationConstraintScheduler;
