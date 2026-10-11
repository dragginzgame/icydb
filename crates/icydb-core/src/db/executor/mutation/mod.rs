//! Module: executor::mutation
//! Responsibility: mutation execution preflight and shared commit-window entry helpers.
//! Does not own: relation semantics or logical-plan construction.
//! Boundary: write-path setup shared by save/delete executors.

pub(super) mod commit_window;
mod constraint_scheduler;
#[cfg(feature = "sql")]
pub(in crate::db) mod job;
pub(in crate::db) mod structural;

pub(in crate::db) use commit_window::{
    MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT,
    commit_structural_row_ops_with_mutation_progress, commit_structural_row_ops_with_window,
};
#[cfg(test)]
pub(in crate::db) use commit_window::{
    MutationCommitInterruption, interrupt_next_mutation_commit_for_tests,
};
pub(in crate::db) use constraint_scheduler::{
    AcceptedMutationConstraintContext, AcceptedMutationConstraintScheduler,
};
