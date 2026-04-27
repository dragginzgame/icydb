//! Module: executor::aggregate::runtime::grouped_fold::generic
//! Responsibility: generic grouped reducer execution.
//! Boundary: exports the runner entrypoint for grouped fold orchestration.

mod runner;

pub(super) use runner::execute_generic_grouped_fold_stage;
