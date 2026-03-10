//! Module: db::executor::load::execute::contracts
//! Responsibility: module-local ownership and contracts for db::executor::load::execute::contracts.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod inputs;
mod outcomes;
mod stream;

pub(in crate::db::executor) use inputs::{ExecutionInputs, ExecutionInputsProjection};
pub(in crate::db::executor) use outcomes::{ExecutionOutcomeMetrics, MaterializedExecutionAttempt};
pub(in crate::db::executor) use stream::ResolvedExecutionKeyStream;
