//! Module: executor::aggregate::runtime
//! Responsibility: aggregate-owned grouped runtime mechanics for read execution.
//! Does not own: grouped route derivation or shared executor contracts.
//! Boundary: grouped fold/distinct/having/output execution for grouped read paths.

mod grouped_distinct;
mod grouped_fold;
mod grouped_having;
mod grouped_output;

pub(in crate::db::executor) use grouped_fold::execute_group_fold_stage;
