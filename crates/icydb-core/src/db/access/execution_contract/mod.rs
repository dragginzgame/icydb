//! Module: db::access::execution_contract
//! Responsibility: shared normalized access contracts consumed by query/cursor/executor.
//! Does not own: logical access-path selection policy.
//! Boundary: planner lowers `AccessPlan`/`AccessPath` into these execution mechanics.

mod executable;
mod summary;
#[cfg(test)]
mod tests;
mod types;

pub(in crate::db) use executable::{ExecutableAccessNode, ExecutableAccessPlan};
pub(in crate::db) use summary::summarize_executable_access_plan;
pub(in crate::db) use types::ExecutionPathPayload;
