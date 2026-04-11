//! Module: db::access::execution_contract
//! Responsibility: shared normalized access contracts consumed by query/cursor/executor.
//! Does not own: logical access-path selection policy.
//! Boundary: planner lowers `AccessPlan`/`AccessPath` into these execution mechanics.

mod executable;
mod pushdown;
mod route;
mod strategy;
mod summary;
#[cfg(test)]
mod tests;
mod types;

pub(in crate::db) use executable::{
    ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
};
pub(in crate::db) use route::AccessRouteClass;
pub(in crate::db) use strategy::AccessStrategy;
pub(in crate::db) use types::{
    AccessPathExecutionKind, ExecutionBounds, ExecutionDistinctMode, ExecutionOrdering,
    ExecutionPathPayload,
};
