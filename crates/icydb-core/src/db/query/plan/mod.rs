//! Plan module wiring; must not implement planning or validation logic.

pub mod canonical;
pub(crate) mod executable;
pub mod explain;
pub mod fingerprint;
pub(crate) mod logical;
pub mod planner;
#[cfg(test)]
mod tests;
mod types;
pub mod validate;

///
/// Re-Exports
///
pub use executable::ExecutablePlan;
pub use explain::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainOrder, ExplainOrderBy, ExplainPagination,
    ExplainPlan, ExplainPredicate,
};
pub use fingerprint::PlanFingerprint;
pub(crate) use logical::LogicalPlan;
pub use types::OrderDirection;
pub(crate) use types::{AccessPath, AccessPlan, DeleteLimitSpec, OrderSpec, PageSpec};
pub use validate::PlanError;
