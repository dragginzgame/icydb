//! Plan module wiring; must not implement planning or validation logic.

pub mod canonical;
pub(crate) mod executable;
pub mod explain;
pub mod fingerprint;
mod invariants;
pub(crate) mod logical;
pub mod planner;
mod types;
pub mod validate;

pub use executable::ExecutablePlan;
pub use explain::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainOrder, ExplainOrderBy, ExplainPagination,
    ExplainPlan, ExplainPredicate, ExplainProjection,
};
pub use fingerprint::PlanFingerprint;
pub(crate) use invariants::validate_plan_invariants;
pub(crate) use logical::LogicalPlan;
pub use types::OrderDirection;
pub(crate) use types::{
    AccessPath, AccessPlan, DeleteLimitSpec, OrderSpec, PageSpec, ProjectionSpec,
};
pub use validate::PlanError;

#[doc(hidden)]
pub mod __internal {
    pub use crate::db::query::plan::executable::ExecutablePlanErased;
}
