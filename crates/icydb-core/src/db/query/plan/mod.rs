//! Plan module wiring; must not implement planning or validation logic.

pub(crate) mod cache;
pub mod canonical;
pub mod executable;
pub mod explain;
pub mod fingerprint;
mod invariants;
pub(crate) mod logical;
pub mod planner;
mod types;
pub mod validate;

pub use executable::{ExecutablePlan, ExecutablePlanErased};
pub use explain::{
    ExplainAccessPath, ExplainOrder, ExplainOrderBy, ExplainPagination, ExplainPlan,
    ExplainPredicate, ExplainProjection,
};
pub use fingerprint::PlanFingerprint;
pub(crate) use invariants::validate_plan_invariants;
pub(crate) use logical::LogicalPlan;
pub use types::OrderDirection;
pub(crate) use types::{AccessPath, AccessPlan, OrderSpec, PageSpec, ProjectionSpec};
pub use validate::PlanError;

#[cfg(debug_assertions)]
#[must_use]
/// Expose plan cache stats in debug builds only.
pub fn plan_cache_stats() -> cache::CacheStats {
    cache::stats()
}
