//! Plan module wiring; must not implement planning or validation logic.

pub mod access;
pub(crate) mod cache;
pub mod canonical;
pub mod explain;
pub mod fingerprint;
mod invariants;
pub mod logical;
pub mod planner;
pub mod types;
pub mod validate;

pub use explain::{
    ExplainAccessPath, ExplainOrder, ExplainOrderBy, ExplainPagination, ExplainPlan,
    ExplainPredicate,
};
pub use fingerprint::PlanFingerprint;
pub(crate) use invariants::validate_plan_invariants;
pub use logical::LogicalPlan;
pub use types::{AccessPath, AccessPlan, OrderDirection, OrderSpec, PageSpec};
pub use validate::PlanError;
pub(crate) use validate::validate_plan_with_model;

#[cfg(debug_assertions)]
#[must_use]
/// Expose plan cache stats in debug builds only.
pub fn plan_cache_stats() -> cache::CacheStats {
    cache::stats()
}
