//! v2 plan module wiring; must not implement planning or validation logic.

pub mod access;
pub mod canonical;
mod invariants;
pub mod logical;
pub mod planner;
pub mod types;
pub mod validate;

pub(crate) use invariants::validate_plan_invariants;
pub use logical::LogicalPlan;
pub use planner::plan_access;
pub use types::{AccessPath, AccessPlan, OrderDirection, OrderSpec, PageSpec};
pub use validate::{PlanError, validate_plan};
