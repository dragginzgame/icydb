//! Plan module wiring; must not implement planning or validation logic.

mod access_projection;
pub(crate) mod executable;
mod index_bounds;
pub(crate) mod logical;
pub(crate) mod planner;
#[cfg(test)]
mod tests;
mod types;
pub(crate) mod validate;

pub(in crate::db) use crate::db::index::KeyEnvelope;
pub(in crate::db) use crate::db::{index::Direction, query::fingerprint::canonical};
pub(crate) use access_projection::{
    AccessPlanProjection, project_access_plan, project_explain_access_path,
};
///
/// Re-Exports
///
pub(in crate::db) use executable::{
    ExecutablePlan, IndexPrefixSpec, IndexRangeSpec, PlannedCursor,
};
pub(in crate::db) use index_bounds::raw_bounds_for_semantic_index_component_range;
pub(crate) use logical::LogicalPlan;
pub use types::OrderDirection;
pub(in crate::db) use types::derive_scan_direction;
pub(crate) use types::{
    AccessPath, AccessPlan, DeleteLimitSpec, OrderSpec, PageSpec, SlotSelectionPolicy,
    compute_page_window,
};
pub use validate::PlanError;
pub(crate) use validate::{AccessPlanError, CursorPlanError, OrderPlanError};
