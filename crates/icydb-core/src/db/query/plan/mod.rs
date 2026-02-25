//! Plan module wiring; must not implement planning or validation logic.

mod access_projection;
pub(crate) mod cursor;
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
pub(crate) use cursor::CursorPlanError;
///
/// Re-Exports
///
pub(crate) use logical::{AccessPlannedQuery, LogicalPlan};
pub use types::OrderDirection;
pub(crate) use types::{
    AccessPath, AccessPlan, DeleteLimitSpec, OrderSpec, PageSpec, SemanticIndexRangeSpec,
};
pub use validate::PlanError;
pub(crate) use validate::{AccessPlanError, OrderPlanError};
