//! Plan module wiring; must not implement planning or validation logic.

mod access_projection;
pub(crate) mod logical;
pub(crate) mod planner;
#[cfg(test)]
mod tests;
mod types;
pub(crate) mod validate;

pub(crate) use crate::db::cursor::CursorPlanError;
pub(crate) use crate::db::direction::Direction;
pub(in crate::db) use crate::db::query::fingerprint::canonical;
pub(crate) use access_projection::{
    AccessPlanProjection, project_access_plan, project_explain_access_path,
};
///
/// Re-Exports
///
pub(crate) use logical::{AccessPlannedQuery, LogicalPlan};
pub use types::OrderDirection;
pub(crate) use types::{DeleteLimitSpec, OrderSpec, PageSpec};
pub use validate::PlanError;
pub(crate) use validate::{AccessPlanError, OrderPlanError};
