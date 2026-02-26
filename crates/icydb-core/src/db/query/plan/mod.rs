//! Plan module wiring; must not implement planning or validation logic.

mod access_projection;
pub(crate) mod planner;
#[cfg(test)]
mod tests;
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
pub use crate::db::access::OrderDirection;
pub(crate) use crate::db::access::{
    AccessPlannedQuery, DeleteLimitSpec, LogicalPlan, OrderSpec, PageSpec,
};
pub use validate::PlanError;
pub(crate) use validate::{AccessPlanError, OrderPlanError};
