//! Plan module wiring; must not implement planning or validation logic.

mod access_projection;
pub(crate) mod canonical;
pub(crate) mod continuation;
pub(crate) mod executable;
pub(crate) mod explain;
pub(crate) mod fingerprint;
mod hash_parts;
pub(crate) mod logical;
pub(crate) mod planner;
#[cfg(test)]
mod tests;
mod types;
pub(crate) mod validate;

pub(crate) use access_projection::{
    AccessPlanProjection, project_access_plan, project_explain_access_path,
};
pub(in crate::db) use continuation::{
    ContinuationSignature, ContinuationToken, IndexRangeCursorAnchor, decode_pk_cursor_boundary,
};
///
/// Re-Exports
///
pub(in crate::db) use executable::{ExecutablePlan, PlannedCursor};
#[cfg(test)]
pub(crate) use explain::ExplainOrderPushdown;
pub(crate) use explain::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainOrderBy, ExplainPagination, ExplainPlan,
    ExplainPredicate,
};
pub(crate) use fingerprint::PlanFingerprint;
pub(crate) use logical::LogicalPlan;
pub use types::OrderDirection;
pub(crate) use types::{
    AccessPath, AccessPlan, CursorBoundary, CursorBoundarySlot, DeleteLimitSpec, OrderSpec,
    PageSpec,
};
pub use validate::PlanError;
