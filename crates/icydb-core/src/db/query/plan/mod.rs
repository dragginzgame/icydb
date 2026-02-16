//! Plan module wiring; must not implement planning or validation logic.

mod access_projection;
pub mod canonical;
pub mod continuation;
pub(crate) mod executable;
pub mod explain;
pub mod fingerprint;
mod hash_parts;
pub(crate) mod logical;
pub mod planner;
#[cfg(test)]
mod tests;
mod types;
pub mod validate;

pub(crate) use access_projection::{
    AccessPlanProjection, project_access_plan, project_explain_access_path,
};
pub use continuation::ContinuationSignature;
pub(crate) use continuation::{ContinuationToken, decode_pk_cursor_boundary};
///
/// Re-Exports
///
pub use executable::ExecutablePlan;
pub use explain::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainOrder, ExplainOrderBy, ExplainOrderPushdown,
    ExplainPagination, ExplainPlan, ExplainPredicate,
};
pub use fingerprint::PlanFingerprint;
pub(crate) use logical::LogicalPlan;
pub use types::OrderDirection;
pub(crate) use types::{
    AccessPath, AccessPlan, CursorBoundary, CursorBoundarySlot, DeleteLimitSpec, OrderSpec,
    PageSpec,
};
pub use validate::PlanError;
