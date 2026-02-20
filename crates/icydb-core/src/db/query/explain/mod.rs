//! Explain projection surface split from query plan internals.

mod explain;

pub(crate) use crate::db::query::plan::{
    AccessPlan, AccessPlanProjection, DeleteLimitSpec, LogicalPlan, OrderDirection, OrderSpec,
    PageSpec, project_access_plan,
};
#[cfg(test)]
pub(crate) use explain::ExplainOrderPushdown;
pub(crate) use explain::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainOrderBy, ExplainPagination, ExplainPlan,
    ExplainPredicate,
};
