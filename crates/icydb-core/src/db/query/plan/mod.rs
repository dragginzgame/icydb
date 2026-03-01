//! Module: query::plan
//! Responsibility: logical query-plan module wiring and boundary re-exports.
//! Does not own: plan-model construction or semantic helper implementation details.
//! Boundary: intent/explain/planner/validator consumers import from this root only.

mod access_plan;
mod group;
mod model;
mod model_builder;
mod planner;
mod semantics;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

pub(crate) use access_plan::AccessPlannedQuery;
pub(in crate::db) use group::{GroupedExecutorHandoff, grouped_executor_handoff};
pub use model::OrderDirection;
pub(crate) use model::{AggregateKind, DeleteSpec, LoadSpec, QueryMode};
pub(crate) use model::{
    DeleteLimitSpec, FieldSlot, GroupAggregateKind, GroupAggregateSpec, GroupPlan, GroupSpec,
    GroupedExecutionConfig, LogicalPlan, OrderSpec, PageSpec, ScalarPlan,
};
pub(crate) use planner::{PlannerError, plan_access};
pub(crate) use semantics::{
    AccessPlanProjection, project_access_plan, project_explain_access_path,
};
pub use validate::PlanError;
pub(crate) use validate::{
    GroupPlanError, PolicyPlanError, has_explicit_order, validate_group_query_semantics,
    validate_intent_plan_shape, validate_order_shape, validate_query_semantics,
};
