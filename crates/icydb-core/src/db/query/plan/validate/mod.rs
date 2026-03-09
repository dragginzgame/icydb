//! Module: query::plan::validate
//! Responsibility: planner-owned query semantic validation and plan-policy enforcement.
//! Does not own: executor defensive runtime checks or execution-route dispatch semantics.
//! Boundary: emits plan-domain typed errors before executor handoff.
//!
//! Query-plan validation for planner-owned logical semantics.
//!
//! Validation ownership contract:
//! - `validate_query_semantics` owns user-facing query semantics and emits `PlanError`.
//! - executor-boundary defensive checks live in `db::executor::plan_validate`.
//!
//! Future rule changes must declare a semantic owner. Defensive re-check layers may mirror
//! rules, but must not reinterpret semantics or error class intent.

mod core;
mod cursor_policy;
mod errors;
mod fluent_policy;
mod grouped;
mod intent_policy;
mod order;
mod plan_shape;
mod symbols;

pub(crate) use core::{validate_group_query_semantics, validate_query_semantics};
pub(crate) use cursor_policy::{
    validate_cursor_order_plan_shape, validate_cursor_paging_requirements,
};
pub use errors::PlanError;
pub(crate) use errors::{
    CursorOrderPlanShapeError, FluentLoadPolicyViolation, IntentKeyAccessKind,
    IntentKeyAccessPolicyViolation,
};
pub(crate) use errors::{
    CursorPagingPolicyError, ExprPlanError, GroupPlanError, OrderPlanError, PolicyPlanError,
};
#[cfg(test)]
pub(crate) use errors::{PlanPolicyError, PlanUserError};
pub(crate) use fluent_policy::{validate_fluent_non_paged_mode, validate_fluent_paged_mode};
#[cfg(test)]
pub(in crate::db::query) use grouped::validate_group_projection_expr_compatibility_for_test;
pub(crate) use intent_policy::{validate_intent_key_access_policy, validate_intent_plan_shape};
pub(crate) use order::validate_order;
pub(crate) use plan_shape::{has_explicit_order, validate_order_shape, validate_plan_shape};
pub(crate) use symbols::resolve_group_field_slot;
