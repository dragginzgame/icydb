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
pub(in crate::db::query) mod grouped;
mod intent_policy;
mod order;
mod plan_shape;
mod symbols;
#[cfg(test)]
mod tests;

pub(in crate::db::query) use core::{validate_group_query_semantics, validate_query_semantics};
pub(in crate::db) use cursor_policy::validate_cursor_order_plan_shape;
#[cfg(test)]
pub(in crate::db::query) use cursor_policy::validate_cursor_paging_requirements;
pub(in crate::db) use errors::CursorOrderPlanShapeError;
pub use errors::PlanError;
pub(crate) use errors::{
    CursorPagingPolicyError, ExprPlanError, GroupPlanError, OrderPlanError, PolicyPlanError,
};
pub(in crate::db::query) use errors::{
    FluentLoadPolicyViolation, IntentKeyAccessKind, IntentKeyAccessPolicyViolation,
};
#[cfg(test)]
pub(crate) use errors::{PlanPolicyError, PlanUserError};
pub(in crate::db::query) use fluent_policy::{
    validate_fluent_non_paged_mode, validate_fluent_paged_mode,
};
pub(in crate::db::query) use intent_policy::{
    validate_intent_key_access_policy, validate_intent_plan_shape,
};
pub(in crate::db::query) use plan_shape::{has_explicit_order, validate_plan_shape};
pub(in crate::db::query::plan::validate) use symbols::resolve_group_aggregate_target_field_type;
pub(in crate::db) use symbols::{
    resolve_aggregate_target_field_slot_with_schema, resolve_group_field_slot,
    resolve_group_field_slot_with_schema,
};
