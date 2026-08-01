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

mod cursor_policy;
mod errors;
pub(in crate::db::query) mod grouped;
mod intent_policy;
mod order;
mod plan_shape;
mod semantic_gates;
mod symbols;
pub(in crate::db) use cursor_policy::validate_cursor_order_plan_shape;
pub(in crate::db) use errors::CursorOrderPlanShapeError;
pub use errors::PlanError;
pub(crate) use errors::{
    ExprPlanError, ExprPlanTypeClass, GroupPlanError, OrderPlanError, PolicyPlanError,
};
#[cfg(test)]
pub(crate) use errors::{PlanPolicyError, PlanUserError};
pub(in crate::db::query) use intent_policy::validate_intent_plan_shape;
pub(in crate::db::query) use plan_shape::validate_plan_shape;
pub(in crate::db::query) use semantic_gates::{
    validate_group_query_semantics_with_schema, validate_query_semantics_with_schema,
};
#[cfg(feature = "sql")]
pub(in crate::db) use symbols::resolve_aggregate_target_field_slot_with_schema;
pub(in crate::db::query::plan::validate) use symbols::resolve_group_aggregate_target_field_type;
pub(in crate::db) use symbols::resolve_group_field_slot_with_schema;
