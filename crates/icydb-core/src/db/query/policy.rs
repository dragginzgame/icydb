//! Query-shape policy rules used by intent, planning, and executor guardrails.
//!
//! This module centralizes query policy invariants so boundary layers can map
//! one canonical rule set into their own error types.
//!
//! Ownership contract:
//! - This module is the sole owner of query shape-policy rules.
//! - ORDER semantic validation stays in `query::plan::validate`.
//! - Executors may assert these rules defensively, but must not redefine them.

use crate::db::query::{
    intent::{LoadSpec, QueryMode},
    plan::{LogicalPlan, OrderSpec},
};
use thiserror::Error as ThisError;

///
/// PlanPolicyError
/// Canonical policy failures for logical plan shape invariants.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum PlanPolicyError {
    #[error("order specification must include at least one field")]
    EmptyOrderSpec,

    #[error("delete plans must not include pagination")]
    DeletePlanWithPagination,

    #[error("load plans must not include delete limits")]
    LoadPlanWithDeleteLimit,

    #[error("delete limit requires an explicit ordering")]
    DeleteLimitRequiresOrder,

    #[error("unordered pagination is not allowed")]
    UnorderedPagination,
}

///
/// CursorPagingPolicyError
/// Canonical policy failures for cursor-pagination readiness.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum CursorPagingPolicyError {
    #[error("cursor pagination requires an explicit ordering")]
    CursorRequiresOrder,

    #[error("cursor pagination requires a limit")]
    CursorRequiresLimit,
}

/// Return true when an ORDER BY exists and contains at least one field.
#[must_use]
pub(crate) fn has_explicit_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| !order.fields.is_empty())
}

/// Return true when an ORDER BY exists but is empty.
#[must_use]
pub(crate) fn has_empty_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| order.fields.is_empty())
}

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
pub(crate) const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    if !has_order {
        return Err(CursorPagingPolicyError::CursorRequiresOrder);
    }
    if spec.limit.is_none() {
        return Err(CursorPagingPolicyError::CursorRequiresLimit);
    }

    Ok(())
}

/// Validate order-shape rules shared across intent and logical plan boundaries.
pub(crate) fn validate_order_shape(order: Option<&OrderSpec>) -> Result<(), PlanPolicyError> {
    if has_empty_order(order) {
        return Err(PlanPolicyError::EmptyOrderSpec);
    }

    Ok(())
}

/// Validate intent-level plan-shape rules derived from query mode + order.
pub(crate) fn validate_intent_plan_shape(
    mode: QueryMode,
    order: Option<&OrderSpec>,
) -> Result<(), PlanPolicyError> {
    validate_order_shape(order)?;

    let has_order = has_explicit_order(order);
    if matches!(mode, QueryMode::Delete(spec) if spec.limit.is_some()) && !has_order {
        return Err(PlanPolicyError::DeleteLimitRequiresOrder);
    }

    Ok(())
}

/// Validate mode/order/pagination invariants for a logical plan.
pub(crate) fn validate_plan_shape(plan: &LogicalPlan) -> Result<(), PlanPolicyError> {
    let grouped = matches!(plan, LogicalPlan::Grouped(_));
    let plan = match plan {
        LogicalPlan::Scalar(plan) => plan,
        LogicalPlan::Grouped(plan) => &plan.scalar,
    };
    validate_order_shape(plan.order.as_ref())?;

    let has_order = has_explicit_order(plan.order.as_ref());
    if plan.delete_limit.is_some() && !has_order {
        return Err(PlanPolicyError::DeleteLimitRequiresOrder);
    }

    match plan.mode {
        QueryMode::Delete(_) => {
            if plan.page.is_some() {
                return Err(PlanPolicyError::DeletePlanWithPagination);
            }
        }
        QueryMode::Load(_) => {
            if plan.delete_limit.is_some() {
                return Err(PlanPolicyError::LoadPlanWithDeleteLimit);
            }
            // GROUP BY v1 uses canonical grouped key ordering when ORDER BY is
            // omitted, so grouped pagination remains deterministic without an
            // explicit sort clause.
            if plan.page.is_some() && !has_order && !grouped {
                return Err(PlanPolicyError::UnorderedPagination);
            }
        }
    }

    Ok(())
}
