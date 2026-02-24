//! Shared query-policy rules used by intent, planning, and execution guardrails.
//!
//! This module centralizes semantic invariants so boundary layers can map one
//! canonical rule set into their own error types.

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

impl PlanPolicyError {
    /// Canonical invariant message for executor-boundary plan-shape failures.
    #[must_use]
    pub const fn invariant_message(self) -> &'static str {
        match self {
            Self::EmptyOrderSpec => {
                "invalid logical plan: order specification must include at least one field"
            }
            Self::DeletePlanWithPagination => {
                "invalid logical plan: delete plans must not carry pagination"
            }
            Self::LoadPlanWithDeleteLimit => {
                "invalid logical plan: load plans must not carry delete limits"
            }
            Self::DeleteLimitRequiresOrder => {
                "invalid logical plan: delete limit requires an explicit ordering"
            }
            Self::UnorderedPagination => {
                "invalid logical plan: unordered pagination is not allowed"
            }
        }
    }
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
pub(crate) fn validate_plan_shape<K>(plan: &LogicalPlan<K>) -> Result<(), PlanPolicyError> {
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
            if plan.page.is_some() && !has_order {
                return Err(PlanPolicyError::UnorderedPagination);
            }
        }
    }

    Ok(())
}
