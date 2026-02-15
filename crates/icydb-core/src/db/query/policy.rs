//! Shared query-policy rules used by intent, planning, and execution guardrails.
//!
//! This module centralizes semantic invariants so boundary layers can map one
//! canonical rule set into their own error types.

use crate::db::query::{
    LoadSpec, QueryMode,
    plan::{LogicalPlan, OrderSpec},
};

///
/// PlanPolicyError
/// Canonical policy failures for logical plan shape invariants.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanPolicyError {
    EmptyOrderSpec,
    DeletePlanWithPagination,
    LoadPlanWithDeleteLimit,
    DeleteLimitRequiresOrder,
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

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorPagingPolicyError {
    CursorRequiresOrder,
    CursorRequiresLimit,
    CursorWithOffsetUnsupported,
}

///
/// CursorOrderPolicyError
/// Canonical policy failures for cursor order preconditions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorOrderPolicyError {
    CursorRequiresOrder,
}

/// Return true when an ORDER BY exists and contains at least one field.
#[must_use]
pub fn has_explicit_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| !order.fields.is_empty())
}

/// Return true when an ORDER BY exists but is empty.
#[must_use]
pub fn has_empty_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| order.fields.is_empty())
}

/// Require a non-empty ORDER BY and return the order spec.
pub const fn require_cursor_order(
    order: Option<&OrderSpec>,
) -> Result<&OrderSpec, CursorOrderPolicyError> {
    match order {
        Some(order) if !order.fields.is_empty() => Ok(order),
        _ => Err(CursorOrderPolicyError::CursorRequiresOrder),
    }
}

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
pub const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    if !has_order {
        return Err(CursorPagingPolicyError::CursorRequiresOrder);
    }
    if spec.limit.is_none() {
        return Err(CursorPagingPolicyError::CursorRequiresLimit);
    }
    if spec.offset > 0 {
        return Err(CursorPagingPolicyError::CursorWithOffsetUnsupported);
    }

    Ok(())
}

/// Validate mode/order/pagination invariants for a logical plan.
pub fn validate_plan_shape<K>(plan: &LogicalPlan<K>) -> Result<(), PlanPolicyError> {
    let has_order = has_explicit_order(plan.order.as_ref());

    if has_empty_order(plan.order.as_ref()) {
        return Err(PlanPolicyError::EmptyOrderSpec);
    }

    match plan.mode {
        QueryMode::Delete(_) => {
            if plan.page.is_some() {
                return Err(PlanPolicyError::DeletePlanWithPagination);
            }
            if plan.delete_limit.is_some() && !has_order {
                return Err(PlanPolicyError::DeleteLimitRequiresOrder);
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
