//! Module: db::query::plan::semantics::group_having
//! Responsibility: grouped cursor policy and HAVING streaming eligibility.
//! Does not own: grouped executor runtime or generic predicate normalization outside HAVING.
//! Boundary: keeps HAVING-specific grouped semantics isolated within planning.

use crate::db::{
    cursor::CursorPlanError,
    query::plan::{
        GroupPlan,
        expr::{BinaryOp, Expr, truth_condition_binary_compare_op},
    },
};
use icydb_diagnostic_code::DiagnosticDecodeReason;

///
/// GroupedCursorPolicyViolation
///
/// Canonical grouped cursor-policy violations shared by planner and executor
/// boundaries so grouped continuation rules are not reimplemented per layer.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedCursorPolicyViolation {
    ContinuationRequiresLimit,
    GlobalDistinctContinuationUnsupported,
}

impl GroupedCursorPolicyViolation {
    /// Convert one grouped cursor-policy violation into the cursor-plan error
    /// surface used by continuation validation.
    #[must_use]
    pub(in crate::db) const fn into_cursor_plan_error(self) -> CursorPlanError {
        // A decoded cursor can still violate request policy without indicating
        // an internal planner/executor contract failure.
        let reason = match self {
            Self::ContinuationRequiresLimit => {
                DiagnosticDecodeReason::CursorGroupedContinuationRequiresLimit
            }
            Self::GlobalDistinctContinuationUnsupported => {
                DiagnosticDecodeReason::CursorGlobalDistinctContinuationUnsupported
            }
        };
        CursorPlanError::invalid_continuation_cursor_payload(reason)
    }
}

/// Return grouped cursor-policy violations for one grouped plan shape.
#[must_use]
pub(in crate::db) fn grouped_cursor_policy_violation(
    grouped: &GroupPlan,
    cursor_present: bool,
) -> Option<GroupedCursorPolicyViolation> {
    if !cursor_present {
        return None;
    }
    if grouped
        .scalar
        .page
        .as_ref()
        .and_then(|page| page.limit)
        .is_none()
    {
        return Some(GroupedCursorPolicyViolation::ContinuationRequiresLimit);
    }
    if grouped.is_global_distinct_aggregate_without_group_keys() {
        return Some(GroupedCursorPolicyViolation::GlobalDistinctContinuationUnsupported);
    }

    None
}

pub(in crate::db::query::plan::semantics) fn grouped_having_streaming_compatible<E>(
    having_expr: Option<&Expr>,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<bool, E> {
    let Some(expr) = having_expr else {
        return Ok(true);
    };
    expr.try_all_tree_expr(&mut |node| {
        observe(1)?;
        Ok(match node {
            Expr::Field(_)
            | Expr::FieldPath(_)
            | Expr::Literal(_)
            | Expr::Aggregate(_)
            | Expr::FunctionCall { .. }
            | Expr::Unary { .. }
            | Expr::Case { .. } => true,
            Expr::Binary { op, .. } => {
                // Streaming supports comparisons joined by AND. Other binary
                // expressions remain executable through non-streaming routes.
                truth_condition_binary_compare_op(*op).is_some() || matches!(op, BinaryOp::And)
            }
        })
    })
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_copy!(GroupedCursorPolicyViolation);

///
/// TESTS
///

#[cfg(test)]
mod tests;
