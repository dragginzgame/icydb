//! Module: db::query::plan::semantics::group_having
//! Responsibility: validate and normalize HAVING semantics against grouped
//! projection and aggregate visibility rules.
//! Does not own: grouped executor runtime or generic predicate normalization outside HAVING.
//! Boundary: keeps HAVING-specific grouped semantics isolated within planning.

use crate::db::{
    cursor::CursorPlanError,
    predicate::CompareOp,
    query::plan::{
        GroupPlan,
        expr::{BinaryOp, Expr},
    },
};
#[cfg(test)]
use crate::{
    db::{
        CoercionId,
        predicate::{CoercionSpec, compare_eq, compare_order},
    },
    value::Value,
};

///
/// GroupedCursorPolicyViolation
///
/// Canonical grouped cursor-policy violations shared by planner and executor
/// boundaries so grouped continuation rules are not reimplemented per layer.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedCursorPolicyViolation {
    ContinuationRequiresLimit,
    GlobalDistinctContinuationUnsupported,
}

impl GroupedCursorPolicyViolation {
    /// Return canonical invariant message text for grouped cursor policy violations.
    #[must_use]
    pub(in crate::db) const fn invariant_message(self) -> &'static str {
        match self {
            Self::ContinuationRequiresLimit => {
                "grouped continuation cursors require an explicit LIMIT"
            }
            Self::GlobalDistinctContinuationUnsupported => {
                "global DISTINCT grouped aggregates do not support continuation cursors"
            }
        }
    }

    /// Convert one grouped cursor-policy violation into the cursor-plan error
    /// surface used by continuation validation.
    #[must_use]
    pub(in crate::db) fn into_cursor_plan_error(self) -> CursorPlanError {
        CursorPlanError::continuation_cursor_invariant(self.invariant_message())
    }
}

/// Return whether grouped HAVING supports this compare operator.
#[must_use]
pub(crate) const fn grouped_having_compare_op_supported(op: CompareOp) -> bool {
    grouped_having_compare_kind(op).is_some()
}

/// Evaluate one grouped HAVING comparison under the canonical grouped planner semantics.
#[must_use]
#[cfg(test)]
pub(crate) fn evaluate_grouped_having_compare(
    actual: &Value,
    op: CompareOp,
    expected: &Value,
) -> Option<bool> {
    let kind = grouped_having_compare_kind(op)?;

    if matches!(expected, Value::Null) {
        return Some(match kind {
            GroupedHavingCompareKind::Eq => matches!(actual, Value::Null),
            GroupedHavingCompareKind::Ne => !matches!(actual, Value::Null),
            GroupedHavingCompareKind::Lt
            | GroupedHavingCompareKind::Lte
            | GroupedHavingCompareKind::Gt
            | GroupedHavingCompareKind::Gte => false,
        });
    }

    let numeric = CoercionSpec::new(CoercionId::NumericWiden);
    let strict = CoercionSpec::default();
    let coercion = if actual.supports_numeric_coercion() || expected.supports_numeric_coercion() {
        &numeric
    } else {
        &strict
    };

    Some(match kind {
        GroupedHavingCompareKind::Eq => compare_eq(actual, expected, coercion).unwrap_or(false),
        GroupedHavingCompareKind::Ne => {
            compare_eq(actual, expected, coercion).is_some_and(|equal| !equal)
        }
        GroupedHavingCompareKind::Lt => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_lt)
        }
        GroupedHavingCompareKind::Lte => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_le)
        }
        GroupedHavingCompareKind::Gt => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_gt)
        }
        GroupedHavingCompareKind::Gte => {
            compare_order(actual, expected, coercion).is_some_and(std::cmp::Ordering::is_ge)
        }
    })
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

pub(in crate::db::query::plan::semantics) fn grouped_having_streaming_compatible(
    having_expr: Option<&Expr>,
) -> bool {
    having_expr.is_none_or(grouped_having_expr_streaming_compatible)
}

fn grouped_having_expr_streaming_compatible(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::Aggregate(_) => true,
        Expr::FunctionCall { args, .. } => {
            args.iter().all(grouped_having_expr_streaming_compatible)
        }
        Expr::Unary { expr, .. } => grouped_having_expr_streaming_compatible(expr),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                grouped_having_expr_streaming_compatible(arm.condition())
                    && grouped_having_expr_streaming_compatible(arm.result())
            }) && grouped_having_expr_streaming_compatible(else_expr)
        }
        Expr::Binary { op, left, right } => match op {
            BinaryOp::Eq => {
                grouped_having_compare_op_supported(CompareOp::Eq)
                    && grouped_having_expr_streaming_compatible(left)
                    && grouped_having_expr_streaming_compatible(right)
            }
            BinaryOp::Ne => {
                grouped_having_compare_op_supported(CompareOp::Ne)
                    && grouped_having_expr_streaming_compatible(left)
                    && grouped_having_expr_streaming_compatible(right)
            }
            BinaryOp::Lt => {
                grouped_having_compare_op_supported(CompareOp::Lt)
                    && grouped_having_expr_streaming_compatible(left)
                    && grouped_having_expr_streaming_compatible(right)
            }
            BinaryOp::Lte => {
                grouped_having_compare_op_supported(CompareOp::Lte)
                    && grouped_having_expr_streaming_compatible(left)
                    && grouped_having_expr_streaming_compatible(right)
            }
            BinaryOp::Gt => {
                grouped_having_compare_op_supported(CompareOp::Gt)
                    && grouped_having_expr_streaming_compatible(left)
                    && grouped_having_expr_streaming_compatible(right)
            }
            BinaryOp::Gte => {
                grouped_having_compare_op_supported(CompareOp::Gte)
                    && grouped_having_expr_streaming_compatible(left)
                    && grouped_having_expr_streaming_compatible(right)
            }
            BinaryOp::And => {
                grouped_having_expr_streaming_compatible(left)
                    && grouped_having_expr_streaming_compatible(right)
            }
            BinaryOp::Or | BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => false,
        },
        #[cfg(test)]
        Expr::Alias { expr, .. } => grouped_having_expr_streaming_compatible(expr),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedHavingCompareKind {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

const fn grouped_having_compare_kind(op: CompareOp) -> Option<GroupedHavingCompareKind> {
    match op {
        CompareOp::Eq => Some(GroupedHavingCompareKind::Eq),
        CompareOp::Ne => Some(GroupedHavingCompareKind::Ne),
        CompareOp::Lt => Some(GroupedHavingCompareKind::Lt),
        CompareOp::Lte => Some(GroupedHavingCompareKind::Lte),
        CompareOp::Gt => Some(GroupedHavingCompareKind::Gt),
        CompareOp::Gte => Some(GroupedHavingCompareKind::Gte),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => None,
    }
}
