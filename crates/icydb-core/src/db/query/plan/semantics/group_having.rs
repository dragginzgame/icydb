//! Module: db::query::plan::semantics::group_having
//! Responsibility: validate and normalize HAVING semantics against grouped
//! projection and aggregate visibility rules.
//! Does not own: grouped executor runtime or generic predicate normalization outside HAVING.
//! Boundary: keeps HAVING-specific grouped semantics isolated within planning.

use crate::db::{
    cursor::CursorPlanError,
    predicate::{
        CompareOp,
        grouped_having_compare_op_supported as predicate_grouped_having_compare_op_supported,
    },
    query::plan::{GroupHavingExpr, GroupHavingSpec, GroupPlan},
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
    predicate_grouped_having_compare_op_supported(op)
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
    having: Option<&GroupHavingSpec>,
) -> bool {
    having.is_none_or(|having| {
        grouped_having_expr_streaming_compatible(&GroupHavingExpr::from_legacy_spec(having))
    })
}

fn grouped_having_expr_streaming_compatible(expr: &GroupHavingExpr) -> bool {
    match expr {
        GroupHavingExpr::Compare { op, .. } => grouped_having_compare_op_supported(*op),
        GroupHavingExpr::And(children) => children
            .iter()
            .all(grouped_having_expr_streaming_compatible),
    }
}
