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
        let _ = self;

        CursorPlanError::continuation_cursor_invariant()
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
            Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) | Expr::Aggregate(_) => true,
            Expr::FunctionCall { .. } | Expr::Unary { .. } | Expr::Case { .. } => true,
            Expr::Binary { op, .. } => {
                // Streaming supports comparisons joined by AND. Other binary
                // expressions remain executable through non-streaming routes.
                truth_condition_binary_compare_op(*op).is_some() || matches!(op, BinaryOp::And)
            }
            #[cfg(test)]
            Expr::Alias { .. } => true,
        })
    })
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_copy!(GroupedCursorPolicyViolation);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::grouped_having_streaming_compatible;
    use crate::{
        db::query::plan::expr::{BinaryOp, CaseWhenArm, Expr},
        value::Value,
    };

    #[test]
    fn streaming_having_preserves_binary_operator_policy_and_short_circuiting() {
        for (op, expected) in [
            (BinaryOp::Eq, true),
            (BinaryOp::Ne, true),
            (BinaryOp::Lt, true),
            (BinaryOp::Lte, true),
            (BinaryOp::Gt, true),
            (BinaryOp::Gte, true),
            (BinaryOp::And, true),
            (BinaryOp::Or, false),
            (BinaryOp::Add, false),
            (BinaryOp::Sub, false),
            (BinaryOp::Mul, false),
            (BinaryOp::Div, false),
        ] {
            let expr = Expr::Binary {
                op,
                left: Box::new(Expr::Literal(Value::Bool(true))),
                right: Box::new(Expr::Literal(Value::Bool(false))),
            };
            let mut visits = 0;
            let result = grouped_having_streaming_compatible(Some(&expr), &mut |steps| {
                visits += steps;
                Ok::<(), ()>(())
            })
            .unwrap();
            assert_eq!(result, expected, "{op:?}");
            assert_eq!(visits, if expected { 3 } else { 1 });
        }
    }

    #[test]
    fn streaming_having_checks_nested_branches_and_propagates_observer_failure() {
        let expr = Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Literal(Value::Bool(true)),
                Expr::Literal(Value::Bool(true)),
            )],
            else_expr: Box::new(Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(Expr::Literal(Value::Bool(true))),
                right: Box::new(Expr::Literal(Value::Bool(false))),
            }),
        };
        assert_eq!(
            grouped_having_streaming_compatible(Some(&expr), &mut |_| Ok::<(), ()>(())),
            Ok(false)
        );
        // Even a branch that row evaluation would skip still affects route eligibility.
        let mut visits = 0;
        assert_eq!(
            grouped_having_streaming_compatible(Some(&expr), &mut |_| {
                visits += 1;
                if visits == 3 { Err(()) } else { Ok(()) }
            }),
            Err(())
        );
        assert_eq!(visits, 3);
        assert_eq!(
            grouped_having_streaming_compatible::<()>(None, &mut |_| Err(())),
            Ok(true)
        );
    }
}
