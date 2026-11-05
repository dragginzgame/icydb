use super::{GroupedCursorPolicyViolation, grouped_having_streaming_compatible};
use crate::{
    db::query::plan::expr::{BinaryOp, CaseWhenArm, Expr},
    value::Value,
};

#[test]
fn grouped_cursor_policies_preserve_request_rejection_taxonomy() {
    use crate::{
        db::QueryError,
        error::{ErrorClass, ErrorOrigin},
    };
    use icydb_diagnostic_code::{DiagnosticDecodeReason, DiagnosticFactTag, ErrorCode};

    for (policy, reason) in [
        (
            GroupedCursorPolicyViolation::ContinuationRequiresLimit,
            DiagnosticDecodeReason::CursorGroupedContinuationRequiresLimit,
        ),
        (
            GroupedCursorPolicyViolation::GlobalDistinctContinuationUnsupported,
            DiagnosticDecodeReason::CursorGlobalDistinctContinuationUnsupported,
        ),
    ] {
        let internal = policy.into_cursor_plan_error().into_internal_error();
        assert_eq!(internal.class(), ErrorClass::Unsupported);
        assert_eq!(internal.origin(), ErrorOrigin::Cursor);
        let query = QueryError::from_cursor_plan_error(policy.into_cursor_plan_error());
        assert_eq!(
            query.diagnostic().error_code(),
            ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR
        );
        assert_eq!(
            query.diagnostic_facts(),
            vec![(DiagnosticFactTag::DecodeReason, reason.raw())]
        );
    }
}

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
