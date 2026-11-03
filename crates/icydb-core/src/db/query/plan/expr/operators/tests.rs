//! Caller parity keeps literal refusal distinct from runtime failure.

use crate::{
    db::query::{
        plan::expr::{
            BinaryOp, CompiledExpr, CompiledExprValueReader, Expr,
            eval_builder_expr_for_value_preview, eval_literal_only_expr_value,
        },
        preparation::with_preparation_work,
    },
    value::Value,
};
use std::borrow::Cow;

struct EmptyReader;
impl CompiledExprValueReader for EmptyReader {
    fn read_slot(&self, _: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_group_key(&self, _: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_aggregate(&self, _: usize) -> Option<Cow<'_, Value>> {
        None
    }
}

fn results(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> (Option<Value>, Option<Value>, Option<Value>) {
    let expr = Expr::Binary {
        op,
        left: Box::new(Expr::Literal(left.clone())),
        right: Box::new(Expr::Literal(right.clone())),
    };
    let compiled = CompiledExpr::Binary {
        op,
        left: Box::new(CompiledExpr::Literal(left)),
        right: Box::new(CompiledExpr::Literal(right)),
    };
    let folded = with_preparation_work(|work| eval_literal_only_expr_value(&expr, work)).unwrap();
    let preview = eval_builder_expr_for_value_preview(&expr, "unused", &Value::Null).ok();
    let executed = compiled.evaluate(&EmptyReader).ok().map(Cow::into_owned);
    (folded, preview, executed)
}

#[test]
fn scalar_operator_callers_preserve_the_complete_three_valued_truth_table() {
    let values = [Value::Bool(false), Value::Bool(true), Value::Null];
    // Rows/columns are FALSE, TRUE, NULL. This oracle does not invoke the kernel.
    let and = [
        [Some(false), Some(false), Some(false)],
        [Some(false), Some(true), None],
        [Some(false), None, None],
    ];
    let or = [
        [Some(false), Some(true), None],
        [Some(true), Some(true), Some(true)],
        [None, Some(true), None],
    ];
    for (op, expected) in [(BinaryOp::And, and), (BinaryOp::Or, or)] {
        for (i, left) in values.iter().enumerate() {
            for (j, right) in values.iter().enumerate() {
                let value = Some(expected[i][j].map_or(Value::Null, Value::Bool));
                assert_eq!(
                    results(op, left.clone(), right.clone()),
                    (value.clone(), value.clone(), value)
                );
            }
        }
    }
}

#[test]
fn scalar_operator_callers_preserve_nulls_numeric_failures_and_strict_comparison() {
    for (op, left, right, expected) in [
        (
            BinaryOp::Add,
            Value::Int64(2),
            Value::Int64(3),
            Some(Value::Decimal(5.into())),
        ),
        (
            BinaryOp::Mul,
            Value::Text("invalid".into()),
            Value::Null,
            Some(Value::Null),
        ),
        (BinaryOp::Div, Value::Int64(1), Value::Int64(0), None),
        (
            BinaryOp::Mul,
            Value::Int128(i128::MAX),
            Value::Int128(2),
            None,
        ),
        (
            BinaryOp::Add,
            Value::Text("invalid".into()),
            Value::Int64(2),
            None,
        ),
        (
            BinaryOp::Lt,
            Value::Text("a".into()),
            Value::Text("b".into()),
            Some(Value::Bool(true)),
        ),
        (
            BinaryOp::Eq,
            Value::Null,
            Value::Int64(1),
            Some(Value::Null),
        ),
        (
            BinaryOp::Ne,
            Value::Nat64(1),
            Value::Int64(1),
            Some(Value::Bool(false)),
        ),
    ] {
        assert_eq!(
            results(op, left, right),
            (expected.clone(), expected.clone(), expected)
        );
    }
}
