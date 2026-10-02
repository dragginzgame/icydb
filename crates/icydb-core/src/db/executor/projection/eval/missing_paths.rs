//! Missing-path signals compose as UNKNOWN without hiding reader failures.

use crate::{
    db::{
        executor::projection::eval::scalar::{
            eval_compiled_expr_with_value_reader, eval_compiled_filter_expr_with_value_cow_reader,
        },
        query::plan::expr::{
            BinaryOp, CompiledExpr, CompiledExprCaseArm, CompiledExprValueReader, Function,
            ProjectionEvalError, UnaryOp,
        },
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

fn path() -> CompiledExpr {
    CompiledExpr::FieldPath {
        root_slot: 0,
        segments: vec!["rank".into()].into_boxed_slice(),
        segment_bytes: vec![b"rank".to_vec().into_boxed_slice()].into_boxed_slice(),
    }
}

fn binary(op: BinaryOp, left: CompiledExpr, right: CompiledExpr) -> CompiledExpr {
    CompiledExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn comparison() -> CompiledExpr {
    binary(BinaryOp::Eq, path(), CompiledExpr::Literal(Value::Nat64(5)))
}

fn call(function: Function, args: Vec<CompiledExpr>) -> CompiledExpr {
    CompiledExpr::FunctionCall {
        function,
        args: args.into_boxed_slice(),
    }
}

fn filter(expr: &CompiledExpr, root: &Value) -> bool {
    eval_compiled_filter_expr_with_value_cow_reader(expr, &mut |slot| {
        (slot == 0).then_some(Cow::Borrowed(root))
    })
    .unwrap()
}

fn record(rank: Value) -> Value {
    Value::Map(vec![(Value::Text("rank".into()), rank)])
}

#[test]
fn missing_path_boolean_truth_matrix_preserves_both_operand_orders() {
    let rows = [
        (Value::Null, None),
        (Value::Map(vec![]), None),
        (record(Value::Null), None),
        (record(Value::Nat64(5)), Some(true)),
        (record(Value::Nat64(7)), Some(false)),
    ];
    for (root, leaf) in rows {
        for (literal, sibling) in [
            (Value::Bool(true), Some(true)),
            (Value::Bool(false), Some(false)),
            (Value::Null, None),
        ] {
            for op in [BinaryOp::And, BinaryOp::Or] {
                let matches = match op {
                    BinaryOp::And => leaf == Some(true) && sibling == Some(true),
                    BinaryOp::Or => leaf == Some(true) || sibling == Some(true),
                    _ => unreachable!(),
                };
                for path_on_left in [true, false] {
                    let truth = CompiledExpr::Literal(literal.clone());
                    let (left, right) = if path_on_left {
                        (comparison(), truth)
                    } else {
                        (truth, comparison())
                    };
                    let expr = binary(op, left, right);
                    assert_eq!(
                        filter(&expr, &root),
                        matches,
                        "{root:?}, {op:?}, {literal:?}, left={path_on_left}"
                    );
                }
            }
        }
    }
}

#[test]
fn missing_path_null_tests_not_and_value_coalesce_keep_leaf_contracts() {
    for root in [Value::Null, Value::Map(vec![]), record(Value::Null)] {
        let is_null = call(Function::IsNull, vec![path()]);
        assert_eq!(filter(&is_null, &root), root == record(Value::Null));
        assert!(!filter(&call(Function::IsNotNull, vec![path()]), &root));
        assert!(!filter(&comparison(), &root));
        assert!(!filter(
            &CompiledExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(comparison())
            },
            &root
        ));
        let coalesce = binary(
            BinaryOp::Eq,
            call(
                Function::Coalesce,
                vec![path(), CompiledExpr::Literal(Value::Nat64(0))],
            ),
            CompiledExpr::Literal(Value::Nat64(0)),
        );
        assert_eq!(filter(&coalesce, &root), root == record(Value::Null));
        // Projection intentionally materializes missing descendants as NULL.
        assert_eq!(
            eval_compiled_expr_with_value_reader(&path(), &mut |_| Some(root.clone())).unwrap(),
            Value::Null
        );
        for op in [BinaryOp::And, BinaryOp::Or] {
            for sibling in [true, false] {
                let expr = binary(
                    op,
                    is_null.clone(),
                    CompiledExpr::Literal(Value::Bool(sibling)),
                );
                assert_eq!(
                    filter(&expr, &root),
                    if matches!(op, BinaryOp::Or) {
                        root == record(Value::Null) || sibling
                    } else {
                        root == record(Value::Null) && sibling
                    }
                );
            }
        }
    }
}

#[test]
fn missing_path_nested_junctions_survive_not_case_and_boolean_coalesce() {
    for root in [Value::Null, Value::Map(vec![])] {
        let unmatched = binary(
            BinaryOp::Or,
            comparison(),
            CompiledExpr::Literal(Value::Bool(false)),
        );
        let matched = binary(
            BinaryOp::Or,
            comparison(),
            CompiledExpr::Literal(Value::Bool(true)),
        );
        let nested = binary(
            BinaryOp::Or,
            binary(
                BinaryOp::And,
                comparison(),
                CompiledExpr::Literal(Value::Bool(true)),
            ),
            matched.clone(),
        );
        assert!(filter(&nested, &root));
        for expr in [unmatched.clone(), matched.clone()] {
            assert!(!filter(
                &CompiledExpr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(expr)
                },
                &root
            ));
        }
        let false_conjunction = binary(
            BinaryOp::And,
            comparison(),
            CompiledExpr::Literal(Value::Bool(false)),
        );
        assert!(filter(
            &CompiledExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(false_conjunction)
            },
            &root,
        ));
        let case = CompiledExpr::Case {
            when_then_arms: vec![CompiledExprCaseArm::new(
                unmatched,
                CompiledExpr::Literal(Value::Bool(false)),
            )]
            .into_boxed_slice(),
            else_expr: Box::new(CompiledExpr::Literal(Value::Bool(true))),
        };
        assert!(filter(&case, &root));
        assert!(filter(
            &call(
                Function::Coalesce,
                vec![matched, CompiledExpr::Literal(Value::Bool(false))]
            ),
            &root
        ));
    }
}

struct FailedReader(ProjectionEvalError);

impl CompiledExprValueReader for FailedReader {
    fn read_slot(&self, _slot: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_slot_checked(
        &self,
        _slot: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Err(self.0.clone())
    }
    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_field_path(
        &self,
        _root_slot: usize,
        _segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Err(self.0.clone())
    }
}

#[test]
fn missing_path_boolean_boundary_propagates_required_and_corrupt_reader_errors() {
    let corruption = InternalError::persisted_row_decode_corruption();
    for error in [
        ProjectionEvalError::missing_slot_value(0),
        ProjectionEvalError::ReaderFailed {
            class: corruption.class(),
            origin: corruption.origin(),
        },
        ProjectionEvalError::FieldPathEvaluationFailed {
            class: corruption.class(),
            origin: corruption.origin(),
        },
    ] {
        for op in [BinaryOp::And, BinaryOp::Or] {
            for sibling in [true, false] {
                for failure_on_left in [true, false] {
                    let truth = CompiledExpr::Literal(Value::Bool(sibling));
                    let (left, right) = if failure_on_left {
                        (path(), truth)
                    } else {
                        (truth, path())
                    };
                    assert_eq!(
                        binary(op, left, right)
                            .evaluate(&FailedReader(error.clone()))
                            .unwrap_err(),
                        error
                    );
                }
            }
        }
    }
    // A malformed ancestor is corruption, not an absent descendant.
    for failure_on_left in [true, false] {
        let truth = CompiledExpr::Literal(Value::Bool(true));
        let (left, right) = if failure_on_left {
            (comparison(), truth)
        } else {
            (truth, comparison())
        };
        let expr = binary(BinaryOp::Or, left, right);
        let error = eval_compiled_filter_expr_with_value_cow_reader(&expr, &mut |_| {
            Some(Cow::Owned(Value::Nat64(7)))
        })
        .unwrap_err();
        assert_eq!(error.class(), corruption.class());
        assert_eq!(error.origin(), corruption.origin());
    }
}
