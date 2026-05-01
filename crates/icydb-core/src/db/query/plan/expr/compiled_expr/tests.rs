use crate::{
    db::query::plan::expr::{
        BinaryOp, CompiledExpr, CompiledExprValueReader, Function, ProjectionEvalError,
    },
    value::Value,
};
use std::borrow::Cow;
use std::cmp::Ordering;

struct TestRowView {
    slots: Vec<Option<Value>>,
}

struct TestGroupedView {
    group_keys: Vec<Value>,
    aggregates: Vec<Value>,
}

impl CompiledExprValueReader for TestRowView {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slots
            .get(slot)
            .and_then(Option::as_ref)
            .map(Cow::Borrowed)
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }
}

impl CompiledExprValueReader for TestGroupedView {
    fn read_slot(&self, _slot: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_group_key(&self, offset: usize) -> Option<Cow<'_, Value>> {
        self.group_keys.get(offset).map(Cow::Borrowed)
    }

    fn read_aggregate(&self, index: usize) -> Option<Cow<'_, Value>> {
        self.aggregates.get(index).map(Cow::Borrowed)
    }
}

fn row_view() -> TestRowView {
    TestRowView {
        slots: vec![
            Some(Value::Uint(7)),
            Some(Value::Int(3)),
            Some(Value::Null),
            Some(Value::Text("MiXeD".to_string())),
            Some(Value::Bool(true)),
        ],
    }
}

fn grouped_view() -> TestGroupedView {
    TestGroupedView {
        group_keys: vec![Value::Text("fighter".to_string())],
        aggregates: vec![Value::Uint(2)],
    }
}

fn evaluate(expr: &CompiledExpr) -> Value {
    expr.evaluate(&row_view())
        .expect("grouped compiled expression should evaluate")
        .into_owned()
}

#[test]
fn grouped_compiled_expr_reads_slots_without_cloning_contract_drift() {
    let expr = CompiledExpr::Slot {
        slot: 0,
        field: "age".to_string(),
    };

    assert_eq!(evaluate(&expr), Value::Uint(7));
}

#[test]
fn grouped_compiled_expr_preserves_slot_arithmetic_semantics() {
    let expr = CompiledExpr::Add {
        left_slot: 0,
        left_field: "age".to_string(),
        right_slot: 1,
        right_field: "rank".to_string(),
    };
    let value = evaluate(&expr);

    assert_eq!(
        value.cmp_numeric(&Value::Int(10)),
        Some(Ordering::Equal),
        "direct slot arithmetic must preserve shared numeric coercion semantics",
    );
}

#[test]
fn grouped_compiled_expr_case_only_true_selects_branch() {
    let expr = CompiledExpr::Case {
        when_then_arms: vec![
            super::CompiledExprCaseArm {
                condition: CompiledExpr::Literal(Value::Null),
                result: CompiledExpr::Literal(Value::Text("null".to_string())),
            },
            super::CompiledExprCaseArm {
                condition: CompiledExpr::BinarySlotLiteral {
                    op: BinaryOp::Gt,
                    slot: 0,
                    field: "age".to_string(),
                    literal: Value::Uint(5),
                    slot_on_left: true,
                },
                result: CompiledExpr::Literal(Value::Text("selected".to_string())),
            },
        ]
        .into_boxed_slice(),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };

    assert_eq!(evaluate(&expr), Value::Text("selected".to_string()));
}

#[test]
fn grouped_compiled_expr_case_false_and_null_fall_through() {
    let expr = CompiledExpr::Case {
        when_then_arms: vec![
            super::CompiledExprCaseArm {
                condition: CompiledExpr::Literal(Value::Null),
                result: CompiledExpr::Literal(Value::Text("null".to_string())),
            },
            super::CompiledExprCaseArm {
                condition: CompiledExpr::Literal(Value::Bool(false)),
                result: CompiledExpr::Literal(Value::Text("false".to_string())),
            },
        ]
        .into_boxed_slice(),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };

    assert_eq!(evaluate(&expr), Value::Text("else".to_string()));
}

#[test]
fn grouped_compiled_expr_case_slot_literal_selects_without_condition_value() {
    let expr = CompiledExpr::CaseSlotLiteral {
        op: BinaryOp::Gt,
        slot: 0,
        field: "age".to_string(),
        literal: Value::Uint(5),
        slot_on_left: true,
        then_expr: Box::new(CompiledExpr::Literal(Value::Text("selected".to_string()))),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };

    assert_eq!(evaluate(&expr), Value::Text("selected".to_string()));
}

#[test]
fn grouped_compiled_expr_case_slot_bool_preserves_null_fallthrough() {
    let expr = CompiledExpr::CaseSlotBool {
        slot: 2,
        field: "maybe_flag".to_string(),
        then_expr: Box::new(CompiledExpr::Literal(Value::Text("selected".to_string()))),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };

    assert_eq!(evaluate(&expr), Value::Text("else".to_string()));
}

#[test]
fn grouped_compiled_expr_function_calls_reuse_projection_semantics() {
    let expr = CompiledExpr::FunctionCall {
        function: Function::Lower,
        args: vec![CompiledExpr::Slot {
            slot: 3,
            field: "name".to_string(),
        }]
        .into_boxed_slice(),
    };

    assert_eq!(evaluate(&expr), Value::Text("mixed".to_string()));
}

#[test]
fn grouped_compiled_expr_missing_slot_keeps_field_diagnostic() {
    let expr = CompiledExpr::Slot {
        slot: 99,
        field: "missing_field".to_string(),
    };
    let err = expr
        .evaluate(&row_view())
        .expect_err("missing grouped slot should stay a projection error");

    assert_eq!(
        err.to_string(),
        "projection expression could not read field 'missing_field' at index=99",
    );
}

#[test]
fn compiled_expr_aggregate_in_row_context_errors_not_null() {
    let expr = CompiledExpr::Aggregate { index: 0 };
    let err = expr
        .evaluate(&row_view())
        .expect_err("row readers must not silently NULL aggregate leaves");

    assert_eq!(
        err,
        ProjectionEvalError::MissingGroupedAggregateValue {
            aggregate_index: 0,
            aggregate_count: 0,
        },
    );
}

#[test]
fn compiled_expr_group_key_in_row_context_errors_not_null() {
    let expr = CompiledExpr::GroupKey {
        offset: 0,
        field: "class".to_string(),
    };
    let err = expr
        .evaluate(&row_view())
        .expect_err("row readers must not silently NULL grouped-key leaves");

    assert_eq!(
        err,
        ProjectionEvalError::MissingFieldValue {
            field: "class".to_string(),
            index: 0,
        },
    );
}

#[test]
fn compiled_expr_slot_in_grouped_context_errors_not_null() {
    let expr = CompiledExpr::Slot {
        slot: 0,
        field: "age".to_string(),
    };
    let err = expr
        .evaluate(&grouped_view())
        .expect_err("grouped-output readers must not silently NULL slot leaves");

    assert_eq!(
        err,
        ProjectionEvalError::MissingFieldValue {
            field: "age".to_string(),
            index: 0,
        },
    );
}

#[test]
fn compiled_expr_out_of_bounds_grouped_reads_error_not_null() {
    let grouped_view = grouped_view();
    let group_key = CompiledExpr::GroupKey {
        offset: 9,
        field: "class".to_string(),
    };
    let aggregate = CompiledExpr::Aggregate { index: 9 };

    assert!(matches!(
        group_key.evaluate(&grouped_view),
        Err(ProjectionEvalError::MissingFieldValue { field, index })
            if field == "class" && index == 9
    ));
    assert!(matches!(
        aggregate.evaluate(&grouped_view),
        Err(ProjectionEvalError::MissingGroupedAggregateValue {
            aggregate_index: 9,
            ..
        })
    ));
}

#[test]
fn compiled_expr_case_missing_condition_read_errors_before_else() {
    let expr = CompiledExpr::Case {
        when_then_arms: vec![super::CompiledExprCaseArm {
            condition: CompiledExpr::Aggregate { index: 0 },
            result: CompiledExpr::Literal(Value::Text("then".to_string())),
        }]
        .into_boxed_slice(),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };
    let err = expr
        .evaluate(&row_view())
        .expect_err("missing CASE condition reads must not fall through as NULL");

    assert_eq!(
        err,
        ProjectionEvalError::MissingGroupedAggregateValue {
            aggregate_index: 0,
            aggregate_count: 0,
        },
    );
}

#[test]
fn compiled_expr_case_slot_bool_matches_generic_non_boolean_admission() {
    let generic = CompiledExpr::Case {
        when_then_arms: vec![super::CompiledExprCaseArm {
            condition: CompiledExpr::Slot {
                slot: 3,
                field: "name".to_string(),
            },
            result: CompiledExpr::Literal(Value::Text("then".to_string())),
        }]
        .into_boxed_slice(),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };
    let specialized = CompiledExpr::CaseSlotBool {
        slot: 3,
        field: "name".to_string(),
        then_expr: Box::new(CompiledExpr::Literal(Value::Text("then".to_string()))),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };

    assert_eq!(
        generic
            .evaluate(&row_view())
            .expect_err("generic CASE should reject text condition"),
        specialized
            .evaluate(&row_view())
            .expect_err("specialized CASE should reject text condition"),
    );
}
