use crate::{
    db::query::plan::expr::{
        BinaryOp, CompiledExpr, CompiledExprValueReader, Function, ProjectionEvalError, UnaryOp,
    },
    value::Value,
};
use std::{borrow::Cow, cell::RefCell, cmp::Ordering};

use super::ProjectionAccessCode;

struct TestRowView {
    slots: Vec<Option<Value>>,
}

struct TestGroupedView {
    group_keys: Vec<Value>,
    aggregates: Vec<Value>,
}

struct TracingRowView {
    slots: Vec<Option<Value>>,
    read_slots: RefCell<Vec<usize>>,
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

impl CompiledExprValueReader for TracingRowView {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.read_slots.borrow_mut().push(slot);
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

    fn read_field_path(
        &self,
        root_slot: usize,
        _field: &str,
        _segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        self.read_slots.borrow_mut().push(root_slot);

        Ok(self
            .slots
            .get(root_slot)
            .and_then(Option::as_ref)
            .map(Cow::Borrowed))
    }
}

fn row_view() -> TestRowView {
    TestRowView {
        slots: vec![
            Some(Value::Nat64(7)),
            Some(Value::Int64(3)),
            Some(Value::Null),
            Some(Value::Text("MiXeD".to_string())),
            Some(Value::Bool(true)),
        ],
    }
}

fn tracing_row_view() -> TracingRowView {
    TracingRowView {
        slots: vec![
            Some(Value::Nat64(7)),
            Some(Value::Int64(3)),
            Some(Value::Null),
            Some(Value::Text("MiXeD".to_string())),
            Some(Value::Bool(true)),
            Some(Value::Bool(false)),
            Some(Value::Nat64(10)),
            Some(Value::Nat64(2)),
        ],
        read_slots: RefCell::new(Vec::new()),
    }
}

fn grouped_view() -> TestGroupedView {
    TestGroupedView {
        group_keys: vec![Value::Text("fighter".to_string())],
        aggregates: vec![Value::Nat64(2)],
    }
}

fn evaluate(expr: &CompiledExpr) -> Value {
    expr.evaluate(&row_view())
        .expect("grouped compiled expression should evaluate")
        .into_owned()
}

fn field_path_expr(root_slot: usize) -> CompiledExpr {
    CompiledExpr::FieldPath {
        root_slot,
        field: "profile.rank".to_string(),
        segments: vec!["rank".to_string()].into_boxed_slice(),
        segment_bytes: vec![b"rank".to_vec().into_boxed_slice()].into_boxed_slice(),
    }
}

fn slot_expr(slot: usize) -> CompiledExpr {
    CompiledExpr::Slot {
        slot,
        field: format!("slot_{slot}"),
    }
}

fn direct_slot_binary_expr(op: BinaryOp, left_slot: usize, right_slot: usize) -> CompiledExpr {
    match op {
        BinaryOp::Add => CompiledExpr::Add {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Sub => CompiledExpr::Sub {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Mul => CompiledExpr::Mul {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Div => CompiledExpr::Div {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Eq => CompiledExpr::Eq {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Ne => CompiledExpr::Ne {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Lt => CompiledExpr::Lt {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Lte => CompiledExpr::Lte {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Gt => CompiledExpr::Gt {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::Gte => CompiledExpr::Gte {
            left_slot,
            left_field: "left".to_string(),
            right_slot,
            right_field: "right".to_string(),
        },
        BinaryOp::And | BinaryOp::Or => CompiledExpr::Binary {
            op,
            left: Box::new(slot_expr(left_slot)),
            right: Box::new(slot_expr(right_slot)),
        },
    }
}

fn slot_literal_expr(op: BinaryOp, slot: usize, literal: Value) -> CompiledExpr {
    CompiledExpr::BinarySlotLiteral {
        op,
        slot,
        field: format!("slot_{slot}"),
        literal,
        slot_on_left: true,
    }
}

fn case_slot_literal_expr(
    op: BinaryOp,
    slot: usize,
    then_expr: CompiledExpr,
    else_expr: CompiledExpr,
) -> CompiledExpr {
    CompiledExpr::CaseSlotLiteral {
        op,
        slot,
        field: format!("slot_{slot}"),
        literal: Value::Nat64(5),
        slot_on_left: true,
        then_expr: Box::new(then_expr),
        else_expr: Box::new(else_expr),
    }
}

fn case_slot_bool_expr(
    slot: usize,
    then_expr: CompiledExpr,
    else_expr: CompiledExpr,
) -> CompiledExpr {
    CompiledExpr::CaseSlotBool {
        slot,
        field: format!("slot_{slot}"),
        then_expr: Box::new(then_expr),
        else_expr: Box::new(else_expr),
    }
}

fn function_expr(function: Function, args: Vec<CompiledExpr>) -> CompiledExpr {
    CompiledExpr::FunctionCall {
        function,
        args: args.into_boxed_slice(),
    }
}

fn unary_not_expr(expr: CompiledExpr) -> CompiledExpr {
    CompiledExpr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(expr),
    }
}

fn generic_case_expr(
    condition: CompiledExpr,
    then_expr: CompiledExpr,
    else_expr: CompiledExpr,
) -> CompiledExpr {
    CompiledExpr::Case {
        when_then_arms: vec![super::CompiledExprCaseArm::new(condition, then_expr)]
            .into_boxed_slice(),
        else_expr: Box::new(else_expr),
    }
}

fn generic_binary_expr(op: BinaryOp, left: CompiledExpr, right: CompiledExpr) -> CompiledExpr {
    CompiledExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn assert_referenced_slots(expr: &CompiledExpr, expected: &[usize], context: &str) {
    let mut actual = Vec::new();
    expr.extend_referenced_slots(&mut actual);

    assert_eq!(
        actual, expected,
        "{context} should advertise every row slot it may evaluate",
    );
}

fn assert_evaluation_reads_are_advertised(expr: &CompiledExpr, context: &str) {
    let row_view = tracing_row_view();
    let _ = expr
        .evaluate(&row_view)
        .unwrap_or_else(|err| panic!("{context} should evaluate: {err:?}"));
    let mut advertised = Vec::new();
    expr.extend_referenced_slots(&mut advertised);
    let actual_reads = row_view.read_slots.borrow();

    for &slot in actual_reads.iter() {
        assert!(
            advertised.contains(&slot),
            "{context} read slot {slot} without advertising it in referenced slots: {advertised:?}",
        );
    }
}

#[test]
fn grouped_compiled_expr_reads_slots_without_cloning_contract_drift() {
    let expr = CompiledExpr::Slot {
        slot: 0,
        field: "age".to_string(),
    };

    assert_eq!(evaluate(&expr), Value::Nat64(7));
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
        value.cmp_numeric(&Value::Int64(10)),
        Some(Ordering::Equal),
        "direct slot arithmetic must preserve shared numeric coercion semantics",
    );
}

#[test]
fn compiled_expr_referenced_slot_matrix_covers_row_slot_variants() {
    for (context, expr, expected) in [
        ("slot", slot_expr(0), vec![0]),
        ("field-path", field_path_expr(3), vec![3]),
        (
            "group-key",
            CompiledExpr::GroupKey {
                offset: 0,
                field: "tier".to_string(),
            },
            vec![],
        ),
        ("aggregate", CompiledExpr::Aggregate { index: 0 }, vec![]),
        ("literal", CompiledExpr::Literal(Value::Nat64(1)), vec![]),
    ] {
        assert_referenced_slots(&expr, expected.as_slice(), context);
    }

    for (context, op) in [
        ("direct add", BinaryOp::Add),
        ("direct sub", BinaryOp::Sub),
        ("direct mul", BinaryOp::Mul),
        ("direct div", BinaryOp::Div),
        ("direct eq", BinaryOp::Eq),
        ("direct ne", BinaryOp::Ne),
        ("direct lt", BinaryOp::Lt),
        ("direct lte", BinaryOp::Lte),
        ("direct gt", BinaryOp::Gt),
        ("direct gte", BinaryOp::Gte),
    ] {
        assert_referenced_slots(&direct_slot_binary_expr(op, 0, 1), &[0, 1], context);
    }

    for (context, expr, expected) in [
        (
            "slot literal",
            slot_literal_expr(BinaryOp::Gt, 0, Value::Nat64(5)),
            vec![0],
        ),
        (
            "case slot literal",
            case_slot_literal_expr(BinaryOp::Gt, 0, slot_expr(2), slot_expr(3)),
            vec![0, 2, 3],
        ),
        (
            "case slot bool",
            case_slot_bool_expr(4, slot_expr(0), slot_expr(1)),
            vec![4, 0, 1],
        ),
        (
            "function",
            function_expr(Function::Lower, vec![slot_expr(3)]),
            vec![3],
        ),
        ("unary", unary_not_expr(slot_expr(4)), vec![4]),
        (
            "case",
            generic_case_expr(
                slot_literal_expr(BinaryOp::Gt, 0, Value::Nat64(5)),
                slot_expr(2),
                slot_expr(3),
            ),
            vec![0, 2, 3],
        ),
        (
            "binary",
            generic_binary_expr(BinaryOp::And, slot_expr(4), slot_expr(5)),
            vec![4, 5],
        ),
    ] {
        assert_referenced_slots(&expr, expected.as_slice(), context);
    }
}

#[test]
fn compiled_expr_evaluation_reads_are_subset_of_referenced_slots() {
    let cases = [
        ("slot", slot_expr(0)),
        ("field-path", field_path_expr(3)),
        ("direct add", direct_slot_binary_expr(BinaryOp::Add, 0, 1)),
        ("direct gt", direct_slot_binary_expr(BinaryOp::Gt, 0, 1)),
        (
            "slot literal",
            slot_literal_expr(BinaryOp::Gt, 0, Value::Nat64(5)),
        ),
        (
            "case slot literal then branch",
            case_slot_literal_expr(BinaryOp::Gt, 0, slot_expr(6), slot_expr(7)),
        ),
        (
            "case slot literal else branch",
            case_slot_literal_expr(BinaryOp::Lt, 0, slot_expr(6), slot_expr(7)),
        ),
        (
            "case slot bool then branch",
            case_slot_bool_expr(4, slot_expr(6), slot_expr(7)),
        ),
        (
            "case slot bool else branch",
            case_slot_bool_expr(5, slot_expr(6), slot_expr(7)),
        ),
        (
            "function",
            function_expr(Function::Lower, vec![slot_expr(3)]),
        ),
        ("unary", unary_not_expr(slot_expr(4))),
        (
            "case",
            generic_case_expr(
                slot_literal_expr(BinaryOp::Gt, 0, Value::Nat64(5)),
                slot_expr(6),
                slot_expr(7),
            ),
        ),
        (
            "binary",
            generic_binary_expr(BinaryOp::And, slot_expr(4), slot_expr(5)),
        ),
    ];

    for (context, expr) in cases {
        assert_evaluation_reads_are_advertised(&expr, context);
    }
}

#[cfg(feature = "sql")]
#[test]
fn compiled_expr_contains_field_path_matrix_reaches_child_expressions() {
    let cases = [
        (
            "function",
            function_expr(
                Function::Coalesce,
                vec![field_path_expr(0), CompiledExpr::Literal(Value::Null)],
            ),
        ),
        ("unary", unary_not_expr(field_path_expr(0))),
        (
            "binary",
            generic_binary_expr(
                BinaryOp::Eq,
                field_path_expr(0),
                CompiledExpr::Literal(Value::Nat64(1)),
            ),
        ),
        (
            "case",
            generic_case_expr(
                CompiledExpr::Literal(Value::Bool(true)),
                field_path_expr(0),
                CompiledExpr::Literal(Value::Null),
            ),
        ),
        (
            "case slot literal",
            case_slot_literal_expr(
                BinaryOp::Gt,
                0,
                field_path_expr(1),
                CompiledExpr::Literal(Value::Null),
            ),
        ),
        (
            "case slot bool",
            case_slot_bool_expr(4, CompiledExpr::Literal(Value::Null), field_path_expr(1)),
        ),
    ];

    for (context, expr) in cases {
        assert!(
            expr.contains_field_path(),
            "{context} should report child field-path expressions",
        );
    }
    assert!(
        !slot_expr(0).contains_field_path(),
        "plain slots should not report nested field paths",
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
                    literal: Value::Nat64(5),
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
        literal: Value::Nat64(5),
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
fn compiled_expr_case_slot_literal_references_condition_and_branch_slots() {
    let expr = CompiledExpr::CaseSlotLiteral {
        op: BinaryOp::Gt,
        slot: 0,
        field: "score".to_string(),
        literal: Value::Nat64(10),
        slot_on_left: true,
        then_expr: Box::new(CompiledExpr::Slot {
            slot: 2,
            field: "min_score".to_string(),
        }),
        else_expr: Box::new(CompiledExpr::Slot {
            slot: 3,
            field: "max_score".to_string(),
        }),
    };
    let mut slots = Vec::new();
    expr.extend_referenced_slots(&mut slots);

    assert_eq!(
        slots,
        vec![0, 2, 3],
        "specialized CASE slot/literal predicates must retain branch field reads in row layouts",
    );
}

#[test]
fn compiled_expr_case_slot_bool_references_condition_and_branch_slots() {
    let expr = CompiledExpr::CaseSlotBool {
        slot: 4,
        field: "active".to_string(),
        then_expr: Box::new(CompiledExpr::Slot {
            slot: 0,
            field: "score".to_string(),
        }),
        else_expr: Box::new(CompiledExpr::Slot {
            slot: 1,
            field: "fallback".to_string(),
        }),
    };
    let mut required = [false; 5];
    expr.mark_referenced_slots(&mut required);

    assert_eq!(
        required,
        [true, true, false, false, true],
        "specialized CASE boolean predicates must retain branch field reads in row layouts",
    );
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
fn grouped_compiled_expr_missing_slot_keeps_compact_diagnostic() {
    let expr = CompiledExpr::Slot {
        slot: 99,
        field: "missing_field".to_string(),
    };
    let err = expr
        .evaluate(&row_view())
        .expect_err("missing grouped slot should stay a projection error");

    assert_eq!(
        err,
        ProjectionEvalError::MissingFieldValue {
            access: ProjectionAccessCode::SLOT,
            index: 99,
        }
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
        ProjectionEvalError::MissingGroupedAggregateValue { index: 0 }
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
            access: ProjectionAccessCode::GROUP_KEY,
            index: 0,
        }
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
            access: ProjectionAccessCode::SLOT,
            index: 0,
        }
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

    std::assert_matches!(
        group_key.evaluate(&grouped_view),
        Err(ProjectionEvalError::MissingFieldValue {
            access: ProjectionAccessCode::GROUP_KEY,
            index: 9,
        })
    );
    std::assert_matches!(
        aggregate.evaluate(&grouped_view),
        Err(ProjectionEvalError::MissingGroupedAggregateValue { index: 9 })
    );
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
        ProjectionEvalError::MissingGroupedAggregateValue { index: 0 }
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
