use super::{eval_builder_expr_for_value_preview, eval_projection_function_call};
use crate::{
    db::{
        QueryError,
        query::plan::expr::{
            BinaryOp, CaseWhenArm, CompiledExpr, CompiledExprCaseArm, CompiledExprValueReader,
            Expr, FieldId, FieldPath, Function, UnaryOp,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticCode, DiagnosticDetail, QueryProjectionCode};
use std::borrow::Cow;

struct PreviewParityRow {
    value: Value,
}

impl CompiledExprValueReader for PreviewParityRow {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        (slot == 0).then_some(Cow::Borrowed(&self.value))
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }
}

fn assert_projection_reason(err: QueryError, reason: QueryProjectionCode) {
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryUnsupportedProjection
    );
    assert_eq!(
        diagnostic.detail().copied(),
        Some(DiagnosticDetail::QueryProjection { reason })
    );
}

fn assert_preview_matches_compiled(
    expr: &Expr,
    compiled_expr: &CompiledExpr,
    field_name: &str,
    value: Value,
    expected: Value,
) {
    let preview = eval_builder_expr_for_value_preview(expr, field_name, &value)
        .expect("preview expression should evaluate");
    let compiled = compiled_expr
        .evaluate(&PreviewParityRow { value })
        .expect("compiled expression should evaluate")
        .into_owned();

    assert_eq!(preview, expected);
    assert_eq!(compiled, expected);
}

fn age_eq_null_expr() -> Expr {
    Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("age"))),
        right: Box::new(Expr::Literal(Value::Null)),
    }
}

fn compiled_age_eq_null() -> CompiledExpr {
    CompiledExpr::BinarySlotLiteral {
        op: BinaryOp::Eq,
        slot: 0,
        field: "age".to_string(),
        literal: Value::Null,
        slot_on_left: true,
    }
}

#[test]
fn preview_rejects_nested_field_path_with_compact_projection_code() {
    let err = eval_builder_expr_for_value_preview(
        &Expr::FieldPath(FieldPath::new("profile", vec!["name".to_string()])),
        "profile",
        &Value::Text("wizard".to_string()),
    )
    .expect_err("nested field-path preview should reject");

    assert_projection_reason(err, QueryProjectionCode::NestedFieldPathPreview);
}

#[test]
fn preview_eval_matches_compiled_expr_for_null_boolean_composition() {
    assert_preview_matches_compiled(
        &Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(age_eq_null_expr()),
        },
        &CompiledExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(compiled_age_eq_null()),
        },
        "age",
        Value::Nat64(20),
        Value::Null,
    );
    assert_preview_matches_compiled(
        &Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(age_eq_null_expr()),
            right: Box::new(Expr::Literal(Value::Bool(true))),
        },
        &CompiledExpr::Binary {
            op: BinaryOp::Or,
            left: Box::new(compiled_age_eq_null()),
            right: Box::new(CompiledExpr::Literal(Value::Bool(true))),
        },
        "age",
        Value::Nat64(20),
        Value::Bool(true),
    );
    assert_preview_matches_compiled(
        &Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(age_eq_null_expr()),
            right: Box::new(Expr::Literal(Value::Bool(true))),
        },
        &CompiledExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(compiled_age_eq_null()),
            right: Box::new(CompiledExpr::Literal(Value::Bool(true))),
        },
        "age",
        Value::Nat64(20),
        Value::Null,
    );
}

#[test]
fn preview_eval_matches_compiled_expr_for_case_truth_admission() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            age_eq_null_expr(),
            Expr::Literal(Value::Text("then".to_string())),
        )],
        else_expr: Box::new(Expr::Literal(Value::Text("else".to_string()))),
    };
    let compiled_expr = CompiledExpr::Case {
        when_then_arms: vec![CompiledExprCaseArm::new(
            compiled_age_eq_null(),
            CompiledExpr::Literal(Value::Text("then".to_string())),
        )]
        .into_boxed_slice(),
        else_expr: Box::new(CompiledExpr::Literal(Value::Text("else".to_string()))),
    };

    assert_preview_matches_compiled(
        &expr,
        &compiled_expr,
        "age",
        Value::Nat64(20),
        Value::Text("else".to_string()),
    );
}

#[test]
fn preview_rejects_non_boolean_case_condition_with_compact_projection_code() {
    let err = eval_builder_expr_for_value_preview(
        &Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Literal(Value::Text("truthy".to_string())),
                Expr::Literal(Value::Text("yes".to_string())),
            )],
            else_expr: Box::new(Expr::Literal(Value::Text("no".to_string()))),
        },
        "label",
        &Value::Text("source".to_string()),
    )
    .expect_err("non-boolean CASE condition should reject");

    assert_projection_reason(err, QueryProjectionCode::CaseConditionBooleanRequired);
}

#[test]
fn function_eval_rejects_numeric_input_mismatch_with_compact_projection_code() {
    let err =
        eval_projection_function_call(Function::Abs, &[Value::Text("not-number".to_string())])
            .expect_err("numeric function should reject text input");

    assert_projection_reason(err, QueryProjectionCode::NumericInputRequired);
}

#[test]
fn function_eval_rejects_text_or_blob_input_mismatch_with_compact_projection_code() {
    let err = eval_projection_function_call(Function::OctetLength, &[Value::Int64(42)])
        .expect_err("octet_length should reject non-text/non-blob input");

    assert_projection_reason(err, QueryProjectionCode::TextOrBlobInputRequired);
}

#[test]
fn function_eval_rejects_text_input_mismatch_with_compact_projection_code() {
    let err = eval_projection_function_call(Function::Lower, &[Value::Int64(42)])
        .expect_err("lower should reject non-text input");

    assert_projection_reason(err, QueryProjectionCode::TextInputRequired);
}

#[test]
fn function_eval_rejects_text_literal_mismatch_with_compact_projection_code() {
    let err = eval_projection_function_call(
        Function::StartsWith,
        &[Value::Text("wizard".to_string()), Value::Int64(7)],
    )
    .expect_err("starts_with should reject non-text literal argument");

    assert_projection_reason(err, QueryProjectionCode::TextOrNullArgumentRequired);
}

#[test]
fn function_eval_rejects_integer_literal_mismatch_with_compact_projection_code() {
    let err = eval_projection_function_call(
        Function::Left,
        &[Value::Text("wizard".to_string()), Value::Bool(true)],
    )
    .expect_err("left should reject non-integer literal argument");

    assert_projection_reason(err, QueryProjectionCode::IntegerOrNullArgumentRequired);
}

#[test]
fn function_eval_rejects_numeric_scale_mismatch_with_compact_projection_code() {
    let err = eval_projection_function_call(Function::Round, &[Value::Int64(42), Value::Int64(-1)])
        .expect_err("round should reject negative scale argument");

    assert_projection_reason(err, QueryProjectionCode::NumericScaleArguments);
}

#[test]
fn preview_rejects_unary_operand_mismatch_with_compact_projection_code() {
    let err = eval_builder_expr_for_value_preview(
        &Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Literal(Value::Int64(1))),
        },
        "active",
        &Value::Bool(true),
    )
    .expect_err("not should reject non-boolean operand");

    assert_projection_reason(err, QueryProjectionCode::UnaryOperandIncompatible);
}

#[test]
fn preview_rejects_binary_operand_mismatch_with_compact_projection_code() {
    let err = eval_builder_expr_for_value_preview(
        &Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Text("left".to_string()))),
            right: Box::new(Expr::Literal(Value::Text("right".to_string()))),
        },
        "rank",
        &Value::Int64(1),
    )
    .expect_err("add should reject non-numeric operands");

    assert_projection_reason(err, QueryProjectionCode::BinaryOperandsIncompatible);
}

#[test]
fn preview_allows_valid_binary_projection_after_compact_error_split() {
    let value = eval_builder_expr_for_value_preview(
        &Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Int64(2))),
            right: Box::new(Expr::Literal(Value::Int64(3))),
        },
        "rank",
        &Value::Int64(1),
    )
    .expect("valid numeric projection should still evaluate");

    assert_eq!(value, Value::Decimal(5.into()));
}
