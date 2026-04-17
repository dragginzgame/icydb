use super::*;
use crate::db::query::plan::expr::{CaseWhenArm, UnaryOp};

#[test]
fn eval_expr_supports_arithmetic_projection() {
    let (_, entity) = row(1, 7, true);
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(1))),
    };

    let value = eval_scalar_expr_for_row(&expr, &entity)
        .expect("numeric projection expression should evaluate");

    assert_eq!(
        value.cmp_numeric(&Value::Int(8)),
        Some(Ordering::Equal),
        "arithmetic projection must preserve numeric semantics",
    );
}

#[test]
fn scalar_projection_expr_matches_generic_eval_for_arithmetic_projection() {
    let (_, entity) = row(7, 41, true);
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(1))),
    };
    let value = eval_scalar_expr_for_row(&expr, &entity)
        .expect("scalar arithmetic projection should evaluate");

    assert_eq!(
        value.cmp_numeric(&Value::Int(42)),
        Some(Ordering::Equal),
        "compiled scalar projection should preserve arithmetic projection semantics",
    );
}

#[test]
fn required_projection_eval_preserves_internal_slot_errors() {
    let expr = Expr::Field(FieldId::new("rank"));
    let err = eval_canonical_scalar_expr_with_required_reader(&expr, &mut |_| {
        Err(InternalError::persisted_row_declared_field_missing("rank"))
    })
    .expect_err("required projection evaluation should preserve structural slot errors");

    assert_eq!(err.class(), ErrorClass::Corruption);
    assert_eq!(err.origin(), ErrorOrigin::Serialize);
}

#[test]
fn canonical_scalar_projection_preserves_missing_declared_slot_corruption() {
    let expr = Expr::Field(FieldId::new("rank"));
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, &expr)
        .expect("rank field should compile onto scalar seam");
    let err =
        eval_canonical_scalar_projection_expr(&compiled, &ProjectionMissingDeclaredSlotReader)
            .expect_err("canonical scalar projection should fail closed on missing declared slot");

    assert_eq!(err.class(), ErrorClass::Corruption);
    assert_eq!(err.origin(), ErrorOrigin::Serialize);
}

#[test]
fn eval_expr_supports_boolean_projection() {
    let (_, entity) = row(2, 3, true);
    let expr = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Field(FieldId::new("flag"))),
        right: Box::new(Expr::Literal(Value::Bool(true))),
    };

    let value = eval_scalar_expr_for_row(&expr, &entity)
        .expect("boolean projection expression should evaluate");

    assert_eq!(value, Value::Bool(true));
}

#[test]
fn eval_expr_supports_unary_not_projection() {
    let (_, entity) = row(21, 3, false);
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Field(FieldId::new("flag"))),
    };

    let value =
        eval_scalar_expr_for_row(&expr, &entity).expect("unary boolean projection should evaluate");

    assert_eq!(value, Value::Bool(true));
}

#[test]
fn eval_expr_supports_searched_case_projection() {
    let (_, entity) = row(21, 7, true);
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(5))),
            },
            Expr::Literal(Value::Text("high".to_string())),
        )],
        else_expr: Box::new(Expr::Literal(Value::Text("low".to_string()))),
    };

    let value =
        eval_scalar_expr_for_row(&expr, &entity).expect("searched CASE projection should evaluate");

    assert_eq!(value, Value::Text("high".to_string()));
}

#[test]
fn eval_expr_keeps_searched_case_branches_lazy() {
    let (_, entity) = row(22, 3, true);
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Literal(Value::Bool(false)),
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("label"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
        )],
        else_expr: Box::new(Expr::Literal(Value::Text("fallback".to_string()))),
    };

    let value = eval_scalar_expr_for_row(&expr, &entity)
        .expect("searched CASE should not evaluate non-selected branches");

    assert_eq!(value, Value::Text("fallback".to_string()));
}

#[test]
fn eval_expr_supports_numeric_equality_widening() {
    let (_, entity) = row(12, 7, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Uint(7))),
    };

    let value = eval_scalar_expr_for_row(&expr, &entity).expect("numeric equality should widen");

    assert_eq!(value, Value::Bool(true));
}

#[test]
fn eval_expr_supports_numeric_order_comparison() {
    let (_, entity) = row(22, 7, true);
    let expr = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(5))),
    };

    let value =
        eval_scalar_expr_for_row(&expr, &entity).expect("numeric comparison should evaluate");

    assert_eq!(value, Value::Bool(true));
}

#[test]
fn eval_expr_rejects_numeric_and_non_numeric_equality_mix() {
    let (_, entity) = row(13, 7, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("label"))),
    };

    let err = eval_scalar_expr_for_row(&expr, &entity)
        .expect_err("mixed numeric/non-numeric equality should fail invariant checks");
    assert_eq!(err.class(), ErrorClass::InvariantViolation);
    assert_eq!(err.origin(), ErrorOrigin::Planner);
    assert!(
        err.message
            .contains("projection binary operator 'eq' is incompatible"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn eval_expr_propagates_null_values() {
    let (_, entity) = row(3, 5, false);
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Null)),
    };

    let value = eval_scalar_expr_for_row(&expr, &entity)
        .expect("null propagation should remain deterministic");

    assert_eq!(value, Value::Null);
}

#[test]
fn eval_expr_alias_wrapper_is_semantic_no_op() {
    let (_, entity) = row(4, 11, true);
    let plain = Expr::Field(FieldId::new("rank"));
    let aliased = Expr::Alias {
        expr: Box::new(Expr::Field(FieldId::new("rank"))),
        name: Alias::new("rank_alias"),
    };

    let plain_value =
        eval_scalar_expr_for_row(&plain, &entity).expect("plain field expression should evaluate");
    let alias_value = eval_scalar_expr_for_row(&aliased, &entity)
        .expect("aliased expression should evaluate identically");

    assert_eq!(plain_value, alias_value);
}
