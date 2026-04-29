use super::*;
use crate::db::query::plan::expr::{CaseWhenArm, Function, UnaryOp};

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
fn canonical_scalar_projection_executes_simple_field_projection() {
    let (_, entity) = row(31, 23, true);
    let expr = Expr::Field(FieldId::new("label"));
    let value = eval_canonical_scalar_expr_for_row(&expr, &entity)
        .expect("plain scalar projection should evaluate");

    assert_eq!(value, Value::Text("label-31".to_string()));
}

#[test]
fn canonical_scalar_projection_executes_field_path_projection() {
    let (_, entity) = row(32, 29, true);
    let expr = Expr::FieldPath(FieldPath::new("profile", vec!["rank".to_string()]));
    let value = eval_canonical_scalar_expr_for_row(&expr, &entity)
        .expect("field-path projection should evaluate");

    assert_eq!(value, Value::Int(29));
}

#[test]
fn canonical_scalar_projection_returns_null_for_missing_field_path() {
    let (_, entity) = row(33, 31, false);
    let expr = Expr::FieldPath(FieldPath::new("profile", vec!["missing".to_string()]));
    let value = eval_canonical_scalar_expr_for_row(&expr, &entity)
        .expect("missing field-path projection should evaluate as null");

    assert_eq!(value, Value::Null);
}

#[test]
fn canonical_scalar_projection_fails_closed_for_non_map_path_root() {
    let (_, entity) = row(34, 37, true);
    let expr = Expr::FieldPath(FieldPath::new("label", vec!["rank".to_string()]));
    let err = eval_canonical_scalar_expr_for_row(&expr, &entity)
        .expect_err("non-map field-path roots should fail closed");

    assert_eq!(err.class(), ErrorClass::Corruption);
    assert_eq!(err.origin(), ErrorOrigin::Serialize);
}

#[test]
fn scalar_filter_expr_matches_field_path_value() {
    let (_, entity) = row(35, 41, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FieldPath(FieldPath::new(
            "profile",
            vec!["rank".to_string()],
        ))),
        right: Box::new(Expr::Literal(Value::Int(41))),
    };

    let admitted =
        eval_scalar_filter_expr_for_row(&expr, &entity).expect("field-path filter should evaluate");

    assert!(
        admitted,
        "matching field-path predicate should admit the row"
    );
}

#[test]
fn scalar_filter_expr_matches_text_field_path_value() {
    let (_, entity) = row(35, 41, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FieldPath(FieldPath::new(
            "profile",
            vec!["name".to_string()],
        ))),
        right: Box::new(Expr::Literal(Value::Text("profile-35".to_string()))),
    };

    let admitted = eval_scalar_filter_expr_for_row(&expr, &entity)
        .expect("text field-path filter should evaluate");

    assert!(
        admitted,
        "matching text field-path predicate should admit the row"
    );
}

#[test]
fn scalar_filter_expr_matches_uint_field_path_value() {
    let (_, entity) = row(35, 41, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FieldPath(FieldPath::new(
            "profile",
            vec!["score".to_string()],
        ))),
        right: Box::new(Expr::Literal(Value::Uint(41))),
    };

    let admitted = eval_scalar_filter_expr_for_row(&expr, &entity)
        .expect("uint field-path filter should evaluate");

    assert!(
        admitted,
        "matching uint field-path predicate should admit the row"
    );
}

#[test]
fn scalar_filter_expr_matches_bool_field_path_value() {
    let (_, entity) = row(35, 41, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FieldPath(FieldPath::new(
            "profile",
            vec!["details".to_string(), "flag".to_string()],
        ))),
        right: Box::new(Expr::Literal(Value::Bool(true))),
    };

    let admitted = eval_scalar_filter_expr_for_row(&expr, &entity)
        .expect("bool field-path filter should evaluate");

    assert!(
        admitted,
        "matching bool field-path predicate should admit the row"
    );
}

#[test]
fn scalar_filter_expr_rejects_missing_field_path() {
    let (_, entity) = row(36, 43, false);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FieldPath(FieldPath::new(
            "profile",
            vec!["missing".to_string()],
        ))),
        right: Box::new(Expr::Literal(Value::Int(43))),
    };

    let admitted = eval_scalar_filter_expr_for_row(&expr, &entity)
        .expect("missing field-path filter should evaluate as false");

    assert!(
        !admitted,
        "missing field-path predicate should reject the row"
    );
}

#[test]
fn scalar_filter_expr_does_not_treat_missing_field_path_as_null() {
    let (_, entity) = row(36, 43, false);
    let expr = Expr::FunctionCall {
        function: Function::IsNull,
        args: vec![Expr::FieldPath(FieldPath::new(
            "profile",
            vec!["missing".to_string()],
        ))],
    };

    let admitted = eval_scalar_filter_expr_for_row(&expr, &entity)
        .expect("missing field-path NULL test should evaluate as false");

    assert!(
        !admitted,
        "missing field-path predicate should reject before NULL-test semantics"
    );
}

#[test]
fn scalar_filter_expr_fails_closed_for_non_map_path_root() {
    let (_, entity) = row(37, 47, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FieldPath(FieldPath::new(
            "label",
            vec!["rank".to_string()],
        ))),
        right: Box::new(Expr::Literal(Value::Int(47))),
    };

    let err = eval_scalar_filter_expr_for_row(&expr, &entity)
        .expect_err("non-map field-path filters should fail closed");

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
