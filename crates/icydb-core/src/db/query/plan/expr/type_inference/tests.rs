//! Module: db::query::plan::expr::type_inference::tests
//! Responsibility: module-local ownership and contracts for db::query::plan::expr::type_inference::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        query::{
            builder::aggregate::{AggregateExpr, min, min_by, sum},
            plan::{AggregateKind, PlanError, PlanUserError, validate::ExprPlanError},
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    value::Value,
};

use super::{BinaryOp, Expr, ExprType, FieldId, NumericSubtype, UnaryOp, infer_expr_type};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::new(
    "query::plan::expr::idx_empty",
    "query::plan::expr::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

crate::test_entity! {
    ident = ExprInferenceEntity,
    id = crate::types::Ulid,
    entity_name = "ExprInferenceEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Uint),
        ("flag", FieldKind::Bool),
        ("label", FieldKind::Text),
        ("created_on", FieldKind::Date),
    ],
    indexes = [&EMPTY_INDEX],
}

fn schema() -> SchemaInfo {
    let model: &'static EntityModel = <ExprInferenceEntity as crate::traits::EntitySchema>::MODEL;
    SchemaInfo::from_entity_model(model).expect("schema should validate")
}

fn is_expr_plan_error(err: &PlanError, predicate: impl FnOnce(&ExprPlanError) -> bool) -> bool {
    matches!(
        err,
        PlanError::User(inner)
            if matches!(
                inner.as_ref(),
                PlanUserError::Expr(inner) if predicate(inner.as_ref())
            )
    )
}

#[test]
fn infer_field_type_uses_schema_field_kind() {
    let schema = schema();
    let expr = Expr::Field(FieldId::new("rank"));

    let inferred = infer_expr_type(&expr, &schema).expect("field should infer");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Integer));
}

#[test]
fn infer_literal_type_is_deterministic() {
    let schema = schema();
    let expr = Expr::Literal(Value::Bool(true));
    let duration_expr = Expr::Literal(Value::Duration(crate::types::Duration::from_millis(5)));

    let inferred = infer_expr_type(&expr, &schema).expect("literal should infer");
    let duration_inferred =
        infer_expr_type(&duration_expr, &schema).expect("duration literal should infer");

    assert_eq!(inferred, ExprType::Bool);
    assert_eq!(
        duration_inferred,
        ExprType::Numeric(NumericSubtype::Integer)
    );
}

#[test]
fn infer_binary_numeric_expr_requires_numeric_operands() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Uint(7))),
    };

    let inferred = infer_expr_type(&expr, &schema).expect("numeric addition should infer");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Integer));
}

#[test]
fn infer_binary_numeric_expr_rejects_decidable_non_numeric_schema_operand() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("label"))),
    };

    let err = infer_expr_type(&expr, &schema)
        .expect_err("numeric operators must reject schema-known non-numeric fields");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
    ));
}

#[test]
fn infer_binary_numeric_expr_rejects_decidable_non_numeric_bool_field_operand() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("flag"))),
    };

    let err = infer_expr_type(&expr, &schema)
        .expect_err("numeric operators must reject schema-known bool fields");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
    ));
}

#[test]
fn infer_binary_numeric_expr_rejects_decidable_non_numeric_date_field_operand() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("created_on"))),
    };

    let err = infer_expr_type(&expr, &schema)
        .expect_err("numeric operators must reject schema-known date fields");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
    ));
}

#[test]
fn infer_binary_numeric_expr_rejects_decidable_non_numeric_literal_operand() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Literal(Value::Bool(true))),
        right: Box::new(Expr::Literal(Value::Int(5))),
    };

    let err = infer_expr_type(&expr, &schema)
        .expect_err("numeric operators must reject non-numeric literal operands");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
    ));
}

#[test]
fn infer_binary_numeric_expr_keeps_numeric_with_unknown_subtype_for_mixed_operands() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Decimal(
            crate::types::Decimal::from_num(7_u64).expect("decimal literal"),
        ))),
    };

    let inferred =
        infer_expr_type(&expr, &schema).expect("mixed numeric addition should stay numeric");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Unknown));
}

#[test]
fn infer_binary_numeric_expr_rejects_unknown_non_eligible_operands() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(min())),
        right: Box::new(Expr::Literal(Value::Int(1))),
    };

    let err = infer_expr_type(&expr, &schema)
        .expect_err("unknown type does not imply numeric eligibility");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
    ));
}

#[test]
fn infer_sum_aggregate_rejects_decidable_non_numeric_bool_target() {
    let schema = schema();
    let expr = Expr::Aggregate(sum("flag"));

    let err = infer_expr_type(&expr, &schema).expect_err("sum over bool should fail");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::NonNumericAggregateTarget { field, .. } if field == "flag")
    ));
}

#[test]
fn infer_min_by_aggregate_keeps_existing_non_numeric_semantics() {
    let schema = schema();
    let expr = Expr::Aggregate(min_by("label"));

    let inferred = infer_expr_type(&expr, &schema).expect("min_by(text) should remain valid");

    assert_eq!(inferred, ExprType::Text);
}

#[test]
fn infer_sum_aggregate_requires_numeric_target() {
    let schema = schema();
    let expr = Expr::Aggregate(sum("label"));

    let err = infer_expr_type(&expr, &schema).expect_err("sum over text should fail");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::NonNumericAggregateTarget { field, .. } if field == "label")
    ));
}

#[test]
fn infer_sum_aggregate_without_target_rejects_missing_target() {
    let schema = schema();
    let expr = Expr::Aggregate(AggregateExpr::from_semantic_parts(
        AggregateKind::Sum,
        None,
        false,
    ));

    let err = infer_expr_type(&expr, &schema).expect_err("sum without target should fail");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::AggregateTargetRequired { kind } if kind == "sum")
    ));
}

#[test]
fn infer_unary_bool_not_rejects_non_bool_operands() {
    let schema = schema();
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Field(FieldId::new("rank"))),
    };

    let err = infer_expr_type(&expr, &schema).expect_err("not over numeric field should fail");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidUnaryOperand { op, .. } if op == "not")
    ));
}

#[test]
fn infer_binary_compare_rejects_incompatible_operand_types() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("label"))),
    };

    let err = infer_expr_type(&expr, &schema)
        .expect_err("numeric/text comparison should fail deterministic type inference");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "eq")
    ));
}

#[test]
fn infer_binary_compare_rejects_unknown_operands_fail_closed() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Aggregate(AggregateExpr::from_semantic_parts(
            AggregateKind::Min,
            None,
            false,
        ))),
        right: Box::new(Expr::Aggregate(AggregateExpr::from_semantic_parts(
            AggregateKind::Max,
            None,
            false,
        ))),
    };

    let err = infer_expr_type(&expr, &schema)
        .expect_err("unknown aggregate operand comparison should fail closed");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "eq")
    ));
}
