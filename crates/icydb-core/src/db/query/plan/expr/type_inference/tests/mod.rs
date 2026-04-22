//! Module: db::query::plan::expr::type_inference::tests
//! Covers expression type inference behavior for planner-owned expressions.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        query::{
            builder::aggregate::{AggregateExpr, min, min_by, sum},
            plan::{
                AggregateKind, PlanError, PlanUserError,
                expr::{BinaryOp, CaseWhenArm, Expr, FieldId, Function},
                validate::ExprPlanError,
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    value::Value,
};

use super::{
    ExprCoarseTypeFamily, ExprType, NumericSubtype, UnaryOp, dynamic_function_arg_coarse_family,
    function_arg_coarse_family, function_is_compare_operand_coarse_family,
    function_result_coarse_family, infer_case_result_exprs_coarse_family,
    infer_dynamic_function_result_exprs_coarse_family, infer_expr_coarse_family, infer_expr_type,
};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::generated(
    "query::plan::expr::idx_empty",
    "query::plan::expr::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

crate::test_entity! {
    ident = ExprInferenceEntity,
    id = crate::types::Ulid,
    entity_name = "ExprInferenceEntity",
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

fn schema() -> &'static SchemaInfo {
    let model: &'static EntityModel = <ExprInferenceEntity as crate::traits::EntitySchema>::MODEL;
    SchemaInfo::cached_for_entity_model(model)
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

    let inferred = infer_expr_type(&expr, schema).expect("field should infer");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Integer));
}

#[test]
fn infer_literal_type_is_deterministic() {
    let schema = schema();
    let expr = Expr::Literal(Value::Bool(true));
    let duration_expr = Expr::Literal(Value::Duration(crate::types::Duration::from_millis(5)));

    let inferred = infer_expr_type(&expr, schema).expect("literal should infer");
    let duration_inferred =
        infer_expr_type(&duration_expr, schema).expect("duration literal should infer");

    assert_eq!(inferred, ExprType::Bool);
    assert_eq!(
        duration_inferred,
        ExprType::Numeric(NumericSubtype::Integer)
    );
}

#[test]
fn infer_expr_coarse_family_projects_planner_types_for_boundary_consumers() {
    let schema = schema();
    let bool_expr = Expr::Literal(Value::Bool(true));
    let text_expr = Expr::Field(FieldId::new("label"));
    let numeric_expr = Expr::Field(FieldId::new("rank"));

    assert_eq!(
        infer_expr_coarse_family(&bool_expr, schema).expect("bool coarse family should infer"),
        Some(ExprCoarseTypeFamily::Bool),
    );
    assert_eq!(
        infer_expr_coarse_family(&text_expr, schema).expect("text coarse family should infer"),
        Some(ExprCoarseTypeFamily::Text),
    );
    assert_eq!(
        infer_expr_coarse_family(&numeric_expr, schema)
            .expect("numeric coarse family should infer"),
        Some(ExprCoarseTypeFamily::Numeric),
    );
}

#[test]
fn function_arg_coarse_family_matches_shared_scalar_signature_contracts() {
    assert_eq!(
        function_arg_coarse_family(Function::Lower, 0),
        Some(ExprCoarseTypeFamily::Text),
    );
    assert_eq!(
        function_arg_coarse_family(Function::Substring, 1),
        Some(ExprCoarseTypeFamily::Numeric),
    );
    assert_eq!(function_arg_coarse_family(Function::Coalesce, 0), None);
}

#[test]
fn function_result_coarse_family_matches_shared_scalar_signature_contracts() {
    assert_eq!(
        function_result_coarse_family(Function::Contains),
        Some(ExprCoarseTypeFamily::Bool),
    );
    assert_eq!(
        function_result_coarse_family(Function::Length),
        Some(ExprCoarseTypeFamily::Numeric),
    );
    assert_eq!(
        function_result_coarse_family(Function::Trim),
        Some(ExprCoarseTypeFamily::Text),
    );
    assert_eq!(function_result_coarse_family(Function::NullIf), None);
}

#[test]
fn dynamic_function_arg_coarse_family_reuses_resolved_result_family() {
    assert_eq!(
        dynamic_function_arg_coarse_family(Function::Coalesce, ExprCoarseTypeFamily::Numeric),
        Some(ExprCoarseTypeFamily::Numeric),
    );
    assert_eq!(
        dynamic_function_arg_coarse_family(Function::NullIf, ExprCoarseTypeFamily::Text),
        Some(ExprCoarseTypeFamily::Text),
    );
    assert_eq!(
        dynamic_function_arg_coarse_family(Function::Lower, ExprCoarseTypeFamily::Text),
        None,
    );
}

#[test]
fn function_is_compare_operand_coarse_family_matches_shared_signature_table() {
    assert!(function_is_compare_operand_coarse_family(Function::Lower));
    assert!(function_is_compare_operand_coarse_family(Function::Length));
    assert!(function_is_compare_operand_coarse_family(
        Function::Coalesce
    ));
    assert!(function_is_compare_operand_coarse_family(Function::NullIf));
    assert!(!function_is_compare_operand_coarse_family(
        Function::Contains
    ));
    assert!(!function_is_compare_operand_coarse_family(Function::IsNull));
}

#[test]
fn infer_binary_numeric_expr_requires_numeric_operands() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Uint(7))),
    };

    let inferred = infer_expr_type(&expr, schema).expect("numeric addition should infer");

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

    let err = infer_expr_type(&expr, schema)
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

    let err = infer_expr_type(&expr, schema)
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

    let err = infer_expr_type(&expr, schema)
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

    let err = infer_expr_type(&expr, schema)
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
        infer_expr_type(&expr, schema).expect("mixed numeric addition should stay numeric");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Unknown));
}

#[test]
fn infer_binary_boolean_or_returns_bool() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Field(FieldId::new("flag"))),
        right: Box::new(Expr::Literal(Value::Bool(false))),
    };

    let inferred = infer_expr_type(&expr, schema).expect("boolean or should infer");

    assert_eq!(inferred, ExprType::Bool);
}

#[test]
fn infer_binary_order_compare_over_numeric_expr_returns_bool() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(5))),
    };

    let inferred = infer_expr_type(&expr, schema).expect("numeric comparison should infer");

    assert_eq!(inferred, ExprType::Bool);
}

#[test]
fn infer_searched_case_returns_shared_branch_type() {
    let schema = schema();
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Field(FieldId::new("flag")),
            Expr::Literal(Value::Int(1)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Uint(0))),
    };

    let inferred = infer_expr_type(&expr, schema).expect("searched CASE should infer");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Integer));
}

#[test]
fn infer_case_result_exprs_coarse_family_uses_planner_branch_unification() {
    let schema = schema();
    let result_exprs = [
        Expr::Literal(Value::Int(1)),
        Expr::Literal(Value::Uint(0)),
        Expr::Literal(Value::Null),
    ];

    let inferred = infer_case_result_exprs_coarse_family(result_exprs.iter(), schema)
        .expect("CASE result branches should project one shared coarse family");

    assert_eq!(inferred, Some(ExprCoarseTypeFamily::Numeric));
}

#[test]
fn infer_dynamic_function_result_exprs_coarse_family_uses_planner_unification() {
    let schema = schema();
    let args = [
        Expr::Literal(Value::Int(1)),
        Expr::Literal(Value::Uint(0)),
        Expr::Literal(Value::Null),
    ];

    let inferred =
        infer_dynamic_function_result_exprs_coarse_family(Function::Coalesce, &args, schema)
            .expect("COALESCE result family should infer from lowerable planner arguments");

    assert_eq!(inferred, Some(ExprCoarseTypeFamily::Numeric));
}

#[test]
fn infer_searched_case_rejects_non_boolean_conditions() {
    let schema = schema();
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Field(FieldId::new("rank")),
            Expr::Literal(Value::Int(1)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Int(0))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("searched CASE must reject non-boolean branch conditions");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidCaseConditionType { .. }
    )));
}

#[test]
fn infer_searched_case_rejects_incompatible_branch_types() {
    let schema = schema();
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Field(FieldId::new("flag")),
            Expr::Literal(Value::Text("yes".to_string())),
        )],
        else_expr: Box::new(Expr::Literal(Value::Int(0))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("searched CASE must reject incompatible result branches");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::IncompatibleCaseBranchTypes { .. }
    )));
}

#[test]
fn infer_binary_numeric_expr_rejects_unknown_non_eligible_operands() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(min())),
        right: Box::new(Expr::Literal(Value::Int(1))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("unknown type does not imply numeric eligibility");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
    ));
}

#[test]
fn infer_round_function_expr_returns_decimal_for_numeric_input() {
    let schema = schema();
    let expr = Expr::FunctionCall {
        function: crate::db::query::plan::expr::Function::Round,
        args: vec![
            Expr::Field(FieldId::new("rank")),
            Expr::Literal(Value::Uint(2)),
        ],
    };

    let inferred = infer_expr_type(&expr, schema).expect("ROUND(rank, 2) should infer");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Decimal));
}

#[test]
fn infer_round_function_expr_rejects_non_numeric_input() {
    let schema = schema();
    let expr = Expr::FunctionCall {
        function: crate::db::query::plan::expr::Function::Round,
        args: vec![
            Expr::Field(FieldId::new("label")),
            Expr::Literal(Value::Uint(2)),
        ],
    };

    let err = infer_expr_type(&expr, schema).expect_err("ROUND(text, 2) should fail closed");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidFunctionArgument { function, index, .. } if function == "ROUND" && *index == 0)
    ));
}

#[test]
fn infer_sum_aggregate_rejects_decidable_non_numeric_bool_target() {
    let schema = schema();
    let expr = Expr::Aggregate(sum("flag"));

    let err = infer_expr_type(&expr, schema).expect_err("sum over bool should fail");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::NonNumericAggregateTarget { field, .. } if field == "flag")
    ));
}

#[test]
fn infer_min_by_aggregate_keeps_existing_non_numeric_semantics() {
    let schema = schema();
    let expr = Expr::Aggregate(min_by("label"));

    let inferred = infer_expr_type(&expr, schema).expect("min_by(text) should remain valid");

    assert_eq!(inferred, ExprType::Text);
}

#[test]
fn infer_sum_aggregate_requires_numeric_target() {
    let schema = schema();
    let expr = Expr::Aggregate(sum("label"));

    let err = infer_expr_type(&expr, schema).expect_err("sum over text should fail");
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

    let err = infer_expr_type(&expr, schema).expect_err("sum without target should fail");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::AggregateTargetRequired { kind } if kind == "sum")
    ));
}

#[test]
fn infer_avg_aggregate_over_numeric_expression_uses_expression_result_type() {
    let schema = schema();
    let expr = Expr::Aggregate(AggregateExpr::from_expression_input(
        AggregateKind::Avg,
        Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        },
    ));

    let inferred =
        infer_expr_type(&expr, schema).expect("avg over numeric input expression should infer");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Unknown));
}

#[test]
fn infer_sum_aggregate_rejects_non_numeric_expression_target() {
    let schema = schema();
    let expr = Expr::Aggregate(AggregateExpr::from_expression_input(
        AggregateKind::Sum,
        Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("label"))),
        },
    ));

    let err = infer_expr_type(&expr, schema)
        .expect_err("sum over non-numeric input expression should fail");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
    ));
}

#[test]
fn infer_unary_bool_not_rejects_non_bool_operands() {
    let schema = schema();
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Field(FieldId::new("rank"))),
    };

    let err = infer_expr_type(&expr, schema).expect_err("not over numeric field should fail");
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

    let err = infer_expr_type(&expr, schema)
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

    let err = infer_expr_type(&expr, schema)
        .expect_err("unknown aggregate operand comparison should fail closed");
    assert!(is_expr_plan_error(
        &err,
        |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "eq")
    ));
}
