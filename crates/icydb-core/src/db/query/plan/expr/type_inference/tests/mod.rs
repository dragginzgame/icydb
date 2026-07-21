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
                expr::{BinaryOp, CaseWhenArm, Expr, FieldId, FieldPath, Function},
                validate::{
                    ExprPlanBinaryOpCode, ExprPlanError, ExprPlanFunctionCode, ExprPlanTypeClass,
                    ExprPlanUnaryOpCode,
                },
            },
        },
        schema::{
            AcceptedFieldKind, AcceptedSchemaSnapshot, FieldId as SchemaFieldId,
            PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot, SchemaInfo, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        },
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
        index::IndexModel,
    },
    testing::entity_model_from_static,
    value::Value,
};

use super::{
    ExprType, NumericSubtype, UnaryOp, function_is_compare_operand_coarse_family, infer_expr_type,
};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::generated(
    "query::plan::expr::idx_empty",
    "query::plan::expr::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

crate::test_schema_entity! {
    ident = ExprInferenceEntity,
    entity_name = "ExprInferenceEntity",
    key_type = crate::types::Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: crate::types::Ulid => FieldKind::Ulid },
        crate::test_field! { rank: () => FieldKind::Nat64 },
        crate::test_field! { flag: () => FieldKind::Bool },
        crate::test_field! { label: () => FieldKind::Text { max_len: None } },
        crate::test_field! { created_on: () => FieldKind::Date },
    ],
    indexes = [&EMPTY_INDEX],
}

static PROFILE_NESTED_FIELDS: [FieldModel; 1] = [FieldModel::generated("rank", FieldKind::Nat64)];
static PROFILE_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
        "profile",
        FieldKind::empty_test_composite("query::expr::type_inference::tests::Profile"),
        FieldStorageDecode::CatalogValue,
        false,
        None,
        None,
        &PROFILE_NESTED_FIELDS,
    ),
];
static PROFILE_MODEL: EntityModel = entity_model_from_static(
    "query::plan::expr::type_inference::tests::ProfileEntity",
    "ProfileEntity",
    &PROFILE_FIELDS[0],
    0,
    &PROFILE_FIELDS,
    &[],
);

fn schema() -> &'static SchemaInfo {
    let model: &'static EntityModel =
        <ExprInferenceEntity as crate::entity::EntityDeclaration>::MODEL;
    SchemaInfo::cached_for_generated_entity_model(model)
}

fn accepted_profile_schema_with_nested_rank(kind: AcceptedFieldKind) -> SchemaInfo {
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        PROFILE_MODEL.path().to_string(),
        PROFILE_MODEL.name().to_string(),
        SchemaFieldId::new(1),
        SchemaRowLayout::initial(vec![
            (SchemaFieldId::new(1), SchemaFieldSlot::new(0)),
            (SchemaFieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                SchemaFieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                SchemaFieldId::new(2),
                "profile".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::test_composite(),
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["rank".to_string()],
                    kind,
                    false,
                )],
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::CatalogValue,
                LeafCodec::Structural,
            ),
        ],
    ));

    SchemaInfo::from_snapshot_with_generated_model_for_test(&PROFILE_MODEL, &accepted)
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
fn infer_field_type_uses_accepted_schema_field_type() {
    let model: &'static EntityModel =
        <ExprInferenceEntity as crate::entity::EntityDeclaration>::MODEL;
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        model.path().to_string(),
        model.name().to_string(),
        SchemaFieldId::new(1),
        SchemaRowLayout::initial(vec![(SchemaFieldId::new(2), SchemaFieldSlot::new(1))]),
        vec![PersistedFieldSnapshot::new_initial(
            SchemaFieldId::new(2),
            "rank".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Blob { max_len: None },
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        )],
    ));
    let schema = SchemaInfo::from_snapshot_with_generated_model_for_test(model, &accepted);
    let expr = Expr::Field(FieldId::new("rank"));

    let inferred = infer_expr_type(&expr, &schema).expect("field should infer");

    assert_eq!(inferred, ExprType::Blob);
}

#[test]
fn infer_field_path_type_uses_accepted_nested_leaf_type() {
    let schema =
        accepted_profile_schema_with_nested_rank(AcceptedFieldKind::Blob { max_len: None });
    let expr = Expr::FieldPath(FieldPath::new(
        FieldId::new("profile"),
        vec!["rank".to_string()],
    ));

    let inferred = infer_expr_type(&expr, &schema).expect("field path should infer");

    assert_eq!(inferred, ExprType::Blob);
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
        right: Box::new(Expr::Literal(Value::Nat64(7))),
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
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::ADD,
            left: ExprPlanTypeClass::Numeric,
            right: ExprPlanTypeClass::Text,
        }
    )));
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
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::ADD,
            left: ExprPlanTypeClass::Numeric,
            right: ExprPlanTypeClass::Bool,
        }
    )));
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
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::ADD,
            left: ExprPlanTypeClass::Numeric,
            right: ExprPlanTypeClass::Opaque,
        }
    )));
}

#[test]
fn infer_binary_numeric_expr_rejects_decidable_non_numeric_literal_operand() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Literal(Value::Bool(true))),
        right: Box::new(Expr::Literal(Value::Int64(5))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("numeric operators must reject non-numeric literal operands");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::ADD,
            left: ExprPlanTypeClass::Bool,
            right: ExprPlanTypeClass::Numeric,
        }
    )));
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
        right: Box::new(Expr::Literal(Value::Int64(5))),
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
            Expr::Literal(Value::Int64(1)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Nat64(0))),
    };

    let inferred = infer_expr_type(&expr, schema).expect("searched CASE should infer");

    assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Integer));
}

#[test]
fn infer_searched_case_rejects_non_boolean_conditions() {
    let schema = schema();
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Field(FieldId::new("rank")),
            Expr::Literal(Value::Int64(1)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Int64(0))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("searched CASE must reject non-boolean branch conditions");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidCaseConditionType {
            arm_index: 0,
            found: ExprPlanTypeClass::Numeric,
        }
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
        else_expr: Box::new(Expr::Literal(Value::Int64(0))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("searched CASE must reject incompatible result branches");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::IncompatibleCaseBranchTypes {
            left_branch_index: Some(0),
            right_branch_index: None,
            left: ExprPlanTypeClass::Text,
            right: ExprPlanTypeClass::Numeric,
        }
    )));
}

#[test]
fn infer_binary_numeric_expr_rejects_unknown_non_eligible_operands() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(min())),
        right: Box::new(Expr::Literal(Value::Int64(1))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("unknown type does not imply numeric eligibility");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::ADD,
            left: ExprPlanTypeClass::Unknown,
            right: ExprPlanTypeClass::Numeric,
        }
    )));
}

#[test]
fn infer_round_function_expr_returns_decimal_for_numeric_input() {
    let schema = schema();
    let expr = Expr::FunctionCall {
        function: crate::db::query::plan::expr::Function::Round,
        args: vec![
            Expr::Field(FieldId::new("rank")),
            Expr::Literal(Value::Nat64(2)),
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
            Expr::Literal(Value::Nat64(2)),
        ],
    };

    let err = infer_expr_type(&expr, schema).expect_err("ROUND(text, 2) should fail closed");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidFunctionArgument {
            function: ExprPlanFunctionCode::ROUND,
            argument_index: 0,
            found: ExprPlanTypeClass::Text,
        }
    )));
}

#[test]
fn infer_sum_aggregate_rejects_decidable_non_numeric_bool_target() {
    let schema = schema();
    let expr = Expr::Aggregate(sum("flag"));

    let err = infer_expr_type(&expr, schema).expect_err("sum over bool should fail");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::NonNumericAggregateTarget {
            kind: AggregateKind::Sum,
            found: ExprPlanTypeClass::Bool,
        }
    )));
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
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::NonNumericAggregateTarget {
            kind: AggregateKind::Sum,
            found: ExprPlanTypeClass::Text,
        }
    )));
}

#[test]
fn infer_sum_aggregate_without_target_rejects_missing_target() {
    let schema = schema();
    let expr = Expr::Aggregate(AggregateExpr::from_optional_field_input(
        AggregateKind::Sum,
        None,
        false,
    ));

    let err = infer_expr_type(&expr, schema).expect_err("sum without target should fail");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::AggregateTargetRequired {
            kind: AggregateKind::Sum,
        }
    )));
}

#[test]
fn infer_avg_aggregate_over_numeric_expression_uses_expression_result_type() {
    let schema = schema();
    let expr = Expr::Aggregate(AggregateExpr::from_expression_input(
        AggregateKind::Avg,
        Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Nat64(1))),
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
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::ADD,
            left: ExprPlanTypeClass::Numeric,
            right: ExprPlanTypeClass::Text,
        }
    )));
}

#[test]
fn infer_unary_bool_not_rejects_non_bool_operands() {
    let schema = schema();
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Field(FieldId::new("rank"))),
    };

    let err = infer_expr_type(&expr, schema).expect_err("not over numeric field should fail");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidUnaryOperand {
            op: ExprPlanUnaryOpCode::NOT,
            found: ExprPlanTypeClass::Numeric,
        }
    )));
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
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::EQ,
            left: ExprPlanTypeClass::Numeric,
            right: ExprPlanTypeClass::Text,
        }
    )));
}

#[test]
fn infer_binary_compare_rejects_unknown_operands_fail_closed() {
    let schema = schema();
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Aggregate(AggregateExpr::from_optional_field_input(
            AggregateKind::Min,
            None,
            false,
        ))),
        right: Box::new(Expr::Aggregate(AggregateExpr::from_optional_field_input(
            AggregateKind::Max,
            None,
            false,
        ))),
    };

    let err = infer_expr_type(&expr, schema)
        .expect_err("unknown aggregate operand comparison should fail closed");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::EQ,
            left: ExprPlanTypeClass::Unknown,
            right: ExprPlanTypeClass::Unknown,
        }
    )));
}
