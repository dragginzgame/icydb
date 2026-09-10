//! Analysis-based admission agrees with scalar compilation without building a program.

use super::validate_analyzed_schema_bound_scalar_expr;
use crate::{
    db::{
        query::{
            builder::aggregate::count,
            plan::expr::{
                Alias, BinaryOp, CaseWhenArm, Expr, FieldId as ExprFieldId, FieldPath, Function,
                UnaryOp, compile_scalar_projection_expr_with_schema,
            },
        },
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
            AcceptedValueCatalogHandle, FieldId, FieldStorageDecode, LeafCodec,
            PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaInfo, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, build_record_newtype_composite_catalog_for_tests,
            empty_accepted_enum_catalog_for_tests,
        },
        sql::lowering::{AnalyzedLoweredExpr, SqlLoweringError},
    },
    value::Value,
};
use icydb_diagnostic_code::QueryFieldRole;

fn schema(id_name: &str) -> SchemaInfo {
    let enums = empty_accepted_enum_catalog_for_tests();
    let (composites, record, name, _) = build_record_newtype_composite_catalog_for_tests(
        "tests::ScalarAdmissionProfile".into(),
        "name".into(),
        "tests::ScalarAdmissionName".into(),
        AcceptedFieldKind::Text { max_len: Some(64) },
        &enums,
    )
    .unwrap();
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            id_name.into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "profile".into(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Composite { type_id: record },
            vec![PersistedNestedLeafSnapshot::new(
                vec!["name".into()],
                AcceptedFieldKind::Composite { type_id: name },
                false,
            )],
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::ScalarAdmission".into(),
        "ScalarAdmission".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    SchemaInfo::from_accepted_snapshot_and_catalog(
        &AcceptedSchemaSnapshot::new(snapshot),
        AcceptedValueCatalogHandle::new_for_tests(
            enums,
            composites,
            AcceptedSchemaRevision::INITIAL,
        ),
        true,
    )
}

fn field(name: &str) -> Expr {
    Expr::Field(ExprFieldId::new(name))
}

fn path(root: &str) -> Expr {
    Expr::FieldPath(FieldPath::new(root, vec!["name".into()]))
}

fn binary(left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn validate(schema: &SchemaInfo, analyzed: &AnalyzedLoweredExpr) -> Result<(), SqlLoweringError> {
    validate_analyzed_schema_bound_scalar_expr(
        schema,
        analyzed,
        QueryFieldRole::AggregateTarget,
        || SqlLoweringError::UnsupportedAggregateInputExpressions,
    )
}

#[test]
fn analyzed_scalar_admission_matches_compiler_across_expression_shapes() {
    let schema = schema("id");
    let cases = vec![
        field("id"),
        path("profile"),
        Expr::Literal(Value::Null),
        Expr::Literal(Value::Text("payload".repeat(8192))),
        Expr::FunctionCall {
            function: Function::Lower,
            args: vec![path("profile")],
        },
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(binary(field("id"), Expr::Literal(Value::Nat64(0)))),
        },
        binary(field("id"), Expr::Literal(Value::Nat64(1))),
        Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                binary(field("id"), Expr::Literal(Value::Nat64(1))),
                path("profile"),
            )],
            else_expr: Box::new(Expr::Literal(Value::Text("fallback".into()))),
        },
        Expr::Alias {
            expr: Box::new(field("id")),
            name: Alias::new("display"),
        },
        field("missing"),
        path("missing"),
        Expr::Aggregate(count()),
        Expr::FunctionCall {
            function: Function::Lower,
            args: vec![Expr::Aggregate(count())],
        },
        Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Literal(Value::Bool(false)),
                field("missing"),
            )],
            else_expr: Box::new(field("id")),
        },
    ];
    for expr in cases {
        let expected = crate::db::query::preparation::with_preparation_work(|work| {
            compile_scalar_projection_expr_with_schema(&schema, &expr, work)
        })
        .unwrap()
        .is_some();
        let analyzed = AnalyzedLoweredExpr::new(expr);
        assert_eq!(
            validate(&schema, &analyzed).is_ok(),
            expected,
            "{:?}",
            analyzed.expr()
        );
    }
}

#[test]
fn scalar_admission_keeps_current_authority_and_unknown_field_precedence() {
    let old = schema("id");
    let current = schema("renamed");
    let analyzed = AnalyzedLoweredExpr::new(field("id"));
    assert!(validate(&old, &analyzed).is_ok());
    assert!(
        matches!(validate(&current, &analyzed), Err(SqlLoweringError::UnknownField { field, role: QueryFieldRole::AggregateTarget }) if field == "id")
    );
    // The first outer unknown source still wins, even after an aggregate leaf.
    let analyzed = AnalyzedLoweredExpr::new(binary(
        Expr::Aggregate(count()),
        binary(path("first"), field("second")),
    ));
    for role in [QueryFieldRole::AggregateTarget, QueryFieldRole::Predicate] {
        let mut unsupported_called = false;
        let result = validate_analyzed_schema_bound_scalar_expr(&old, &analyzed, role, || {
            unsupported_called = true;
            SqlLoweringError::UnsupportedWhereExpression
        });
        assert!(
            matches!(result, Err(SqlLoweringError::UnknownField { field, role: actual }) if field == "first" && actual == role)
        );
        assert!(!unsupported_called);
    }
    let analyzed = AnalyzedLoweredExpr::new(Expr::Aggregate(count()));
    assert!(matches!(
        validate(&old, &analyzed),
        Err(SqlLoweringError::UnsupportedAggregateInputExpressions)
    ));
    assert!(matches!(
        validate_analyzed_schema_bound_scalar_expr(
            &old,
            &analyzed,
            QueryFieldRole::Predicate,
            || SqlLoweringError::UnsupportedWhereExpression
        ),
        Err(SqlLoweringError::UnsupportedWhereExpression)
    ));
}
