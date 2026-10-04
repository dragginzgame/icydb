//! Qualify SQL and source Boolean tests at accepted checks and index membership.

use super::*;
use crate::{
    db::{
        predicate::PredicateProgram,
        schema::{AcceptedIndexPredicate, SchemaInfo},
        sql::parser::{SqlStatement, parse_sql},
    },
    types::EntityTag,
};
use icydb_schema::{FieldSourceKey, SourceCheckExpr, SourceCheckInstruction};

fn boolean_snapshot() -> PersistedSchemaSnapshot {
    let fields = vec![
        field(
            1,
            0,
            "id",
            AcceptedFieldKind::Ulid,
            false,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        ),
        field(
            2,
            1,
            "flag",
            AcceptedFieldKind::Bool,
            true,
            LeafCodec::Scalar(ScalarCodec::Bool),
        ),
    ];
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::TruthRow".into(),
        "TruthRow".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(fields.iter().map(|f| (f.id(), f.slot())).collect()),
        fields,
    )
}

fn sql_expression(sql: &str) -> crate::db::sql::parser::SqlExpr {
    let SqlStatement::Select(mut statement) =
        parse_sql(&format!("SELECT * FROM TruthRow WHERE {sql}")).unwrap()
    else {
        panic!("expected SELECT");
    };
    statement.predicate.take().unwrap()
}

#[test]
fn boolean_tests_are_total_in_accepted_checks_and_index_membership() {
    let snapshot = boolean_snapshot();
    let catalog = value_catalog();
    for (sql, expected) in [
        ("flag IS TRUE", [true, false, false]),
        ("flag IS FALSE", [false, true, false]),
        ("flag IS NOT TRUE", [false, true, true]),
        ("flag IS NOT FALSE", [true, false, true]),
    ] {
        let expression = bind_sql_check_expr(
            &sql_expression(sql),
            &snapshot,
            catalog.enum_catalog(),
            catalog.composite_catalog(),
        )
        .unwrap();
        let constraints = snapshot
            .constraint_catalog()
            .clone()
            .with_added_check(
                "truth_policy".into(),
                ConstraintOrigin::SqlDdl,
                expression.clone(),
            )
            .unwrap();
        let constraint_id = constraints
            .constraints()
            .iter()
            .find(|c| matches!(c.kind(), AcceptedConstraintKind::Check { .. }))
            .unwrap()
            .id();
        let accepted =
            AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(constraints))
                .unwrap();
        let program = CompiledAcceptedRowConstraints::compile(
            &accepted,
            &catalog,
            FINGERPRINT,
            &MaintenanceConstructionBudget::new(),
        )
        .unwrap();
        let membership = AcceptedIndexPredicate::from_check(
            &expression,
            snapshot.fields(),
            catalog.composite_catalog(),
        )
        .unwrap();
        let schema = SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog.clone());
        let predicate = membership
            .to_predicate(snapshot.fields(), &catalog)
            .unwrap();
        let index_program = PredicateProgram::compile_with_schema_info(&schema, &predicate);
        for (value, expected) in [Value::Bool(true), Value::Bool(false), Value::Null]
            .into_iter()
            .zip(expected)
        {
            let values = vec![
                Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
                Some(value.clone()),
            ];
            let result = program.evaluate(FINGERPRINT, &borrowed_values(&values));
            if expected {
                result.unwrap();
            } else {
                assert_eq!(
                    result,
                    Err(AcceptedRowConstraintEvaluationError::Violation {
                        constraint_id,
                        kind: AcceptedRowConstraintViolationKind::Check
                    }),
                    "{sql} for {value:?}"
                );
            }
            assert_eq!(
                index_program.eval_with_slot_value_cow_reader(
                    &mut |slot| (slot == 1).then_some(Cow::Borrowed(&value))
                ),
                expected,
                "{sql} for {value:?}"
            );
        }
    }
}

#[test]
fn source_boolean_guards_converge_with_sql_without_changing_unknown_check_policy() {
    let snapshot = boolean_snapshot();
    let catalog = value_catalog();
    let entity = EntityTag::new(1);
    let field = FieldSourceKey::try_new("flag").unwrap();
    let bindings = AcceptedSourceBindingCatalog::initial(
        BTreeMap::new(),
        BTreeMap::from([((entity, field.clone()), FieldId::new(2))]),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
    for (sql, value, negated) in [
        ("flag IS TRUE", true, false),
        ("flag IS FALSE", false, false),
        ("flag IS NOT TRUE", true, true),
        ("flag IS NOT FALSE", false, true),
    ] {
        let mut instructions = vec![
            SourceCheckInstruction::Field(field.clone()),
            SourceCheckInstruction::IsNotNull,
            SourceCheckInstruction::Field(field.clone()),
            SourceCheckInstruction::Literal(ScalarLiteral::Bool(value)),
            SourceCheckInstruction::Equal,
            SourceCheckInstruction::And,
        ];
        if negated {
            instructions.push(SourceCheckInstruction::Not);
        }
        let source = bind_source_check_expr(
            &SourceCheckExpr::try_new(instructions).unwrap(),
            entity,
            &bindings,
            &snapshot,
            catalog.enum_catalog(),
            catalog.composite_catalog(),
        )
        .unwrap();
        let sql = bind_sql_check_expr(
            &sql_expression(sql),
            &snapshot,
            catalog.enum_catalog(),
            catalog.composite_catalog(),
        )
        .unwrap();
        assert_eq!(source, sql);
    }
    let ordinary = bind_sql_check_expr(
        &sql_expression("flag = TRUE"),
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .unwrap();
    let constraints = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check("ordinary".into(), ConstraintOrigin::SqlDdl, ordinary)
        .unwrap();
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraints)).unwrap();
    let program = CompiledAcceptedRowConstraints::compile(
        &accepted,
        &catalog,
        FINGERPRINT,
        &MaintenanceConstructionBudget::new(),
    )
    .unwrap();
    program
        .evaluate(
            FINGERPRINT,
            &borrowed_values(&[
                Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
                Some(Value::Null),
            ]),
        )
        .expect("ordinary nullable equality remains UNKNOWN and passes CHECK");
}

#[test]
fn boolean_check_tests_reject_non_boolean_and_unsupported_operands() {
    let snapshot = boolean_snapshot();
    let catalog = value_catalog();
    for (sql, expected) in [
        (
            "id IS TRUE",
            AcceptedCheckExprV1Error::LiteralAdmissionRejected,
        ),
        (
            "LENGTH(flag) IS FALSE",
            AcceptedCheckExprV1Error::LiteralAdmissionRejected,
        ),
        (
            "COALESCE(flag, FALSE) IS TRUE",
            AcceptedCheckExprV1Error::UnsupportedOperator,
        ),
    ] {
        assert_eq!(
            bind_sql_check_expr(
                &sql_expression(sql),
                &snapshot,
                catalog.enum_catalog(),
                catalog.composite_catalog()
            ),
            Err(expected),
            "{sql}",
        );
    }
}
