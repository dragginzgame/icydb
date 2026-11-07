//! Literal decoding shares the row-program caller's existing byte allowance.

use super::*;
use crate::db::{
    QueryError, query::preparation::PreparationWork, tests::support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

fn assert_budget(facts: &[(DiagnosticFactTag, u64)], lane: Lane, limit: u64, actual: u64) {
    for fact in [
        (
            DiagnosticFactTag::BudgetResource,
            Resource::DecodedBytes.raw(),
        ),
        (DiagnosticFactTag::ExecutionLane, lane.raw()),
        (DiagnosticFactTag::Limit, limit),
        (DiagnosticFactTag::Actual, actual),
    ] {
        assert!(facts.contains(&fact), "missing {fact:?}");
    }
}

fn comparison_bytes(expression: &AcceptedCheckExprV1) -> u64 {
    let AcceptedCheckExprV1::Compare {
        right: AcceptedCheckValueExprV1::Literal(literal),
        ..
    } = expression
    else {
        panic!("fixture must contain one bound literal");
    };
    literal.payload().len() as u64
}

// Read the operation's actual encoded operands independently of its new roster.
fn operation_bytes(operation: &AcceptedRuleOperation) -> u64 {
    match operation {
        AcceptedRuleOperation::LengthRangeInclusive { .. } => 0,
        AcceptedRuleOperation::MultipleOf { divisor } => divisor.payload().len() as u64,
        AcceptedRuleOperation::NumericMaximumInclusive { value }
        | AcceptedRuleOperation::NumericMinimumInclusive { value } => value.payload().len() as u64,
        AcceptedRuleOperation::NumericRangeInclusive { min, max } => {
            (min.payload().len() + max.payload().len()) as u64
        }
    }
}

fn compile(
    schema: &AcceptedSchemaSnapshot,
    catalog: &AcceptedValueCatalogHandle,
    limit: u64,
    lane: Lane,
) -> (u64, Result<CompiledAcceptedRowConstraints, QueryError>) {
    let root = request_with_limit(Resource::DecodedBytes, limit);
    let result = PreparationWork::run(&root.scope(), lane, |work| {
        CompiledAcceptedRowConstraints::compile(schema, catalog, FINGERPRINT, work)
            .map_err(QueryError::execute)
    });
    (root.observed(Resource::DecodedBytes), result)
}

#[test]
fn check_literal_decode_obeys_caller_allowance() {
    let (schema, catalog, _) = accepted_with_check(CheckExprV1Input::Compare {
        left: CheckValueExprV1Input::Field("nickname".into()),
        op: AcceptedCheckCompareOpV1::Eq,
        right: CheckValueExprV1Input::Literal(InputValue::text("é🦀".into())),
    });
    assert!(
        compile(&schema, &catalog, 0, Lane::Mutation).1.is_err(),
        "CHECK decoding must not bypass the existing zero byte allowance"
    );
}

#[test]
fn targeted_literal_decode_obeys_caller_allowance() {
    let fixture = targeted_rule_fixture();
    assert!(
        compile(&fixture.accepted, &fixture.value_catalog, 0, Lane::Mutation)
            .1
            .is_err(),
        "targeted-rule decoding must not bypass the existing zero byte allowance"
    );
}

#[test]
fn check_literal_input_obeys_exact_and_short_bounds_across_scalar_shapes_and_boolean_children() {
    for (field, input) in [
        ("score", InputValue::int64(-7)),
        ("nickname", InputValue::text(String::new())),
        ("nickname", InputValue::text("é🦀".repeat(16))),
        ("payload", InputValue::blob(Vec::new())),
        ("payload", InputValue::blob(vec![255; 64])),
    ] {
        for repeated in [false, true] {
            let comparison = |op| CheckExprV1Input::Compare {
                left: CheckValueExprV1Input::Field(field.into()),
                op,
                right: CheckValueExprV1Input::Literal(input.clone()),
            };
            let tree = if repeated {
                CheckExprV1Input::Or(vec![
                    comparison(AcceptedCheckCompareOpV1::Eq),
                    comparison(AcceptedCheckCompareOpV1::Ne),
                ])
            } else {
                comparison(AcceptedCheckCompareOpV1::Eq)
            };
            let (schema, catalog, id) = accepted_with_check(tree);
            let expression = schema
                .persisted_snapshot()
                .constraints()
                .iter()
                .find_map(|constraint| {
                    if constraint.id() != id {
                        return None;
                    }
                    let AcceptedConstraintKind::Check { expression } = constraint.kind() else {
                        return None;
                    };
                    Some(expression.as_ref())
                })
                .unwrap();
            let required = if let AcceptedCheckExprV1::Or(children) = expression {
                comparison_bytes(&children[0]) * 2
            } else {
                comparison_bytes(expression)
            };
            for lane in [
                Lane::PublicRead,
                Lane::TrustedRead,
                Lane::Diagnostic,
                Lane::Mutation,
            ] {
                let baseline = compile(&schema, &catalog, u64::MAX, lane).1.unwrap();
                for limit in [required, required + 1] {
                    let (observed, program) = compile(&schema, &catalog, limit, lane);
                    assert_eq!(observed, required);
                    assert_eq!(program.unwrap(), baseline);
                }
                let (_, short) = compile(&schema, &catalog, required - 1, lane);
                assert_budget(
                    &short.unwrap_err().diagnostic_facts(),
                    lane,
                    required - 1,
                    required,
                );
            }
        }
    }
}

#[test]
fn every_targeted_operation_admits_its_encoded_operands_and_length_rules_need_no_decode_bytes() {
    use crate::db::schema::check::targeted::CompiledAcceptedTargetedRules;
    let fixture = targeted_rule_fixture();
    for source in fixture.accepted_targeted_sources() {
        let required = operation_bytes(source.2);
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let run = |limit| {
                let root = request_with_limit(Resource::DecodedBytes, limit);
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    CompiledAcceptedTargetedRules::compile_sources(
                        &fixture.accepted,
                        &fixture.value_catalog,
                        vec![source],
                        work,
                    )
                    .map_err(QueryError::execute)
                });
                (root.observed(Resource::DecodedBytes), result)
            };
            let baseline = run(u64::MAX).1.unwrap();
            for limit in [required, required + 1] {
                let (observed, program) = run(limit);
                assert_eq!(observed, required);
                assert_eq!(program.unwrap(), baseline);
            }
            if required > 0 {
                assert_budget(
                    &run(required - 1).1.unwrap_err().diagnostic_facts(),
                    lane,
                    required - 1,
                    required,
                );
            }
        }
    }
}

#[test]
fn accepted_and_pending_checks_share_cumulative_maintenance_decode_bytes_with_targeted_rules() {
    let fixture = targeted_rule_fixture();
    let targeted_bytes = fixture
        .accepted_targeted_sources()
        .iter()
        .map(|source| operation_bytes(source.2))
        .sum::<u64>();
    let snapshot = fixture.accepted.persisted_snapshot();
    let expression = bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("degree_wrapper".into()),
            op: AcceptedCheckCompareOpV1::Eq,
            right: CheckValueExprV1Input::Literal(InputValue::nat64(5)),
        },
        snapshot,
        fixture.value_catalog.enum_catalog(),
        fixture.value_catalog.composite_catalog(),
    )
    .unwrap();
    let required = targeted_bytes + comparison_bytes(&expression);
    for pending in [false, true] {
        let catalog = snapshot.constraint_catalog().clone();
        let catalog = if pending {
            catalog.with_added_check_activation(
                "decode_gate".into(),
                ConstraintOrigin::Generated,
                expression.clone(),
                AcceptedSchemaFingerprint::new([0xA5; 32]),
                2,
            )
        } else {
            catalog.with_added_check(
                "decode_gate".into(),
                ConstraintOrigin::Generated,
                expression.clone(),
            )
        }
        .unwrap();
        let schema =
            AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(catalog))
                .unwrap();
        let baseline = compile(&schema, &fixture.value_catalog, required, Lane::Mutation)
            .1
            .unwrap();
        let work = MaintenanceConstructionBudget::with_limit_for_tests(
            Resource::DecodedBytes,
            required * 2 - 1,
        );
        let run = || {
            work.run(
                |work| {
                    CompiledAcceptedRowConstraints::compile(
                        &schema,
                        &fixture.value_catalog,
                        FINGERPRINT,
                        work,
                    )
                },
                |error| error,
            )
        };
        assert_eq!(run().unwrap(), baseline);
        let error = run().unwrap_err();
        assert_budget(
            &error.diagnostic_facts(),
            Lane::Mutation,
            required * 2 - 1,
            required * 2,
        );
        assert_eq!(
            run().unwrap_err().diagnostic_facts(),
            error.diagnostic_facts()
        );
    }
}

#[test]
fn literal_free_checks_need_no_decode_allowance_and_corrupt_payloads_preserve_typed_refusals() {
    let (schema, catalog, _) = accepted_with_check(CheckExprV1Input::IsNull(
        CheckValueExprV1Input::Field("nickname".into()),
    ));
    assert_eq!(compile(&schema, &catalog, 0, Lane::Mutation).0, 0);
    assert!(compile(&schema, &catalog, 0, Lane::Mutation).1.is_ok());
    let snapshot = snapshot();
    let expression = AcceptedCheckExprV1::Compare {
        left: AcceptedCheckValueExprV1::Field(FieldId::new(3)),
        op: AcceptedCheckCompareOpV1::Eq,
        right: AcceptedCheckValueExprV1::Literal(AcceptedCheckLiteralV1::from_accepted_parts(
            AcceptedFieldKind::Text { max_len: Some(64) },
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
            vec![255],
        )),
    };
    let constraints = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "corrupt_payload".into(),
            ConstraintOrigin::Generated,
            expression,
        )
        .unwrap();
    let schema =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraints)).unwrap();
    let (_, refused) = compile(&schema, &catalog, 0, Lane::Mutation);
    assert_budget(
        &refused.unwrap_err().diagnostic_facts(),
        Lane::Mutation,
        0,
        1,
    );
    assert_eq!(
        compile(&schema, &catalog, 1, Lane::Mutation)
            .1
            .unwrap_err()
            .diagnostic_code(),
        crate::error::InternalError::accepted_row_constraint_program_corrupt().diagnostic_code()
    );
}
