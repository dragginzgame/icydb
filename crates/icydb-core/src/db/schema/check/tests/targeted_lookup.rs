//! Target resolution uses the caller's admitted metadata extent and accepted slots.

use super::*;
use crate::db::{
    QueryError, query::preparation::PreparationWork,
    schema::check::targeted::CompiledAcceptedTargetedRules, tests::support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

fn extended_fixture(extra_fields: u16, extra_types: u32) -> TargetedRuleFixture {
    let mut fixture = targeted_rule_fixture();
    let snapshot = fixture.accepted.persisted_snapshot();
    let mut fields = snapshot.fields().to_vec();
    let first_id = fields.iter().map(|field| field.id().get()).max().unwrap() + 1;
    let first_slot = u16::try_from(snapshot.row_layout().allocated_slot_count()).unwrap();
    for ordinal in 0..extra_fields {
        fields.push(field(
            first_id + u32::from(ordinal),
            first_slot + ordinal,
            &format!("unused_{ordinal}"),
            AcceptedFieldKind::Int64,
            true,
            LeafCodec::Scalar(ScalarCodec::Int64),
        ));
    }
    let layout = SchemaRowLayout::initial(
        fields
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect(),
    );
    fixture.accepted = AcceptedSchemaSnapshot::try_new(
        snapshot.clone().with_fields(fields).with_row_layout(layout),
    )
    .unwrap();
    let enum_catalog = fixture.value_catalog.enum_catalog().clone();
    let additions = (0..extra_types)
        .map(|ordinal| {
            (
                CompositeTypeId::new(100 + ordinal).unwrap(),
                (
                    format!("tests::Unused{ordinal}"),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Nat64,
                        false,
                    )),
                ),
            )
        })
        .collect();
    let catalog = fixture
        .value_catalog
        .composite_catalog()
        .clone()
        .with_added_definitions(additions, &enum_catalog)
        .unwrap();
    fixture.value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        catalog,
        AcceptedSchemaRevision::INITIAL,
    );
    fixture
}

fn compile_targeted(
    fixture: &TargetedRuleFixture,
    limit: u64,
    lane: Lane,
) -> (u64, Result<CompiledAcceptedTargetedRules, QueryError>) {
    let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
    let result = PreparationWork::run(&root.scope(), lane, |work| {
        CompiledAcceptedTargetedRules::compile_sources(
            &fixture.accepted,
            &fixture.value_catalog,
            fixture.accepted_targeted_sources(),
            work,
        )
        .map_err(QueryError::execute)
    });
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(root.observed(Resource::KeyIndexEntriesVisited), 0);
    (root.observed(Resource::PredicateExpressionSteps), result)
}

#[test]
fn targeted_resolution_admits_field_and_newtype_catalog_extents() {
    let narrow = extended_fixture(0, 0);
    for (extra_fields, extra_types) in [(6, 0), (0, 4), (6, 4)] {
        let mut wide = extended_fixture(extra_fields, extra_types);
        if extra_types == 0 {
            // Field-only growth keeps the same opaque catalog authority, so
            // complete compiled-program equality remains a meaningful control.
            wide.value_catalog = narrow.value_catalog.clone();
        }
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let (narrow_steps, narrow_program) = compile_targeted(&narrow, u64::MAX, lane);
            let (wide_steps, wide_program) = compile_targeted(&wide, u64::MAX, lane);
            // Larger metadata must consume more work while preserving behavior;
            // the exact number of internal scans is not part of this contract.
            assert!(wide_steps > narrow_steps);
            let narrow_program = narrow_program.unwrap();
            let wide_program = wide_program.unwrap();
            assert_eq!(
                narrow_program.required_slots().collect::<Vec<_>>(),
                wide_program.required_slots().collect::<Vec<_>>()
            );
            assert_eq!(
                wide_program.evaluate(&borrowed_values(&wide.values)),
                narrow_program.evaluate(&borrowed_values(&narrow.values))
            );
            let mut invalid = narrow.values.clone();
            invalid[1] = Some(TargetedRuleFixture::node(12, Vec::new()));
            let expected = narrow_program.evaluate(&borrowed_values(&invalid)).unwrap();
            assert!(expected.is_some());
            assert_eq!(
                wide_program.evaluate(&borrowed_values(&invalid)).unwrap(),
                expected
            );
            if extra_types == 0 {
                assert_eq!(wide_program, narrow_program);
            }
        }
    }
}

fn assert_budget(facts: &[(DiagnosticFactTag, u64)], lane: Lane, limit: u64, actual: u64) {
    for fact in [
        (
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw(),
        ),
        (DiagnosticFactTag::ExecutionLane, lane.raw()),
        (DiagnosticFactTag::Limit, limit),
        (DiagnosticFactTag::Actual, actual),
    ] {
        assert!(facts.contains(&fact), "missing {fact:?}");
    }
}

fn with_pending_rule(mut fixture: TargetedRuleFixture) -> TargetedRuleFixture {
    let (id, target, operation, _) = fixture.accepted_targeted_sources()[0];
    let AcceptedRuleOperation::NumericRangeInclusive { max, .. } = operation else {
        panic!("fixture's first targeted rule must be a numeric range");
    };
    // Replacement requires changed semantics, while these valid fixture rows
    // still satisfy both the accepted range and the pending maximum.
    let replacement = AcceptedRuleOperation::NumericMaximumInclusive { value: max.clone() };
    let catalog = fixture
        .accepted
        .persisted_snapshot()
        .constraint_catalog()
        .clone()
        .with_replaced_targeted_rule_activation(
            id,
            target,
            replacement,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            1,
        )
        .unwrap();
    (fixture.accepted, fixture.program) = reloaded_targeted_program(&fixture, catalog);
    fixture
}

#[test]
fn accepted_and_pending_targeted_lookups_share_exact_and_cumulative_request_limits() {
    for pending in [false, true] {
        let fixture = extended_fixture(3, 2);
        let fixture = if pending {
            with_pending_rule(fixture)
        } else {
            fixture
        };
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let compile = |limit| {
                let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    CompiledAcceptedRowConstraints::compile(
                        &fixture.accepted,
                        &fixture.value_catalog,
                        FINGERPRINT,
                        work,
                    )
                    .map_err(QueryError::execute)
                });
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::KeyIndexEntriesVisited), 0);
                (root.observed(Resource::PredicateExpressionSteps), result)
            };
            let (required, program) = compile(u64::MAX);
            let program = program.unwrap();
            program
                .evaluate(FINGERPRINT, &borrowed_values(&fixture.values))
                .unwrap();
            let (observed, actual) = compile(required);
            assert_eq!(observed, required);
            assert_eq!(actual.unwrap(), program);
            let error = compile(required - 1).1.unwrap_err();
            assert_budget(&error.diagnostic_facts(), lane, required - 1, required);
            let root = request_with_limit(Resource::PredicateExpressionSteps, required * 2 - 1);
            for repetition in 0..2 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    CompiledAcceptedRowConstraints::compile(
                        &fixture.accepted,
                        &fixture.value_catalog,
                        FINGERPRINT,
                        work,
                    )
                    .map_err(QueryError::execute)
                });
                if repetition == 0 {
                    assert_eq!(result.unwrap(), program);
                } else {
                    assert_budget(
                        &result.unwrap_err().diagnostic_facts(),
                        lane,
                        required * 2 - 1,
                        required * 2,
                    );
                }
            }
        }
    }
}

#[test]
fn accepted_and_pending_targeted_lookups_preserve_cumulative_maintenance_refusal() {
    for pending in [false, true] {
        let fixture = extended_fixture(3, 2);
        let fixture = if pending {
            with_pending_rule(fixture)
        } else {
            fixture
        };
        let measure = MaintenanceConstructionBudget::with_limit_for_tests(
            Resource::PredicateExpressionSteps,
            u64::MAX,
        );
        let program = measure
            .run(
                |work| {
                    CompiledAcceptedRowConstraints::compile(
                        &fixture.accepted,
                        &fixture.value_catalog,
                        FINGERPRINT,
                        work,
                    )
                },
                std::convert::identity,
            )
            .unwrap();
        let required = measure.observed_for_tests(Resource::PredicateExpressionSteps);
        let work = MaintenanceConstructionBudget::with_limit_for_tests(
            Resource::PredicateExpressionSteps,
            required * 2 - 1,
        );
        let run = || {
            work.run(
                |work| {
                    CompiledAcceptedRowConstraints::compile(
                        &fixture.accepted,
                        &fixture.value_catalog,
                        FINGERPRINT,
                        work,
                    )
                },
                std::convert::identity,
            )
        };
        assert_eq!(run().unwrap(), program);
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
