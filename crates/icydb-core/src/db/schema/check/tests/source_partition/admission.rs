//! Source classification uses the compiler caller's cumulative authority.

use super::*;
use crate::db::{
    QueryError, query::preparation::PreparationWork, tests::support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

#[test]
fn compiled_checks_require_backing_even_when_source_capacity_is_reused() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let mut constraints = snapshot.constraint_catalog().clone();
    let mut previous_bytes = None;
    for count in 1..=4 {
        constraints = constraints
            .with_added_check(
                format!("true_{count}"),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
            )
            .unwrap();
        let accepted = AcceptedSchemaSnapshot::try_new(
            snapshot
                .clone()
                .with_constraint_catalog(constraints.clone()),
        )
        .unwrap();
        let budget = MaintenanceConstructionBudget::new();
        let program = budget
            .run(
                |work| {
                    CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT, work)
                },
                std::convert::identity,
            )
            .unwrap();
        assert_eq!(program.integrity_constraint_count(), count);
        let bytes = budget.observed_for_tests(Resource::TemporaryBytes);
        // The first four CHECK sources share the source owner's initial
        // capacity. Every additional compiled CHECK still needs program backing.
        if let Some(previous) = previous_bytes {
            assert!(bytes > previous);
        }
        previous_bytes = Some(bytes);
    }
}

fn mixed_source_fixture() -> TargetedRuleFixture {
    let mut fixture = targeted_rule_fixture();
    let snapshot = fixture.accepted.persisted_snapshot();
    let fingerprint = AcceptedSchemaFingerprint::new([0xA5; 32]);
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "accepted_true".into(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
        )
        .unwrap()
        .with_added_check_activation(
            "pending_true".into(),
            ConstraintOrigin::SqlDdl,
            AcceptedCheckExprV1::True,
            fingerprint,
            2,
        )
        .unwrap()
        .with_added_not_null_activation(&snapshot.fields()[7], fingerprint, 2)
        .unwrap()
        .with_added_targeted_rule_activation(
            "pending_label_length".into(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(8),
                AcceptedNamedTypeIdentity::Composite(fixture.label_type),
            ),
            AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 5 },
            fingerprint,
            2,
        )
        .unwrap();
    fixture.accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(catalog)).unwrap();
    fixture
}

#[test]
fn mixed_sources_obey_exact_short_and_cumulative_caller_limits() {
    let fixture = mixed_source_fixture();
    for resource in [Resource::PredicateExpressionSteps, Resource::TemporaryBytes] {
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let baseline = request_with_limit(resource, u64::MAX);
            let compile = |work: &PreparationWork<'_>| {
                CompiledAcceptedRowConstraints::compile(
                    &fixture.accepted,
                    &fixture.value_catalog,
                    FINGERPRINT,
                    work,
                )
                .map_err(QueryError::execute)
            };
            let expected = PreparationWork::run(&baseline.scope(), lane, compile).unwrap();
            let exact = baseline.observed(resource);
            assert!(exact > 0);
            for limit in [0, exact - 1, exact, 2 * exact] {
                let root = request_with_limit(resource, limit);
                for attempt in 1..=3 {
                    let result = PreparationWork::run(&root.scope(), lane, compile);
                    if attempt * exact <= limit {
                        assert_eq!(result.unwrap(), expected);
                        assert_eq!(root.observed(resource), attempt * exact);
                    } else {
                        let error = result.unwrap_err();
                        for fact in [
                            (DiagnosticFactTag::BudgetResource, resource.raw()),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_sources_share_maintenance_capacity_and_keep_program_semantics() {
    let fixture = mixed_source_fixture();
    let baseline = MaintenanceConstructionBudget::new();
    let compile = |work: &MaintenanceConstructionBudget| {
        CompiledAcceptedRowConstraints::compile(
            &fixture.accepted,
            &fixture.value_catalog,
            FINGERPRINT,
            work,
        )
    };
    let expected = baseline.run(compile, std::convert::identity).unwrap();
    expected
        .evaluate(FINGERPRINT, &borrowed_values(&fixture.values))
        .unwrap();
    for resource in [Resource::PredicateExpressionSteps, Resource::TemporaryBytes] {
        let exact = baseline.observed_for_tests(resource);
        assert!(exact > 0);
        for limit in [0, exact - 1, exact, 2 * exact] {
            let budget = MaintenanceConstructionBudget::with_limit_for_tests(resource, limit);
            for attempt in 1..=3 {
                let result = budget.run(compile, std::convert::identity);
                if attempt * exact <= limit {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    let error = result.unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                }
            }
        }
    }
}
