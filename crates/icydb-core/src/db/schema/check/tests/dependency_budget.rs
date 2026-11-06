//! Dependency admission follows semantic projection, not discarded serialized keys.

use super::*;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

#[test]
fn unique_dependencies_need_only_projection_and_slot_bookkeeping_allowance() {
    for (sql, surviving_fields) in [
        ("nickname IS NOT NULL", 1),
        ("nickname IS NOT NULL AND payload IS NOT NULL", 2),
        ("score > 10 AND score < 0 AND nickname IS NOT NULL", 0),
    ] {
        let accepted =
            AcceptedSchemaSnapshot::try_new(unique_activation_predicate_fixture(sql)).unwrap();
        let snapshot = accepted.persisted_snapshot();
        let catalog = value_catalog();
        let predicate = snapshot.candidate_indexes()[0].predicate().unwrap();
        let projection = MaintenanceConstructionBudget::with_limit_for_tests(
            Resource::PredicateExpressionSteps,
            u64::MAX,
        );
        projection
            .run(
                |work| {
                    let projected =
                        predicate.to_predicate(snapshot.fields(), &catalog, Some(work))?;
                    crate::db::predicate::normalize_with_budget(projected, work)
                },
                std::convert::identity,
            )
            .unwrap();
        // Classification and integrity each visit the catalog. Each NOT NULL
        // contributes field resolution, dispatch, integrity, slot emission and
        // deduplication. Unique selection then owns its bitmap/field passes.
        // The projection owner
        // admits its own traversal; dependency selection has no serialized-key consumer.
        let slots = snapshot.row_layout().allocated_slot_count() as u64;
        let steps = 2 * snapshot.constraints().len() as u64
            + snapshot
                .constraints()
                .iter()
                .filter(|constraint| {
                    matches!(constraint.kind(), AcceptedConstraintKind::NotNull { .. })
                })
                .count() as u64
                * (snapshot.fields().len() as u64 + 5)
            + 2 * snapshot.constraint_activations().len() as u64
            + snapshot.candidate_indexes().len() as u64
            + 3 * slots
            + snapshot.fields().len() as u64
            + surviving_fields
            + projection.observed_for_tests(Resource::PredicateExpressionSteps);
        let expected = CompiledAcceptedRowConstraints::compile(
            &accepted,
            &catalog,
            FINGERPRINT,
            &MaintenanceConstructionBudget::new(),
        )
        .unwrap();
        for limit in [steps - 1, steps] {
            let budget = MaintenanceConstructionBudget::with_limit_for_tests(
                Resource::PredicateExpressionSteps,
                limit,
            );
            let result = budget.run(
                |work| {
                    CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT, work)
                },
                std::convert::identity,
            );
            if limit == steps {
                assert_eq!(result.unwrap(), expected);
                assert_eq!(
                    budget.observed_for_tests(Resource::PredicateExpressionSteps),
                    steps
                );
                let error = budget
                    .run(
                        |work| {
                            CompiledAcceptedRowConstraints::compile(
                                &accepted,
                                &catalog,
                                FINGERPRINT,
                                work,
                            )
                        },
                        std::convert::identity,
                    )
                    .unwrap_err();
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw()
                )));
            } else {
                let error = result.unwrap_err();
                for fact in [
                    (
                        DiagnosticFactTag::BudgetResource,
                        Resource::PredicateExpressionSteps.raw(),
                    ),
                    (DiagnosticFactTag::Limit, limit),
                ] {
                    assert!(error.diagnostic_facts().contains(&fact));
                }
            }
        }
    }
}
