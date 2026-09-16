//! Prepared residual inspection borrows frozen authority; DTO copies remain charged.

use super::newtype_query_schema;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{MissingRowPolicy, Predicate},
        query::{
            explain::ExplainPredicate,
            plan::{AccessPlannedQuery, LogicalPlan},
            preparation::{PreparationWork, with_preparation_work},
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn plan(predicate: Option<Predicate>) -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.predicate = predicate;
    plan
}

fn finalize(plan: &mut AccessPlannedQuery) {
    let schema = newtype_query_schema();
    with_preparation_work(|work| {
        let projection = plan.prepare_projection(&schema, work)?;
        plan.finalize_static_execution_planning_contract_with_schema(&schema, projection, work)
    })
    .unwrap();
}

fn predicate() -> Predicate {
    Predicate::And(vec![
        Predicate::eq("id".into(), Value::Nat64(7)),
        Predicate::eq("name".into(), Value::Text("λ".repeat(16))),
    ])
}

fn project(
    plan: &AccessPlannedQuery,
    root: &RequestExecutionRoot,
) -> Result<Option<ExplainPredicate>, QueryError> {
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        plan.effective_execution_predicate()
            .map_err(QueryError::execute)?
            .map(|predicate| ExplainPredicate::from_predicate(predicate, work))
            .transpose()
    })
}

#[test]
fn finalized_residual_inspection_borrows_the_frozen_predicate() {
    for predicate in [None, Some(predicate())] {
        let expected = predicate.clone();
        let mut plan = plan(predicate);
        finalize(&mut plan);
        let frozen = plan
            .static_execution_planning_contract
            .as_ref()
            .unwrap()
            .residual_filter_contract
            .residual_filter_predicate();
        assert_eq!(frozen, expected.as_ref());

        for _ in 0..4 {
            let view = plan.effective_execution_predicate().unwrap();
            assert_eq!(view, frozen);
            assert_eq!(
                plan.has_residual_filter_predicate().unwrap(),
                frozen.is_some()
            );
            if let Some(view) = view {
                assert!(std::ptr::eq(view, frozen.unwrap()));
            }
        }
        if frozen.is_none() {
            assert_eq!(
                project(&plan, &request(Resource::TemporaryBytes, 0)).unwrap(),
                None
            );
        }
    }
}

#[test]
fn residual_inspection_requires_frozen_metadata_even_for_empty_filters() {
    for input in [None, Some(predicate())] {
        let mut plan = plan(input);
        for error in [
            plan.effective_execution_predicate().unwrap_err(),
            plan.residual_filter_expr().unwrap_err(),
            plan.residual_filter_shape().unwrap_err(),
            plan.has_residual_filter_predicate().unwrap_err(),
            plan.has_any_residual_filter().unwrap_err(),
        ] {
            assert_eq!(error.class(), crate::error::ErrorClass::InvariantViolation);
        }
        assert!(project(&plan, &request(Resource::TemporaryBytes, 0)).is_err());
        let expected =
            with_preparation_work(|work| plan.prepare_residual_filter_shape(work)).unwrap();
        finalize(&mut plan);
        assert_eq!(plan.residual_filter_shape().unwrap(), expected);
        assert_eq!(
            plan.has_any_residual_filter().unwrap(),
            !expected.is_absent()
        );
    }
}

#[test]
fn explicit_residual_shape_preparation_admits_copies_and_reuses_frozen_facts() {
    let mut plan = plan(Some(predicate()));
    let original = plan.clone();
    let prepare = |plan: &AccessPlannedQuery, root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            plan.prepare_residual_filter_shape(work)
                .map_err(QueryError::execute)
        })
    };
    let generous = request(Resource::TemporaryBytes, 16_000_000);
    let expected = prepare(&plan, &generous).unwrap();
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let used = generous.observed(resource);
        assert!(used > 0);
        let exact = request(resource, used * 2);
        assert_eq!(prepare(&plan, &exact).unwrap(), expected);
        assert_eq!(prepare(&plan, &exact).unwrap(), expected);
        let error = prepare(&plan, &exact).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
        );
        assert_eq!(plan, original);
    }
    finalize(&mut plan);
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let empty = request(resource, 0);
        assert_eq!(prepare(&plan, &empty).unwrap(), expected);
        assert_eq!(empty.observed(resource), 0);
        assert_eq!(empty.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn borrowed_residual_projection_keeps_cumulative_admission_and_identity() {
    let mut plan = plan(Some(predicate()));
    finalize(&mut plan);
    let before = plan.clone();
    let signature =
        with_preparation_work(|work| plan.continuation_signature("tests::Token", work)).unwrap();
    let generous = request(Resource::TemporaryBytes, 16_000_000);
    let expected = project(&plan, &generous).unwrap();
    assert!(expected.is_some());

    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let used = generous.observed(resource);
        assert!(used > 0);
        let short = request(resource, used - 1);
        let error = project(&plan, &short).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        let exact = request(resource, 2 * used);
        for _ in 0..2 {
            assert_eq!(project(&plan, &exact).unwrap(), expected);
        }
        assert_eq!(exact.observed(resource), 2 * used);
        assert!(project(&plan, &exact).is_err());
        assert_eq!(exact.observed(Resource::RowsVisited), 0);
        assert_eq!(plan, before);
        assert_eq!(
            with_preparation_work(|work| plan.continuation_signature("tests::Token", work))
                .unwrap(),
            signature
        );
        assert!(plan.effective_execution_predicate().unwrap().is_some());
    }
}

#[test]
fn residual_preparation_copy_admission_precedes_backing_and_is_cumulative() {
    for finalized in [false, true] {
        let mut plan = plan(Some(predicate()));
        if finalized {
            finalize(&mut plan);
        }
        let before = plan.clone();
        let signature =
            with_preparation_work(|work| plan.continuation_signature("tests::Token", work))
                .unwrap();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let copy = |root: &RequestExecutionRoot| {
                PreparationWork::run(&root.scope(), lane, |work| {
                    plan.execution_preparation_predicate(work)
                })
            };
            let generous = request(Resource::TemporaryBytes, 16_000_000);
            let expected = copy(&generous).unwrap();
            assert_eq!(expected, Some(predicate()));
            for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
                let used = generous.observed(resource);
                let exact = request(resource, 2 * used);
                assert_eq!(copy(&exact).unwrap(), expected);
                assert_eq!(copy(&exact).unwrap(), expected);
                assert_eq!(exact.observed(resource), 2 * used);
                let error = copy(&exact).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
                assert_eq!(exact.observed(Resource::RowsVisited), 0);
            }
            let short = request(
                Resource::TemporaryBytes,
                (2 * size_of::<Predicate>() - 1) as u64,
            );
            assert!(copy(&short).is_err());
            assert_eq!(short.observed(Resource::PredicateExpressionSteps), 1);
            // Rejected charges record attempted work, not allocator activity.
            assert_eq!(
                short.observed(Resource::TemporaryBytes),
                (2 * size_of::<Predicate>()) as u64
            );
        }
        assert_eq!(plan, before);
        assert_eq!(
            with_preparation_work(|work| plan.continuation_signature("tests::Token", work))
                .unwrap(),
            signature
        );
    }
}

#[test]
fn residual_finalization_exhaustion_does_not_install_partial_contract() {
    let schema = newtype_query_schema();
    let freeze = |plan: &mut AccessPlannedQuery, root: &RequestExecutionRoot| {
        // Isolate static finalization after accepted projection validation.
        let projection =
            with_preparation_work(|work| plan.prepare_projection(&schema, work)).unwrap();
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            plan.finalize_static_execution_planning_contract_with_schema(&schema, projection, work)
        })
    };
    let original = plan(Some(predicate()));
    let signature =
        with_preparation_work(|work| original.continuation_signature("tests::Token", work))
            .unwrap();
    let generous = request(Resource::TemporaryBytes, 16_000_000);
    let mut successful = original.clone();
    freeze(&mut successful, &generous).unwrap();
    assert!(successful.static_execution_planning_contract.is_some());
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let used = generous.observed(resource);
        let short = request(resource, used - 1);
        let mut rejected = original.clone();
        assert!(freeze(&mut rejected, &short).is_err());
        assert_eq!(rejected, original);
        assert_eq!(
            with_preparation_work(|work| rejected.continuation_signature("tests::Token", work))
                .unwrap(),
            signature
        );
        let exact = request(resource, used * 2);
        for _ in 0..2 {
            let mut accepted = original.clone();
            freeze(&mut accepted, &exact).unwrap();
            assert_eq!(accepted, successful);
        }
        let mut rejected = original.clone();
        assert!(freeze(&mut rejected, &exact).is_err());
        assert_eq!(rejected, original);
        assert_eq!(exact.observed(Resource::RowsVisited), 0);
    }
}
