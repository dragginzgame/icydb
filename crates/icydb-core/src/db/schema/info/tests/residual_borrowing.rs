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
use std::borrow::Cow;

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
            .as_deref()
            .map(|predicate| ExplainPredicate::from_predicate(predicate, work))
            .transpose()
    })
}

#[test]
fn finalized_residual_inspection_borrows_the_frozen_predicate() {
    for predicate in [None, Some(predicate())] {
        let mut plan = plan(predicate);
        let expected = plan.effective_execution_predicate().map(Cow::into_owned);
        finalize(&mut plan);
        let frozen = plan
            .static_execution_planning_contract
            .as_ref()
            .unwrap()
            .residual_filter_contract
            .residual_filter_predicate();
        assert_eq!(frozen, expected.as_ref());

        for _ in 0..4 {
            let view = plan.effective_execution_predicate();
            assert_eq!(view.as_deref(), frozen);
            assert_eq!(plan.has_residual_filter_predicate(), frozen.is_some());
            if let Some(view) = view {
                assert!(matches!(view, Cow::Borrowed(_)));
                assert!(std::ptr::eq(view.as_ref(), frozen.unwrap()));
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
fn unfinished_and_finalized_residual_inspection_preserve_semantics() {
    let mut plan = plan(Some(predicate()));
    let derived = plan.effective_execution_predicate().unwrap().into_owned();
    finalize(&mut plan);
    assert_eq!(
        plan.effective_execution_predicate().as_deref(),
        Some(&derived)
    );
}

#[test]
fn borrowed_residual_projection_keeps_cumulative_admission_and_identity() {
    let mut plan = plan(Some(predicate()));
    finalize(&mut plan);
    let before = plan.clone();
    let signature = plan.continuation_signature("tests::Token");
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
        assert_eq!(plan.continuation_signature("tests::Token"), signature);
        assert!(matches!(
            plan.effective_execution_predicate(),
            Some(Cow::Borrowed(_))
        ));
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
        let signature = plan.continuation_signature("tests::Token");
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
        assert_eq!(plan.continuation_signature("tests::Token"), signature);
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
    let signature = original.continuation_signature("tests::Token");
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
        assert_eq!(rejected.continuation_signature("tests::Token"), signature);
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
