//! Logical diagnostics borrow finalized scalar/grouped plans on cold and warm calls.

use super::*;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::{
            SharedPreparedExecutionPlan,
            budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        },
        predicate::Predicate,
        query::{
            builder::count,
            explain::{ExplainGrouping, ExplainPlan},
            plan::{AccessPlannedQuery, GroupAggregateSpec},
            preparation::{PreparationWork, with_preparation_work},
        },
    },
    error::InternalError,
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

fn explain(
    plan: &SharedPreparedExecutionPlan,
    root: &RequestExecutionRoot,
) -> Result<ExplainPlan, QueryError> {
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| plan.explain(work))
}

fn assert_projection_rejections(plan: &SharedPreparedExecutionPlan, resource: Resource, cost: u64) {
    let original = plan.logical_plan().clone();
    let signature = original.continuation_signature(ENTITY_NAME);
    // A prepared plan retains neither a diagnostic allowance nor a partial
    // result after failing partway through projection.
    for limit in [0, cost / 2, cost - 1] {
        let short = request(resource, limit);
        let error = explain(plan, &short).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
        );
        assert_eq!(plan.logical_plan(), &original);
        assert_eq!(
            plan.logical_plan().continuation_signature(ENTITY_NAME),
            signature
        );
        assert_eq!(short.observed(Resource::RowsVisited), 0);
        assert_eq!(short.observed(Resource::PlanCompilations), 0);
    }
}

#[test]
fn prepared_explain_preserves_cold_warm_residual_plans_and_cumulative_admission() {
    let session = initialize();
    seed_rows(&session);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let scalar = StructuralQuery::new(MissingRowPolicy::Ignore).filter_normalized_predicate(
        Predicate::And(vec![
            Predicate::eq("rare".into(), Value::Text("group-a".into())),
            Predicate::TextContains {
                field: "common".into(),
                value: Value::Text("every".into()),
            },
        ]),
    );
    let grouped = with_preparation_work(|work| {
        scalar.clone().group_fields_with_schema(
            &["rare".into()],
            catalog.accepted_schema_info(),
            work,
        )
    })
    .unwrap()
    .group_aggregates(vec![GroupAggregateSpec::from_aggregate_expr(count())])
    .grouped_limits(16, 4096);
    for (query, is_grouped) in [(scalar, false), (grouped, true)] {
        let prepare = || {
            session
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority(),
                    &catalog,
                    &query,
                    Lane::Diagnostic,
                )
                .unwrap()
        };
        let (cold, cold_reuse) = prepare();
        let (warm, warm_reuse) = prepare();
        assert!(!cold_reuse.is_hit());
        assert!(warm_reuse.is_hit());
        assert!(warm.logical_plan().has_static_execution_planning_contract());
        assert!(warm.logical_plan().has_any_residual_filter());
        assert_eq!(cold.logical_plan(), warm.logical_plan());
        let original = warm.logical_plan().clone();
        let signature = original.continuation_signature(ENTITY_NAME);
        let generous = request(Resource::TemporaryBytes, 16_000_000);
        let expected = explain(&cold, &generous).unwrap();
        assert_eq!(
            matches!(expected.grouping(), ExplainGrouping::Grouped { .. }),
            is_grouped,
        );
        let text = expected.render_text_canonical().unwrap();
        let json = expected.render_json_canonical().unwrap();
        for resource in [
            Resource::TemporaryBytes,
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
        ] {
            let cost = generous.observed(resource);
            assert!(cost > 0);
            for plan in [&cold, &warm] {
                assert_projection_rejections(plan, resource, cost);
            }
            let root = request(resource, 2 * cost);
            for _ in 0..2 {
                let projected = explain(&warm, &root).unwrap();
                assert_eq!(projected, expected);
                assert_eq!(projected.render_text_canonical().unwrap(), text);
                assert_eq!(projected.render_json_canonical().unwrap(), json);
            }
            assert_eq!(root.observed(resource), 2 * cost);
            let error = explain(&warm, &root).unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
            );
            assert_eq!(root.observed(Resource::RowsVisited), 0);
            assert_eq!(root.observed(Resource::PlanCompilations), 0);
            assert_eq!(warm.logical_plan(), &original);
            assert_eq!(
                warm.logical_plan().continuation_signature(ENTITY_NAME),
                signature
            );
            // Detached rendering neither borrows nor restarts the exhausted request.
            let used = root.observed(resource);
            assert_eq!(expected.render_text_canonical().unwrap(), text);
            assert_eq!(expected.render_json_canonical().unwrap(), json);
            assert_eq!(root.observed(resource), used);
        }
        let (again, reuse) = prepare();
        assert!(reuse.is_hit());
        assert_eq!(again.logical_plan(), &original);
        assert_eq!(
            explain(&again, &request(Resource::TemporaryBytes, 16_000_000)).unwrap(),
            expected,
        );
    }
}

#[test]
fn prepared_explain_handoff_requires_finalized_execution_facts() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let authority = catalog.accepted_entity_authority();
    let root = request(Resource::TemporaryBytes, 0);
    let error = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        SharedPreparedExecutionPlan::from_plan(
            authority.clone(),
            AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore),
            authority.accepted_schema_fingerprint(),
            work,
        )
        .map_err(QueryError::execute)
    })
    .unwrap_err();
    assert_eq!(
        error.diagnostic(),
        QueryError::execute(InternalError::query_executor_invariant()).diagnostic(),
    );
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}
