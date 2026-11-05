use super::explain_predicate_for_plan;
use crate::db::{
    Predicate, RequestExecutionRoot,
    executor::explain::descriptor::shared::PredicateStageObservability,
    predicate::MissingRowPolicy,
    query::{
        plan::{AccessPlannedQuery, LogicalPlan, ResidualFilterShape},
        preparation::PreparationWork,
    },
    resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use crate::value::Value;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

#[test]
fn execution_predicate_projection_uses_request_budget_without_identity_copy() {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.predicate = Some(Predicate::eq("age".into(), Value::Int64(7)));
    let schema = crate::db::query::plan::exact_metadata_schema(&[], &[]);
    crate::db::query::preparation::with_preparation_work(|work| {
        let projection = plan.prepare_projection(&schema, work)?;
        plan.finalize_static_execution_planning_contract_with_schema(&schema, projection, work)
    })
    .unwrap();
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 3),
    );
    let run = || {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            explain_predicate_for_plan(&plan, work)
        })
    };
    assert!(run().unwrap().is_some());
    assert_eq!(root.observed(Resource::TemporaryBytes), 3);
    assert_eq!(root.observed(Resource::NestedValueSteps), 1);
    assert!(run().is_err());
    assert_eq!(root.observed(Resource::RowsVisited), 0);
}

#[test]
fn predicate_stage_observability_prefers_strict_prefilter() {
    assert_eq!(
        PredicateStageObservability::from_parts(true, ResidualFilterShape::ExpressionAndPredicate,),
        PredicateStageObservability::IndexPrefilterStrictAllOrNone,
        "route-owned strict prefilter stage should win over residual labels",
    );
}

#[test]
fn predicate_stage_observability_reports_residual_and_absent_shapes() {
    assert_eq!(
        PredicateStageObservability::from_parts(false, ResidualFilterShape::Expression).label(),
        "residual_runtime",
    );
    assert_eq!(
        PredicateStageObservability::from_parts(false, ResidualFilterShape::Absent).label(),
        "none",
    );
}
