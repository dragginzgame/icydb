use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::{plan::expr::FieldPath, preparation::PreparationWork},
    sql::lowering::analysis::{LoweredExprAnalysis, LoweredExprSourceRef},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};

fn root(steps: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 0)
        .with_limit_for_tests(Resource::PredicateExpressionSteps, steps),
    )
}

fn check(
    request: &RequestExecutionRoot,
    source: LoweredExprSourceRef,
    labels: &[&str],
    lane: DiagnosticExecutionLane,
) -> Result<bool, QueryError> {
    // Authored inputs and analysis already exist before this membership proof.
    let labels: Vec<_> = labels.iter().map(|label| (*label).to_string()).collect();
    let analysis = LoweredExprAnalysis {
        source_refs: vec![source],
        ..LoweredExprAnalysis::default()
    };
    PreparationWork::run(&request.scope(), lane, |work| {
        analysis.references_only_group_fields(&labels, work)
    })
}

#[test]
fn borrowed_membership_preserves_direct_and_structural_path_identity() {
    for (source, labels, expected) in [
        (
            LoweredExprSourceRef::Direct("name".into()),
            vec!["id", "name", "name"],
            true,
        ),
        (
            LoweredExprSourceRef::Direct("profile.city".into()),
            vec!["profile.city"],
            false,
        ),
        (
            LoweredExprSourceRef::Path(FieldPath::new("profile", vec!["city".into()])),
            vec!["name", "profile.city"],
            true,
        ),
        (
            LoweredExprSourceRef::Path(FieldPath::new("profile", vec!["city".into()])),
            vec!["profile.city_code"],
            false,
        ),
        (
            LoweredExprSourceRef::Path(FieldPath::new("profile", vec!["city.code".into()])),
            vec!["profile.city.code"],
            false,
        ),
        (
            LoweredExprSourceRef::Path(FieldPath::new("é", vec!["名".into()])),
            vec!["é.名"],
            true,
        ),
        (LoweredExprSourceRef::Direct("name".into()), vec![], false),
    ] {
        let request = root(16_000_000);
        assert_eq!(
            check(
                &request,
                source,
                &labels,
                DiagnosticExecutionLane::PublicRead
            )
            .unwrap(),
            expected
        );
        assert_eq!(request.observed(Resource::TemporaryBytes), 0);
        assert_eq!(request.observed(Resource::RowsVisited), 0);
        assert_eq!(request.observed(Resource::QueryExecutions), 0);
    }
}

#[test]
fn membership_exhaustion_is_typed_cumulative_and_not_a_negative_match() {
    let steps = 1 + 2 * "profile.city".len() as u64;
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        let source = || LoweredExprSourceRef::Path(FieldPath::new("profile", vec!["city".into()]));
        let exact = root(steps);
        assert!(check(&exact, source(), &["profile.city"], lane).unwrap());
        assert_eq!(exact.observed(Resource::PredicateExpressionSteps), steps);
        let error = check(&exact, source(), &["profile.city"], lane).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw()
        )));
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
        );
        assert_eq!(
            exact.observed(Resource::PredicateExpressionSteps),
            2 * steps
        );
        assert_eq!(exact.observed(Resource::RowsVisited), 0);
        assert_eq!(exact.observed(Resource::QueryExecutions), 0);
        let below = root(steps - 1);
        assert!(check(&below, source(), &["profile.city"], lane).is_err());
        assert!(check(&root(steps), source(), &["profile.city"], lane).unwrap());
    }
}

#[test]
fn membership_charges_only_candidates_examined_before_the_first_match() {
    let steps = 1 + 2 * "name".len() as u64;
    assert!(
        check(
            &root(steps),
            LoweredExprSourceRef::Direct("name".into()),
            &["name", "unused", "name"],
            DiagnosticExecutionLane::PublicRead,
        )
        .unwrap()
    );
    assert!(
        !check(
            &root(steps),
            LoweredExprSourceRef::Direct("different".into()),
            &["name"],
            DiagnosticExecutionLane::PublicRead,
        )
        .unwrap()
    );
}
