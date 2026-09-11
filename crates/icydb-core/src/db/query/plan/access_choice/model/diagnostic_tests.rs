//! Candidate snapshots are copied only under diagnostic construction authority.

use super::*;
use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::MissingRowPolicy,
    query::{
        explain::{
            ExplainAccessCandidate, ExplainEligibleAlternative, ExplainPlan, ExplainRejectedIndex,
        },
        plan::AccessPlannedQuery,
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn project(
    query: &AccessPlannedQuery,
    root: &RequestExecutionRoot,
) -> Result<ExplainPlan, QueryError> {
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        query.project_explain(work)
    })
}

fn candidate(kind: AccessChoiceCandidateKind) -> AccessChoiceCandidateExplainSummary {
    AccessChoiceCandidateExplainSummary {
        kind,
        index_name: "quoted'λ".into(),
        exact: true,
        filtered: true,
        range_bound_count: 2,
        order_compatible: false,
        residual_burden: AccessChoiceResidualBurden::ScalarExpression,
        residual_predicate_terms: 3,
        exact_prefix_entries: Some(19),
    }
}

fn fixture() -> AccessPlannedQuery {
    let mut query = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    query.access_choice.candidates = [
        AccessChoiceCandidateKind::Prefix,
        AccessChoiceCandidateKind::MultiLookup,
        AccessChoiceCandidateKind::BranchSet,
        AccessChoiceCandidateKind::Range,
    ]
    .into_iter()
    .map(candidate)
    .collect();
    query.access_choice.alternatives = vec!["second".into(), String::new(), "first".into()];
    query.access_choice.rejected = vec![AccessChoiceRejectedIndex::new(
        "rejected'λ".into(),
        AccessChoiceRejectedReason::OperatorNotSupported,
    )];
    query
}

#[test]
fn decision_projection_preserves_candidate_facts_labels_and_list_order() {
    let query = fixture();
    let plan = project(&query, &root(Resource::TemporaryBytes, 16_000_000)).unwrap();
    let decision = plan.access_decision();
    let labels = [
        "IndexPrefix(quoted'λ)",
        "IndexMultiLookup(quoted'λ)",
        "IndexBranchSet(quoted'λ)",
        "IndexRange(quoted'λ)",
    ];
    for (projected, label) in decision.candidates.iter().zip(labels) {
        assert_eq!(
            projected,
            &ExplainAccessCandidate {
                label: label.into(),
                exact: true,
                filtered: true,
                range_bound_count: 2,
                order_compatible: false,
                residual_burden: "scalar_expression",
                residual_predicate_terms: 3,
                exact_prefix_entries: Some(19),
            }
        );
    }
    assert_eq!(
        decision
            .alternatives
            .iter()
            .map(|item| item.index_name.as_str())
            .collect::<Vec<_>>(),
        ["second", "", "first"]
    );
    assert_eq!(
        decision.rejections,
        [ExplainRejectedIndex {
            index_name: Some("rejected'λ".into()),
            reason: Some("operator_not_supported".into()),
            label: "index:rejected'λ=operator_not_supported".into(),
        }]
    );
    assert_eq!(decision.selected.label, "FullScan");
    assert_eq!(decision.residual.burden_class, "none");
}

#[test]
fn decision_projection_repeated_calls_exhaust_without_mutating_identity() {
    let query = fixture();
    let before = query.clone();
    let signature = query.continuation_signature("tests::Entity");
    let generous = root(Resource::TemporaryBytes, 16_000_000);
    let expected = project(&query, &generous).unwrap();
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let used = generous.observed(resource);
        assert!(used > 0);
        for limit in [0, used / 2, used - 1] {
            let short = root(resource, limit);
            let error = project(&query, &short).unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
            );
            assert_eq!(query, before);
            assert_eq!(query.continuation_signature("tests::Entity"), signature);
            assert_eq!(short.observed(Resource::RowsVisited), 0);
        }
        let exact = root(resource, 2 * used);
        assert_eq!(project(&query, &exact).unwrap(), expected);
        assert_eq!(project(&query, &exact).unwrap(), expected);
        let error = project(&query, &exact).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        assert_eq!(query, before);
        assert_eq!(query.continuation_signature("tests::Entity"), signature);
        assert_eq!(exact.observed(Resource::RowsVisited), 0);
        let exhausted = exact.observed(resource);
        assert!(expected.render_json_canonical().is_ok());
        assert!(expected.render_text_canonical().is_ok());
        assert_eq!(exact.observed(resource), exhausted);
    }
    let next = project(&query, &root(Resource::TemporaryBytes, 16_000_000)).unwrap();
    assert_eq!(next, expected);
}

#[test]
fn decision_projection_admits_each_list_backing_before_its_first_payload() {
    let empty = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let baseline = root(Resource::TemporaryBytes, 16_000_000);
    project(&empty, &baseline).unwrap();
    let mut candidates = empty.clone();
    candidates.access_choice.candidates = vec![candidate(AccessChoiceCandidateKind::Prefix); 8];
    let mut alternatives = empty.clone();
    alternatives.access_choice.alternatives = vec!["payload".into(); 8];
    let mut rejected = empty;
    rejected.access_choice.rejected = vec![
        AccessChoiceRejectedIndex::new(
            "payload".into(),
            AccessChoiceRejectedReason::OperatorNotSupported,
        );
        8
    ];
    for (query, item_size) in [
        (candidates, size_of::<ExplainAccessCandidate>()),
        (alternatives, size_of::<ExplainEligibleAlternative>()),
        (rejected, size_of::<ExplainRejectedIndex>()),
    ] {
        let short = root(
            Resource::TemporaryBytes,
            baseline.observed(Resource::TemporaryBytes) + 8 * item_size as u64 - 1,
        );
        assert!(project(&query, &short).is_err());
        // No list payload or final residual-summary visit occurred.
        assert_eq!(
            short.observed(Resource::PredicateExpressionSteps),
            baseline.observed(Resource::PredicateExpressionSteps) - 1
        );
        assert_eq!(short.observed(Resource::RowsVisited), 0);
    }
}
