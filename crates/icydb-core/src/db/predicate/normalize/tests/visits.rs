//! Nodes without pair or sort work still consume the caller's allowance.

use crate::db::{
    QueryError,
    predicate::{Predicate, normalize, normalize_with_budget},
    query::preparation::PreparationWork,
    tests::support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn normalization_node_visits_are_exact_cumulative_and_typed() {
    let leaf = Predicate::IsNull { field: "a".into() };
    let cases = [
        (Predicate::True, 1),
        (Predicate::False, 1),
        (leaf.clone(), 1),
        (Predicate::Not(Box::new(leaf.clone())), 2),
        (
            Predicate::Not(Box::new(Predicate::Not(Box::new(leaf.clone())))),
            3,
        ),
        // Three normalization visits plus the simplifier's surviving guard scan.
        (Predicate::And(vec![Predicate::True, leaf.clone()]), 4),
        (Predicate::Or(vec![Predicate::False, leaf.clone()]), 3),
        // The later nested tree is not visited once an absorbing child wins.
        (
            Predicate::And(vec![
                Predicate::False,
                Predicate::Not(Box::new(leaf.clone())),
            ]),
            2,
        ),
        (
            Predicate::Or(vec![Predicate::True, Predicate::Not(Box::new(leaf))]),
            2,
        ),
    ];
    for (predicate, visits) in cases {
        let expected = normalize(predicate.clone());
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            for limit in [visits - 1, visits, visits * 2] {
                let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
                for attempt in 1..=3 {
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        normalize_with_budget(predicate.clone(), work).map_err(QueryError::execute)
                    });
                    if attempt * visits <= limit {
                        assert_eq!(result.unwrap(), expected);
                        assert_eq!(
                            root.observed(Resource::PredicateExpressionSteps),
                            attempt * visits
                        );
                    } else {
                        let error = result.unwrap_err();
                        for fact in [
                            (
                                DiagnosticFactTag::BudgetResource,
                                Resource::PredicateExpressionSteps.raw(),
                            ),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                            (DiagnosticFactTag::Actual, limit + 1),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                        break;
                    }
                }
            }
        }
    }
}
