//! New normalization vectors must use the caller's cumulative construction budget.

use crate::{
    db::{
        QueryError,
        predicate::{ComparePredicate, Predicate, normalize, normalize_with_budget},
        query::preparation::PreparationWork,
        tests::support::request_with_limit,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn leaf(field: &str) -> Predicate {
    Predicate::IsNull {
        field: field.into(),
    }
}

fn membership() -> Predicate {
    Predicate::Or(
        [3, 1, 3]
            .map(|value| {
                Predicate::Compare(ComparePredicate::eq("tag".into(), Value::Nat64(value)))
            })
            .into(),
    )
}

#[test]
fn normalization_new_backing_is_exact_cumulative_and_typed() {
    let cases = [
        (
            Predicate::And(vec![leaf("c"), Predicate::And(vec![leaf("a"), leaf("b")])]),
            3 * size_of::<Predicate>() as u64,
        ),
        (
            Predicate::Or(vec![leaf("c"), Predicate::Or(vec![leaf("a"), leaf("b")])]),
            3 * size_of::<Predicate>() as u64,
        ),
        (membership(), 3 * size_of::<Value>() as u64),
    ];
    for (predicate, vector_bytes) in cases {
        let expected = normalize(predicate.clone());
        let baseline = request_with_limit(Resource::TemporaryBytes, u64::MAX);
        PreparationWork::run(&baseline.scope(), Lane::PublicRead, |work| {
            normalize_with_budget(predicate.clone(), work).map_err(QueryError::execute)
        })
        .unwrap();
        let bytes = baseline.observed(Resource::TemporaryBytes);
        assert!(bytes >= vector_bytes);
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            for limit in [bytes - 1, bytes, bytes * 2] {
                let root = request_with_limit(Resource::TemporaryBytes, limit);
                for attempt in 1..=3 {
                    let input = predicate.clone();
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        normalize_with_budget(input, work).map_err(QueryError::execute)
                    });
                    if attempt * bytes <= limit {
                        assert_eq!(result.unwrap(), expected);
                        assert_eq!(root.observed(Resource::TemporaryBytes), attempt * bytes);
                    } else {
                        let error = result.unwrap_err();
                        assert!(error.diagnostic_facts().contains(&(
                            DiagnosticFactTag::BudgetResource,
                            Resource::TemporaryBytes.raw(),
                        )));
                        assert!(
                            error
                                .diagnostic_facts()
                                .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw(),))
                        );
                        for fact in [
                            (DiagnosticFactTag::Limit, limit),
                            (
                                DiagnosticFactTag::Actual,
                                root.observed(Resource::TemporaryBytes),
                            ),
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

#[test]
fn normalization_unsorted_owned_and_absorbed_branches_need_no_new_vector() {
    let equal = Predicate::Compare(ComparePredicate::eq("tag".into(), Value::Nat64(1)));
    for predicate in [
        Predicate::And(vec![leaf("a")]),
        Predicate::Or(vec![leaf("a")]),
        Predicate::Not(Box::new(leaf("a"))),
        Predicate::Or(vec![equal; 3]),
        Predicate::And(vec![Predicate::False, membership()]),
        Predicate::Or(vec![Predicate::True, membership()]),
    ] {
        let expected = normalize(predicate.clone());
        let root = request_with_limit(Resource::TemporaryBytes, 0);
        let actual = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            normalize_with_budget(predicate, work).map_err(QueryError::execute)
        })
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn normalization_backing_refusal_stops_before_later_pair_work() {
    let predicate = Predicate::And(vec![
        membership(),
        Predicate::And(vec![leaf("a"), leaf("b")]),
    ]);
    let root = request_with_limit(Resource::TemporaryBytes, 0);
    let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        normalize_with_budget(predicate, work).map_err(QueryError::execute)
    });
    assert!(result.is_err());
    assert_eq!(
        root.observed(Resource::TemporaryBytes),
        3 * size_of::<Value>() as u64
    );
    // The outer AND, membership OR and its three leaves were visited. Backing
    // refusal follows two domain checks (one step plus three field bytes each),
    // but prevents the later conjunction and all of its comparison work.
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 5 + 2 * 4);
}

#[test]
fn membership_backing_refusal_preserves_owned_operands() {
    let Predicate::Or(mut children) = membership() else {
        panic!("membership fixture is a disjunction");
    };
    let before = children.clone();
    let result = crate::db::predicate::normalize::collapse_same_field_or_equalities(
        &mut children,
        &mut |resource, bytes| {
            if resource == Resource::TemporaryBytes {
                assert_eq!(bytes, 3 * size_of::<Value>() as u64);
                Err(17)
            } else {
                Ok(())
            }
        },
    );
    assert_eq!(result, Err(17));
    assert_eq!(children, before);
}

#[test]
fn normalization_sort_keys_refuse_without_construction_backing() {
    let predicate = Predicate::Or(vec![leaf("second"), leaf("first")]);
    let root = request_with_limit(Resource::TemporaryBytes, 0);
    let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        normalize_with_budget(predicate, work).map_err(QueryError::execute)
    });
    let error = result.expect_err("ordering keys need admitted backing");
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
}
