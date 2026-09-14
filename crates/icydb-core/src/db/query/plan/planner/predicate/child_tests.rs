//! Exact child-key reduction borrows intermediate sets and copies survivors once.

use super::{intersect_canonical_value_sets, primary_key_child_access_candidate};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{AccessPath, AccessPlan},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{plan::PlannedNonIndexAccessReason as Reason, preparation::PreparationWork},
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

fn check_admission(
    children: &[AccessPlan<Value>],
    expected: Option<(AccessPlan<Value>, Reason)>,
    bytes: u64,
    steps: u64,
    copies: u64,
) {
    let original = children.to_vec();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (resource, exact) in [
            (Resource::TemporaryBytes, bytes),
            (Resource::PredicateExpressionSteps, steps),
            (Resource::NestedValueSteps, copies),
        ] {
            for limit in [0, exact.saturating_sub(1), exact * 2] {
                let root = request(resource, limit);
                PreparationWork::run(&root.scope(), lane, |work| {
                    for attempt in 1..=3 {
                        let result = primary_key_child_access_candidate(children, work);
                        if exact == 0 || attempt * exact <= limit {
                            let result = result.unwrap().map(|(selection, _)| {
                                let (access, reason) = selection.into_access_and_non_index_reason();
                                (access, reason.unwrap())
                            });
                            assert_eq!(result, expected);
                            assert_eq!(root.observed(resource), attempt * exact);
                        } else {
                            assert!(
                                QueryError::execute(result.unwrap_err())
                                    .diagnostic_facts()
                                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                            );
                            break;
                        }
                    }
                    Ok(())
                })
                .unwrap();
                assert_eq!(children, original);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn child_key_reduction_admits_views_and_only_copies_surviving_keys() {
    let a = Value::Text("a".repeat(128));
    let b = Value::Text("b".repeat(128));
    let c = Value::Text("c".repeat(128));
    let d = Value::Text("d".repeat(128));
    let path = size_of::<AccessPath<Value>>() as u64;
    let reference = size_of::<&Value>() as u64;
    check_admission(
        &[
            AccessPlan::by_keys(vec![b.clone(), a.clone(), b.clone(), c.clone()]),
            AccessPlan::by_keys(vec![c.clone(), b.clone(), d]),
        ],
        Some((
            AccessPlan::by_keys(vec![b.clone(), c]),
            Reason::PlannerKeySetAccess,
        )),
        7 * reference + 2 * size_of::<Value>() as u64 + 256 + path,
        17 + 256,
        2,
    );
    check_admission(
        &[
            AccessPlan::by_keys(vec![b.clone(), a.clone(), b.clone()]),
            AccessPlan::by_key(b.clone()),
        ],
        Some((
            AccessPlan::by_key(b.clone()),
            Reason::SingletonPrimaryKeyChildAccessPreferred,
        )),
        4 * reference + 128 + path,
        11 + 128,
        1,
    );
    check_admission(
        &[AccessPlan::by_key(a.clone()), AccessPlan::by_key(b.clone())],
        Some((
            AccessPlan::by_keys(Vec::new()),
            Reason::ConflictingPrimaryKeyChildrenAccessPreferred,
        )),
        2 * reference + path,
        8,
        0,
    );
    check_admission(
        &[
            AccessPlan::by_key(a),
            AccessPlan::by_key(b),
            AccessPlan::by_keys(Vec::new()),
        ],
        Some((
            AccessPlan::by_keys(Vec::new()),
            Reason::EmptyChildAccessPreferred,
        )),
        path,
        6,
        0,
    );
    check_admission(&[AccessPlan::full_scan()], None, 0, 2, 0);
    check_admission(&[], None, 0, 0, 0);
}

#[test]
fn borrowed_key_intersection_preserves_left_representatives_and_storage() {
    let left = [
        Value::Text("a".into()),
        Value::Text("b".into()),
        Value::Text("c".into()),
    ];
    let right = [
        Value::Text("b".into()),
        Value::Text("c".into()),
        Value::Text("d".into()),
    ];
    let mut retained: Vec<_> = left.iter().collect();
    let backing = retained.as_ptr();
    intersect_canonical_value_sets(&mut retained, &right.iter().collect::<Vec<_>>());
    assert_eq!(retained, vec![&left[1], &left[2]]);
    assert_eq!(retained.as_ptr(), backing);
    assert!(std::ptr::eq(retained[0], &raw const left[1]));
    assert!(std::ptr::eq(retained[1], &raw const left[2]));
    intersect_canonical_value_sets(&mut retained, &[]);
    assert!(retained.is_empty());
    assert_eq!(retained.as_ptr(), backing);
}

#[test]
fn borrowed_key_intersection_matches_all_small_set_pairs() {
    let values = [Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)];
    for left_mask in 0_u8..8 {
        for right_mask in 0_u8..8 {
            let mut left: Vec<_> = values
                .iter()
                .enumerate()
                .filter_map(|(slot, value)| (left_mask & (1 << slot) != 0).then_some(value))
                .collect();
            let right: Vec<_> = values
                .iter()
                .enumerate()
                .filter_map(|(slot, value)| (right_mask & (1 << slot) != 0).then_some(value))
                .collect();
            let expected: Vec<_> = left
                .iter()
                .copied()
                .filter(|value| right.contains(value))
                .collect();
            let backing = left.as_ptr();
            intersect_canonical_value_sets(&mut left, &right);
            assert_eq!(left, expected);
            assert_eq!(left.as_ptr(), backing);
        }
    }
}
