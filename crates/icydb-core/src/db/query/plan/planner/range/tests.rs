use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        numeric::compare_numeric_or_strict_order,
        query::{
            construction::ConstructionBudget,
            plan::planner::range::bounds::compare_range_bound_values, preparation::PreparationWork,
        },
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::cmp::Ordering;

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn run<T>(
    root: &RequestExecutionRoot,
    lane: Lane,
    action: impl FnOnce(&dyn ConstructionBudget) -> Result<T, InternalError>,
) -> Result<T, QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        action(work).map_err(QueryError::execute)
    })
}

fn assert_resource(error: QueryError, resource: Resource) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
    );
}

#[test]
fn range_comparisons_admit_payload_and_canonical_retry_cumulatively() {
    let cases = [
        (Value::Text("prefix".repeat(16)), 97, 2),
        // Lists have no strict ordering; canonical fallback walks the payload.
        (Value::List(vec![Value::Text("prefix".into())]), 13, 8),
        // A map's strict comparison can visit keys before declining a list value.
        (
            Value::Map(vec![(
                Value::Text("key".into()),
                Value::List(vec![Value::Text("value".into())]),
            )]),
            17,
            16,
        ),
    ];
    for (value, steps, nodes) in cases {
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (Resource::PredicateExpressionSteps, steps),
                (Resource::NestedValueSteps, nodes),
            ] {
                for limit in [0, exact - 1, exact * 2] {
                    let root = request(resource, limit);
                    for attempt in 1..=3 {
                        let result = run(&root, lane, |budget| {
                            compare_range_bound_values(&value, &value, budget)
                        });
                        if attempt * exact <= limit {
                            assert_eq!(result.unwrap(), Some(Ordering::Equal));
                            assert_eq!(root.observed(resource), attempt * exact);
                        } else {
                            assert_resource(result.unwrap_err(), resource);
                            break;
                        }
                    }
                    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn range_merge_exhaustion_stops_before_later_bounds() {
    use super::{RangeConstraint, bounds::merge_range_constraint_bounds};
    use std::ops::Bound;

    let mut range = RangeConstraint {
        lower: Bound::Included(Value::Text("lower".into())),
        upper: Bound::Excluded(Value::Text("upper".into())),
    };
    let expected = RangeConstraint {
        lower: range.lower.clone(),
        upper: range.upper.clone(),
    };
    let root = request(Resource::PredicateExpressionSteps, 1);
    let result = run(&root, Lane::Diagnostic, |budget| {
        merge_range_constraint_bounds(
            &mut range,
            RangeConstraint {
                lower: Bound::Excluded(Value::Text("lower".into())),
                upper: Bound::Excluded(Value::Text("next".into())),
            },
            budget,
        )
    });
    assert_resource(result.unwrap_err(), Resource::PredicateExpressionSteps);
    assert_eq!(range, expected);
    assert_eq!(root.observed(Resource::NestedValueSteps), 2);
}

#[test]
fn range_extraction_propagates_comparison_exhaustion_without_publishing_candidates() {
    use super::{index_range_from_and, primary_key_range_from_and};
    use crate::db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::{VisibleIndexes, planner::compare::prefix_tests},
    };

    let schema = prefix_tests::schema();
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let indexes = visible.accepted_semantic_index_contracts();
    for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
        for ops in [
            vec![
                (CompareOp::Gte, "aa"),
                (CompareOp::Gt, "ab"),
                (CompareOp::Lt, "az"),
            ],
            vec![(CompareOp::StartsWith, "a"), (CompareOp::StartsWith, "ab")],
            vec![(CompareOp::Eq, "aa"), (CompareOp::Eq, "aa")],
            vec![(CompareOp::Eq, "aa"), (CompareOp::Eq, "ab")],
        ] {
            let children: Vec<_> = ops
                .into_iter()
                .map(|(op, text)| {
                    Predicate::Compare(ComparePredicate::with_coercion(
                        "name",
                        op,
                        Value::Text(text.into()),
                        coercion,
                    ))
                })
                .collect();
            let before = children.clone();
            let action = |budget: &dyn ConstructionBudget| {
                index_range_from_and(indexes, &schema, &children, None, false, budget)
            };
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for resource in [
                    Resource::PredicateExpressionSteps,
                    Resource::NestedValueSteps,
                ] {
                    let baseline = request(resource, 16_000_000);
                    let expected = run(&baseline, lane, action).unwrap();
                    let used = baseline.observed(resource);
                    assert!(used > 0);
                    for limit in 0..used {
                        let short = request(resource, limit);
                        assert_resource(run(&short, lane, action).unwrap_err(), resource);
                    }
                    let exact = request(resource, used * 2);
                    assert_eq!(run(&exact, lane, action).unwrap(), expected);
                    assert_eq!(run(&exact, lane, action).unwrap(), expected);
                    assert_resource(run(&exact, lane, action).unwrap_err(), resource);
                    assert_eq!(exact.observed(Resource::RowsVisited), 0);
                }
            }
            assert_eq!(children, before);
        }
    }

    let children = [
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Gte,
            Value::Nat64(1),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Lt,
            Value::Nat64(3),
            CoercionId::Strict,
        )),
    ];
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let short = request(Resource::NestedValueSteps, 1);
        assert_resource(
            run(&short, lane, |budget| {
                primary_key_range_from_and(&schema, &children, budget)
            })
            .unwrap_err(),
            Resource::NestedValueSteps,
        );
        assert_eq!(short.observed(Resource::TemporaryBytes), 0);
        let exact = request(Resource::NestedValueSteps, 4);
        assert!(
            run(&exact, lane, |budget| primary_key_range_from_and(
                &schema, &children, budget
            ))
            .unwrap()
            .is_some()
        );
    }
}

#[test]
fn range_merges_move_text_bounds_and_keep_strict_ties() {
    let root = request(Resource::PredicateExpressionSteps, 16_000_000);
    run(&root, Lane::PublicRead, |budget| {
        use super::{
            RangeConstraint,
            bounds::{merge_range_constraint, merge_range_constraint_bounds},
        };
        use crate::db::predicate::CompareOp;
        use std::ops::Bound;

        let lower = "lower".repeat(128);
        let upper = "upper".repeat(128);
        let lower_pointer = lower.as_ptr();
        let upper_pointer = upper.as_ptr();
        let mut range = RangeConstraint::default();
        assert!(
            merge_range_constraint(&mut range, CompareOp::Gte, Value::Text(lower), budget).unwrap()
        );
        assert!(
            merge_range_constraint(&mut range, CompareOp::Lt, Value::Text(upper), budget).unwrap()
        );
        let Bound::Included(Value::Text(lower)) = &range.lower else {
            panic!("included lower");
        };
        assert_eq!(lower.as_ptr(), lower_pointer);
        let Bound::Excluded(Value::Text(upper)) = &range.upper else {
            panic!("excluded upper");
        };
        assert_eq!(upper.as_ptr(), upper_pointer);

        let stricter = "lower".repeat(128);
        let stricter_pointer = stricter.as_ptr();
        assert!(
            merge_range_constraint_bounds(
                &mut range,
                RangeConstraint {
                    lower: Bound::Excluded(Value::Text(stricter)),
                    upper: Bound::Unbounded,
                },
                budget
            )
            .unwrap()
        );
        let Bound::Excluded(Value::Text(lower)) = &range.lower else {
            panic!("exclusive tie wins");
        };
        assert_eq!(lower.as_ptr(), stricter_pointer);
        // A weaker bound leaves the retained upper allocation unchanged.
        assert!(
            merge_range_constraint(&mut range, CompareOp::Lte, Value::Text("z".into()), budget)
                .unwrap()
        );
        let Bound::Excluded(Value::Text(upper)) = &range.upper else {
            panic!("retained upper");
        };
        assert_eq!(upper.as_ptr(), upper_pointer);
        Ok(())
    })
    .unwrap();
}

#[test]
fn range_merges_preserve_singleton_empty_and_incomparable_intervals() {
    let root = request(Resource::PredicateExpressionSteps, 16_000_000);
    run(&root, Lane::PublicRead, |budget| {
        use super::{RangeConstraint, bounds::merge_range_constraint};
        use crate::db::predicate::CompareOp;

        for lower in [CompareOp::Gt, CompareOp::Gte] {
            for upper in [CompareOp::Lt, CompareOp::Lte] {
                let mut range = RangeConstraint::default();
                assert!(
                    merge_range_constraint(&mut range, lower, Value::Int64(7), budget).unwrap()
                );
                assert_eq!(
                    merge_range_constraint(&mut range, upper, Value::Nat64(7), budget).unwrap(),
                    lower == CompareOp::Gte && upper == CompareOp::Lte
                );
            }
        }
        let mut range = RangeConstraint::default();
        assert!(
            merge_range_constraint(&mut range, CompareOp::Gte, Value::Text("a".into()), budget)
                .unwrap()
        );
        assert!(
            !merge_range_constraint(&mut range, CompareOp::Lt, Value::Nat64(9), budget).unwrap()
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn range_bound_numeric_compare_reuses_shared_numeric_authority() {
    let root = request(Resource::PredicateExpressionSteps, 16_000_000);
    run(&root, Lane::PublicRead, |budget| {
        let left = Value::Int64(10);
        let right = Value::Nat64(10);

        assert_eq!(
            compare_range_bound_values(&left, &right, budget).unwrap(),
            compare_numeric_or_strict_order(&left, &right),
            "planner range numeric bounds should delegate to shared numeric comparator",
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn range_bound_mixed_non_numeric_values_are_incomparable() {
    let root = request(Resource::PredicateExpressionSteps, 16_000_000);
    run(&root, Lane::PublicRead, |budget| {
        assert_eq!(
            compare_range_bound_values(&Value::Text("x".to_string()), &Value::Nat64(1), budget)
                .unwrap(),
            None,
            "mixed non-numeric variants should remain incomparable in range planning",
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn range_bound_same_variant_non_numeric_uses_strict_ordering() {
    let root = request(Resource::PredicateExpressionSteps, 16_000_000);
    run(&root, Lane::PublicRead, |budget| {
        assert_eq!(
            compare_range_bound_values(
                &Value::Text("a".to_string()),
                &Value::Text("b".to_string()),
                budget
            )
            .unwrap(),
            Some(Ordering::Less),
            "same-variant non-numeric bounds should use strict value ordering",
        );
        Ok(())
    })
    .unwrap();
}
