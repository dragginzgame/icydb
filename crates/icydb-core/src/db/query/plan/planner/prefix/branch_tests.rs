//! Branch operands share admitted conversion and preserve set/exclusion semantics.

use super::{
    CachedInValue, CachedSetLiteral, build_index_branch_values, prune_branch_values_by_exclusions,
};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{SemanticIndexExpression, SemanticIndexKeyItem},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::CoercionId,
        query::preparation::PreparationWork,
        schema::PersistedIndexExpressionOp,
    },
    value::{Value, lower_text_construction_allowance},
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

fn literal(values: &[Value], coercion: CoercionId) -> CachedSetLiteral<'_> {
    CachedSetLiteral {
        field: "name",
        values: values
            .iter()
            .map(|value| CachedInValue {
                value,
                compatible: true,
            })
            .collect(),
        coercion,
    }
}

#[test]
fn branch_selection_reuses_order_score_without_changing_eligibility_or_ties() {
    use crate::db::{
        predicate::Predicate,
        query::{
            plan::{OrderDirection, OrderSpec, OrderTerm, VisibleIndexes, exact_metadata_schema},
            preparation::with_preparation_work,
        },
    };

    let schema = exact_metadata_schema(
        &[("a", &["age", "rank", "id"]), ("z", &["age", "rank", "id"])],
        &[],
    );
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let children = [
        Predicate::eq("age".into(), Value::Int64(7)),
        Predicate::in_("rank".into(), vec![Value::Int64(2), Value::Int64(1)]),
    ];
    let asc = OrderSpec {
        fields: vec![OrderTerm::field("id", OrderDirection::Asc)],
    };
    let desc = OrderSpec {
        fields: vec![OrderTerm::field("id", OrderDirection::Desc)],
    };
    let other = OrderSpec {
        fields: vec![OrderTerm::field("age", OrderDirection::Asc)],
    };
    for (order, grouped, expected) in [
        (None, false, true),
        (Some(&asc), false, true),
        (Some(&desc), false, false),
        (Some(&other), false, false),
        (None, true, false),
    ] {
        let plan = with_preparation_work(|work| {
            super::index_branch_set_from_and(
                visible.accepted_semantic_index_contracts(),
                &schema,
                &children,
                order,
                grouped,
                work,
            )
        })
        .unwrap();
        assert_eq!(plan.is_some(), expected);
        if let Some(plan) = plan {
            let spec = plan.as_path().unwrap().as_index_branch_set_spec().unwrap();
            assert_eq!(spec.index_ref().name(), "a");
            assert_eq!(spec.fixed_values(), &[Value::Int64(7)]);
            assert_eq!(spec.branch_values(), &[Value::Int64(1), Value::Int64(2)]);
        }
    }
}

#[test]
fn branch_values_admit_backing_and_conversion_before_canonicalization() {
    let values = [Value::Text("İ".into()), Value::Text("İ".into())];
    for (key, coercion, bytes, steps, visits, expected) in [
        (
            SemanticIndexKeyItem::Field("name".into()),
            CoercionId::Strict,
            4,
            4,
            2,
            "İ",
        ),
        {
            let (bytes, steps) = lower_text_construction_allowance(2);
            (
                SemanticIndexKeyItem::Expression(SemanticIndexExpression::new(
                    PersistedIndexExpressionOp::Lower,
                    "name".into(),
                )),
                CoercionId::TextCasefold,
                2 * bytes,
                2 * steps,
                0,
                "i\u{307}",
            )
        },
    ] {
        let literals = [literal(&values, coercion)];
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (
                    Resource::TemporaryBytes,
                    (2 * size_of::<Value>()) as u64 + bytes,
                ),
                (Resource::PredicateExpressionSteps, 3 + steps),
                (Resource::NestedValueSteps, visits),
            ] {
                for limit in [0, exact.saturating_sub(1), exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 1..=3 {
                            let result = build_index_branch_values(key.as_ref(), &literals, work);
                            if exact == 0 || attempt * exact <= limit {
                                assert_eq!(
                                    result.unwrap(),
                                    Some(vec![Value::Text(expected.into())])
                                );
                                assert_eq!(root.observed(resource), attempt * exact);
                            } else {
                                assert!(
                                    QueryError::execute(result.unwrap_err())
                                        .diagnostic_facts()
                                        .contains(&(
                                            DiagnosticFactTag::BudgetResource,
                                            resource.raw()
                                        ))
                                );
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn exclusion_normalizes_once_and_borrows_raw_values_for_all_branches() {
    let excluded = [Value::Text("İ".repeat(64))];
    let (lower_bytes, lower_steps) = lower_text_construction_allowance(128);
    for (key, coercion, bytes, steps, removed) in [
        (
            SemanticIndexKeyItem::Field("name".into()),
            CoercionId::Strict,
            0,
            0,
            excluded[0].clone(),
        ),
        (
            SemanticIndexKeyItem::Expression(SemanticIndexExpression::new(
                PersistedIndexExpressionOp::Lower,
                "name".into(),
            )),
            CoercionId::TextCasefold,
            lower_bytes,
            lower_steps,
            Value::Text("i\u{307}".repeat(64)),
        ),
    ] {
        let literals = [literal(&excluded, coercion)];
        let Value::Text(removed_text) = &removed else {
            unreachable!()
        };
        for width in [1_u64, 4, 16] {
            let mut input: Vec<_> = (1..width)
                .map(|i| Value::Text(format!("kept-{i}")))
                .collect();
            let expected = input.clone();
            input.push(removed.clone());
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, exact) in [
                    (Resource::TemporaryBytes, bytes),
                    (
                        Resource::PredicateExpressionSteps,
                        2 + width + steps + width * removed_text.len() as u64,
                    ),
                    (Resource::NestedValueSteps, 2 * width),
                ] {
                    for limit in [exact.saturating_sub(1), exact * 2] {
                        let root = request(resource, limit);
                        PreparationWork::run(&root.scope(), lane, |work| {
                            for attempt in 1..=3 {
                                let mut branches = input.clone();
                                let result = prune_branch_values_by_exclusions(
                                    key.as_ref(),
                                    &mut branches,
                                    &literals,
                                    work,
                                );
                                if exact == 0 || attempt * exact <= limit {
                                    result.unwrap();
                                    assert_eq!(branches, expected);
                                    assert_eq!(root.observed(resource), attempt * exact);
                                } else {
                                    assert!(
                                        QueryError::execute(result.unwrap_err())
                                            .diagnostic_facts()
                                            .contains(&(
                                                DiagnosticFactTag::BudgetResource,
                                                resource.raw()
                                            ))
                                    );
                                    break;
                                }
                            }
                            Ok(())
                        })
                        .unwrap();
                        assert_eq!(root.observed(Resource::RowsVisited), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn exclusion_sets_admit_visits_before_pruning() {
    let key = SemanticIndexKeyItem::Field("name".into());
    let values = [Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)];
    let literals = [literal(&values, CoercionId::Strict)];
    // One outer set plus three exclusions, each allowed two branch visits and
    // one literal visit. Removing both branches early does not refund admission.
    for limit in [9, 10] {
        let root = request(Resource::PredicateExpressionSteps, limit);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let mut branches = values[..2].to_vec();
            let result =
                prune_branch_values_by_exclusions(key.as_ref(), &mut branches, &literals, work);
            if limit == 10 {
                result.unwrap();
                assert!(branches.is_empty());
                assert_eq!(root.observed(Resource::PredicateExpressionSteps), 10);
            } else {
                assert!(
                    QueryError::execute(result.unwrap_err())
                        .diagnostic_facts()
                        .contains(&(
                            DiagnosticFactTag::BudgetResource,
                            Resource::PredicateExpressionSteps.raw()
                        ))
                );
                assert_eq!(branches, values[..2]);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        assert_eq!(
            root.observed(Resource::NestedValueSteps),
            if limit == 10 { 6 } else { 0 }
        );
    }
}

#[test]
fn exclusion_failure_stops_payload_comparisons_after_partial_compaction() {
    let key = SemanticIndexKeyItem::Field("name".into());
    let values = [Value::Text("drop".into())];
    let literals = [literal(&values, CoercionId::Strict)];
    let root = request(Resource::NestedValueSteps, 2);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        let mut branches = vec![
            values[0].clone(),
            Value::Text("keep".into()),
            values[0].clone(),
        ];
        let error = prune_branch_values_by_exclusions(key.as_ref(), &mut branches, &literals, work)
            .unwrap_err();
        assert!(QueryError::execute(error).diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::NestedValueSteps.raw()
        )));
        // The first removal is complete; the rejected and remaining entries
        // survive compaction. Callers discard the failed candidate.
        assert_eq!(
            branches,
            vec![Value::Text("keep".into()), values[0].clone()]
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::NestedValueSteps), 4);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn branch_lists_require_equal_sets_and_ignore_ineligible_exclusions() {
    let key = SemanticIndexKeyItem::Field("name".into());
    let values = [Value::Nat64(3), Value::Nat64(1), Value::Nat64(3)];
    let equal = [Value::Nat64(1), Value::Nat64(3)];
    let different = [Value::Nat64(1)];
    for (second, expected) in [
        (&equal[..], Some(equal.to_vec())),
        (&different[..], None),
        (&[][..], None),
    ] {
        let root = request(Resource::TemporaryBytes, 16_000_000);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let literals = [
                literal(&values, CoercionId::Strict),
                literal(second, CoercionId::Strict),
            ];
            assert_eq!(
                build_index_branch_values(key.as_ref(), &literals, work).unwrap(),
                expected
            );
            let mut invalid = literal(&different, CoercionId::Strict);
            invalid.values[0].compatible = false;
            assert!(
                build_index_branch_values(key.as_ref(), &[invalid], work)
                    .unwrap()
                    .is_none()
            );
            let mut invalid = literal(&different, CoercionId::Strict);
            invalid.values[0].compatible = false;
            let mut wrong_field = literal(&different, CoercionId::Strict);
            wrong_field.field = "other";
            let mut branches = equal.to_vec();
            prune_branch_values_by_exclusions(
                key.as_ref(),
                &mut branches,
                &[
                    invalid,
                    wrong_field,
                    literal(&different, CoercionId::TextCasefold),
                ],
                work,
            )
            .unwrap();
            assert_eq!(branches, equal);
            prune_branch_values_by_exclusions(
                key.as_ref(),
                &mut branches,
                &[literal(&values, CoercionId::Strict)],
                work,
            )
            .unwrap();
            assert!(branches.is_empty());
            Ok(())
        })
        .unwrap();
    }
}
