//! Selected-access proofs preserve membership without copying identity operands.

use super::{child_is_redundant_under_selected_index_access, key_item_guarantees_compare};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{AccessPath, AccessPlan, SemanticIndexKeyItemRef, SemanticIndexRangeSpec},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            plan::{
                VisibleIndexes, exact_metadata_schema,
                planner::compare::prefix_tests::schema as text_schema,
            },
            preparation::PreparationWork,
        },
    },
    value::{Value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::{borrow::Cow, ops::Bound};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn redundancy_membership_preserves_duplicates_empty_sets_and_incompatible_literals() {
    let schema = text_schema();
    let a = Value::Text("a".into());
    let b = Value::Text("b".into());
    let values = vec![b.clone(), a.clone(), a.clone(), Value::Int64(1)];
    for (op, value, expected, redundant) in [
        (CompareOp::Eq, a.clone(), vec![a.clone()], true),
        (CompareOp::Eq, a.clone(), vec![a.clone(), b.clone()], false),
        (CompareOp::Ne, a.clone(), vec![b.clone()], true),
        (CompareOp::Ne, Value::Int64(1), vec![b.clone()], false),
        (
            CompareOp::In,
            Value::List(values.clone()),
            vec![a.clone(), b.clone()],
            true,
        ),
        (
            CompareOp::NotIn,
            Value::List(values),
            vec![a.clone()],
            false,
        ),
        (CompareOp::In, Value::List(vec![]), vec![a.clone()], false),
        (CompareOp::NotIn, Value::List(vec![]), vec![a.clone()], true),
        (
            CompareOp::In,
            Value::List(vec![Value::Int64(1)]),
            vec![a.clone()],
            false,
        ),
        (
            CompareOp::NotIn,
            Value::List(vec![Value::Int64(1)]),
            vec![a.clone()],
            true,
        ),
        (CompareOp::In, a.clone(), vec![a.clone()], false),
        (CompareOp::In, Value::List(vec![]), vec![], true),
        (CompareOp::Lt, b, vec![a], false),
    ] {
        let cmp = ComparePredicate::with_coercion("name", op, value, CoercionId::Strict);
        let before = cmp.clone();
        let root = request(Resource::NestedValueSteps, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                key_item_guarantees_compare(
                    &schema,
                    SemanticIndexKeyItemRef::Field("name"),
                    &expected,
                    &cmp,
                    work,
                )
                .unwrap(),
                redundant,
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(cmp, before);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn redundancy_lookup_admits_conversion_and_list_backing_cumulatively() {
    let schema = text_schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let source = "İΣ".repeat(128);
    let (lower_bytes, lower_steps) = lower_text_construction_allowance(source.len());
    for expression in [false, true] {
        let index = indexes
            .iter()
            .find(|index| index.name() == if expression { "a_lower" } else { "a_raw" })
            .unwrap();
        let key = index.key_item_at(0).unwrap();
        let expected = [Value::Text(if expression {
            source.to_lowercase()
        } else {
            source.clone()
        })];
        for list in [false, true] {
            let input = Value::Text(source.clone());
            let cmp = ComparePredicate::with_coercion(
                "name",
                if list { CompareOp::In } else { CompareOp::Eq },
                if list {
                    Value::List(vec![input.clone(), input])
                } else {
                    input
                },
                if expression {
                    CoercionId::TextCasefold
                } else {
                    CoercionId::Strict
                },
            );
            let copies = if list { 2 } else { 1 };
            let bytes = if list {
                (2 * size_of::<Cow<'_, Value>>()) as u64
            } else {
                0
            } + if expression { copies * lower_bytes } else { 0 };
            let steps =
                if list { 5 } else { 2 } + if expression { copies * lower_steps } else { 0 };
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, exact) in [
                    (Resource::TemporaryBytes, bytes),
                    (Resource::PredicateExpressionSteps, steps),
                    (Resource::NestedValueSteps, 0),
                ] {
                    for limit in [0, exact.saturating_sub(1), exact * 2] {
                        let root = request(resource, limit);
                        PreparationWork::run(&root.scope(), lane, |work| {
                            for attempt in 1..=3 {
                                let result = key_item_guarantees_compare(
                                    &schema, key, &expected, &cmp, work,
                                );
                                if exact == 0 || attempt * exact <= limit {
                                    assert!(result.unwrap());
                                    assert_eq!(root.observed(resource), attempt * exact);
                                } else {
                                    assert!(
                                        QueryError::execute(result.unwrap_err())
                                            .diagnostic_facts()
                                            .contains(&(
                                                DiagnosticFactTag::BudgetResource,
                                                resource.raw(),
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
fn redundancy_dispatch_preserves_fixed_prefix_range_and_branch_proofs() {
    let schema = exact_metadata_schema(&[("a", &["age", "rank", "id"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let index = visible.accepted_semantic_index_contracts()[0].clone();
    let fixed = vec![Value::Int64(7)];
    let prefix = AccessPlan::index_prefix_from_contract(index.clone(), fixed.clone());
    let range = AccessPlan::Path(Box::new(AccessPath::IndexRange {
        spec: SemanticIndexRangeSpec::from_access_contract(
            index.clone(),
            vec![1, 2],
            fixed.clone(),
            Bound::Unbounded,
            Bound::Unbounded,
        ),
    }));
    let branch = AccessPlan::index_branch_set_from_contract(
        index,
        fixed,
        vec![Value::Int64(8), Value::Int64(9)],
    );
    for access in [&prefix, &range, &branch] {
        for (child, expected) in [
            (Predicate::eq("age".into(), Value::Int64(7)), true),
            (Predicate::eq("age".into(), Value::Int64(6)), false),
            (Predicate::eq("rank".into(), Value::Int64(8)), false),
            (Predicate::True, false),
            (
                Predicate::Compare(ComparePredicate::with_coercion(
                    "rank",
                    CompareOp::In,
                    Value::List(vec![Value::Int64(9), Value::Int64(8), Value::Int64(8)]),
                    CoercionId::Strict,
                )),
                access == &branch,
            ),
        ] {
            let before = child.clone();
            let root = request(Resource::NestedValueSteps, 0);
            PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                assert_eq!(
                    child_is_redundant_under_selected_index_access(
                        &schema,
                        Some(access),
                        &child,
                        work
                    )
                    .unwrap(),
                    expected
                );
                assert!(
                    !child_is_redundant_under_selected_index_access(&schema, None, &child, work)
                        .unwrap()
                );
                Ok(())
            })
            .unwrap();
            assert_eq!(child, before);
        }
    }
}
