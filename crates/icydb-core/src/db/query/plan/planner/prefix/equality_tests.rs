//! Shared equality-prefix construction preserves duplicates, gaps and conflicts.

use super::{CachedEqLiteral, build_index_eq_prefix};
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

#[test]
fn equality_prefix_duplicates_borrow_identity_and_admit_normalized_values() {
    let text = "İΣ".repeat(128);
    let value = Value::Text(text.clone());
    let (lower_bytes, lower_steps) = lower_text_construction_allowance(text.len());
    for repetitions in [1_u64, 4, 16] {
        for (key, coercion, copied, converted, steps, expected) in [
            (
                SemanticIndexKeyItem::Field("name".into()),
                CoercionId::Strict,
                1,
                text.len() as u64,
                text.len() as u64,
                value.clone(),
            ),
            (
                SemanticIndexKeyItem::Expression(SemanticIndexExpression::new(
                    PersistedIndexExpressionOp::Lower,
                    "name".into(),
                )),
                CoercionId::TextCasefold,
                0,
                repetitions * lower_bytes,
                repetitions * lower_steps,
                Value::Text(text.to_lowercase()),
            ),
        ] {
            let keys = [key, SemanticIndexKeyItem::Field("gap".into())];
            let literals: Vec<_> = (0..repetitions)
                .map(|_| CachedEqLiteral {
                    field: "name",
                    value: &value,
                    coercion,
                    compatible: true,
                })
                .collect();
            let bytes = (keys.len() * size_of::<Value>()) as u64 + converted;
            let steps = (keys.len() * (literals.len() + 1)) as u64 + steps;
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, exact) in [
                    (Resource::TemporaryBytes, bytes),
                    (Resource::PredicateExpressionSteps, steps),
                    (Resource::NestedValueSteps, copied),
                ] {
                    for limit in [0, exact.saturating_sub(1), exact * 2] {
                        let root = request(resource, limit);
                        PreparationWork::run(&root.scope(), lane, |work| {
                            for attempt in 0..3 {
                                let result = build_index_eq_prefix(&keys, &literals, work);
                                if exact == 0 || limit >= exact && attempt < 2 {
                                    assert_eq!(result.unwrap(), Some(vec![expected.clone()]));
                                    assert_eq!(root.observed(resource), exact * (attempt + 1));
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
                        assert_eq!(value, Value::Text(text.clone()));
                        assert_eq!(root.observed(Resource::RowsVisited), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn equality_prefix_matching_preserves_mixed_keys_gaps_and_conflicts() {
    let lower = SemanticIndexKeyItem::Expression(SemanticIndexExpression::new(
        PersistedIndexExpressionOp::Lower,
        "b".into(),
    ));
    let keys = [
        SemanticIndexKeyItem::Field("a".into()),
        lower,
        SemanticIndexKeyItem::Field("c".into()),
    ];
    let a = Value::Nat64(7);
    let b = Value::Text("İ".into());
    let normalized_b = Value::Text("i\u{307}".into());
    let conflict = Value::Text("different".into());
    let cases = [
        (
            vec![
                ("a", &a, CoercionId::Strict),
                ("b", &b, CoercionId::TextCasefold),
                ("b", &normalized_b, CoercionId::TextCasefold),
            ],
            Some(vec![a.clone(), normalized_b.clone()]),
        ),
        (
            vec![("a", &a, CoercionId::Strict), ("c", &a, CoercionId::Strict)],
            Some(vec![a.clone()]),
        ),
        (vec![("b", &b, CoercionId::TextCasefold)], Some(Vec::new())),
        (
            vec![
                ("a", &a, CoercionId::Strict),
                ("b", &b, CoercionId::TextCasefold),
                ("b", &conflict, CoercionId::TextCasefold),
            ],
            None,
        ),
    ];
    for (values, expected) in cases {
        let literals: Vec<_> = values
            .into_iter()
            .map(|(field, value, coercion)| CachedEqLiteral {
                field,
                value,
                coercion,
                compatible: true,
            })
            .collect();
        let root = request(Resource::TemporaryBytes, 16_000_000);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                build_index_eq_prefix(&keys, &literals, work).unwrap(),
                expected
            );
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn incompatible_equality_literals_leave_an_empty_prefix_without_copying() {
    let value = Value::Text("not accepted".into());
    let keys = [SemanticIndexKeyItem::Field("name".into())];
    for (compatible, coercion) in [
        (false, CoercionId::Strict),
        (true, CoercionId::TextCasefold),
    ] {
        let literals = [CachedEqLiteral {
            field: "name",
            value: &value,
            coercion,
            compatible,
        }];
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                build_index_eq_prefix(&keys, &literals, work).unwrap(),
                Some(Vec::new())
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    }
}
