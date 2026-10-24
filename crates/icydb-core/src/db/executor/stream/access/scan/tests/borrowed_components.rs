//! Covering scans retain only requested components and preserve scan contracts.

use super::*;
use crate::{
    db::{
        index::{
            EncodedValue, IndexKeyKind,
            predicate::{IndexCompareOp, IndexLiteral, IndexPredicateProgram},
        },
        key_taxonomy::CompositePrimaryKeyValue,
    },
    value::Value,
};

fn covering_keys(composite: bool) -> Vec<IndexKey> {
    (1..=5)
        .map(|id| {
            let primary = if composite {
                PrimaryKeyValue::Composite(
                    CompositePrimaryKeyValue::try_from_components(&[
                        PrimaryKeyComponent::Nat64(id),
                        PrimaryKeyComponent::Nat64(500 + id),
                    ])
                    .unwrap(),
                )
            } else {
                PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(id))
            };
            IndexKey::new_from_components_with_primary_key_value(
                &IndexId::new(EXACT_INTERSECTION_ENTITY, 1),
                IndexKeyKind::User,
                &[
                    EncodedValue::try_new(&Value::Nat64(id)).unwrap(),
                    EncodedValue::try_new(&Value::Text(format!("label_{id}"))).unwrap(),
                    EncodedValue::try_new(&Value::Text("unrequested".repeat(16))).unwrap(),
                ],
                &primary,
            )
            .unwrap()
        })
        .collect()
}

#[test]
fn covering_components_preserve_identity_selection_order_resume_and_limits() {
    let predicate = IndexPredicateProgram::Compare {
        component_index: 0,
        op: IndexCompareOp::Gte,
        literal: IndexLiteral::One(
            EncodedValue::try_new(&Value::Nat64(2))
                .unwrap()
                .into_bytes(),
        ),
    };
    for composite in [false, true] {
        reset_exact_intersection_stores();
        let keys = covering_keys(composite);
        let raw = keys
            .iter()
            .map(|key| key.to_raw().unwrap())
            .collect::<Vec<_>>();
        EXACT_INTERSECTION_INDEX.with_borrow_mut(|store| {
            for key in &raw {
                store.insert(key.clone(), IndexEntryValue::presence());
            }
        });
        for direction in [Direction::Asc, Direction::Desc] {
            for filtered in [false, true] {
                for resumed in [false, true] {
                    let mut expected = keys
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| {
                            (!filtered || *index >= 1)
                                && (!resumed
                                    || match direction {
                                        Direction::Asc => *index > 2,
                                        Direction::Desc => *index < 2,
                                    })
                        })
                        .map(|(_, key)| key)
                        .collect::<Vec<_>>();
                    if direction == Direction::Desc {
                        expected.reverse();
                    }
                    for limit in 0..=5 {
                        let (budget, context) = exact_intersection_budget();
                        let rows = with_query_execution_budget_for_tests(budget, context, || {
                            IndexScan::components_structural(
                                EXACT_INTERSECTION_STORE,
                                EXACT_INTERSECTION_ENTITY,
                                &Bound::Unbounded,
                                &Bound::Unbounded,
                                IndexScanContinuationInput::new(
                                    resumed.then_some(&raw[2]),
                                    direction,
                                ),
                                limit,
                                &[1, 0, 1],
                                filtered.then_some(&predicate),
                            )
                            .map_err(QueryError::execute)
                        })
                        .unwrap();
                        assert_eq!(rows.len(), limit.min(expected.len()));
                        for ((key, witness, components), expected) in
                            rows.iter().zip(expected.iter().take(limit))
                        {
                            assert_eq!(
                                key.primary_key_value(),
                                expected.primary_key_value().unwrap()
                            );
                            assert_eq!(
                                key.to_raw().unwrap(),
                                DecodedDataStoreKey::new_primary_key_value(
                                    EXACT_INTERSECTION_ENTITY,
                                    &expected.primary_key_value().unwrap()
                                )
                                .to_raw()
                                .unwrap()
                            );
                            assert_eq!(
                                *witness,
                                IndexEntryValue::presence()
                                    .decode_existence_witness()
                                    .unwrap()
                            );
                            assert_eq!(
                                components.as_ref(),
                                [1, 0, 1].map(|index| expected.component(index).unwrap().to_vec())
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn covering_components_preserve_corruption_and_refusal_order() {
    let key = nat64_index_key(&IndexId::new(EXACT_INTERSECTION_ENTITY, 1), b"lane", 7);
    let raw = key.to_raw().unwrap();
    let mut invalid_primary = raw.as_bytes().to_vec();
    let suffix = invalid_primary.len()
        - IndexKey::primary_key_value_and_bytes_from_raw(&raw)
            .unwrap()
            .1
            .len();
    invalid_primary[suffix] = 0xff;
    let invalid_primary = RawIndexStoreKey::from_persisted_bytes(invalid_primary);
    let mut trailing = raw.as_bytes().to_vec();
    trailing.push(0);
    let trailing = RawIndexStoreKey::from_persisted_bytes(trailing);
    let presence = IndexEntryValue::presence();
    let invalid_witness = IndexEntryValue::from_persisted_bytes(vec![2]);
    for (key, value, indices, expected) in [
        (
            &invalid_primary,
            &presence,
            &[0][..],
            InternalError::index_entry_decode_failed(),
        ),
        (
            &raw,
            &invalid_witness,
            &[0][..],
            InternalError::index_entry_decode_failed(),
        ),
        (
            &trailing,
            &presence,
            &[0][..],
            InternalError::index_corruption(),
        ),
        (&raw, &presence, &[1][..], InternalError::index_invariant()),
    ] {
        for predicate in [IndexPredicateProgram::False, IndexPredicateProgram::True] {
            let mut rows = Vec::new();
            let result = IndexScan::decode_index_entry_and_push_with_components(
                EXACT_INTERSECTION_ENTITY,
                key,
                value,
                &mut rows,
                Some(1),
                indices,
                Some(&predicate),
            );
            if key != &trailing && indices == [0] && predicate == IndexPredicateProgram::False {
                assert!(!result.unwrap());
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.class(), expected.class());
                assert_eq!(error.origin(), expected.origin());
                assert_eq!(error.diagnostic(), expected.diagnostic());
                assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
            }
            assert!(rows.is_empty());
        }
    }
}

#[test]
fn covering_components_keep_the_decoded_byte_budget_before_output() {
    let raw = covering_keys(false)[0].to_raw().unwrap();
    let (budget, context) = exact_intersection_budget();
    let budget = budget.with_limit_for_tests(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::DecodedBytes,
        0,
    );
    let mut rows = Vec::new();
    let error = with_query_execution_budget_for_tests(budget, context, || {
        IndexScan::decode_index_entry_and_push_with_components(
            EXACT_INTERSECTION_ENTITY,
            &raw,
            &IndexEntryValue::presence(),
            &mut rows,
            Some(1),
            &[0],
            None,
        )
        .map_err(QueryError::execute)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::DecodedBytes.raw(),
    )));
    assert!(rows.is_empty());
}

#[test]
fn covering_component_chunks_preserve_bounded_directional_progress() {
    let predicate = IndexPredicateProgram::Compare {
        component_index: 0,
        op: IndexCompareOp::Gte,
        literal: IndexLiteral::One(
            EncodedValue::try_new(&Value::Nat64(3))
                .unwrap()
                .into_bytes(),
        ),
    };
    for composite in [false, true] {
        reset_exact_intersection_stores();
        let keys = covering_keys(composite);
        let raw = keys
            .iter()
            .map(|key| key.to_raw().unwrap())
            .collect::<Vec<_>>();
        EXACT_INTERSECTION_INDEX.with_borrow_mut(|store| {
            for key in &raw {
                store.insert(key.clone(), IndexEntryValue::presence());
            }
        });
        let lower = Bound::Included(raw[0].clone());
        let upper = Bound::Included(raw[4].clone());
        for direction in [Direction::Asc, Direction::Desc] {
            for filtered in [false, true] {
                for max_entries in [1, 2, 5, 6] {
                    for output_limit in [None, Some(1), Some(3), Some(6)] {
                        let mut anchor = None;
                        let mut actual = Vec::new();
                        // Five keys need at most five nonempty chunks plus exhaustion.
                        for _ in 0..6 {
                            let chunk = IndexScan::components_chunk_structural(
                                EXACT_INTERSECTION_STORE,
                                EXACT_INTERSECTION_ENTITY,
                                &lower,
                                &upper,
                                IndexScanContinuationInput::new(anchor.as_ref(), direction),
                                max_entries,
                                output_limit,
                                &[0],
                                filtered.then_some(&predicate),
                            )
                            .unwrap();
                            let (rows, next) = chunk.into_component_rows_and_resume_anchor();
                            if !filtered {
                                let plain = IndexScan::chunk_structural(
                                    EXACT_INTERSECTION_STORE,
                                    EXACT_INTERSECTION_ENTITY,
                                    &lower,
                                    &upper,
                                    IndexScanContinuationInput::new(anchor.as_ref(), direction),
                                    max_entries,
                                    output_limit,
                                )
                                .unwrap();
                                let (plain_keys, plain_next) =
                                    plain.into_decoded_keys_and_resume_anchor();
                                assert_eq!(plain_next, next);
                                assert_eq!(
                                    plain_keys
                                        .iter()
                                        .map(DecodedDataStoreKey::primary_key_value)
                                        .collect::<Vec<_>>(),
                                    rows.iter()
                                        .map(|(key, _, _)| key.primary_key_value())
                                        .collect::<Vec<_>>()
                                );
                            }
                            actual.extend(
                                rows.into_iter().map(|(key, _, _)| key.primary_key_value()),
                            );
                            let Some(next) = next else {
                                break;
                            };
                            if let Some(previous) = &anchor {
                                assert!(match direction {
                                    Direction::Asc => previous < &next,
                                    Direction::Desc => previous > &next,
                                });
                            }
                            anchor = Some(next);
                        }
                        let mut expected = keys
                            .iter()
                            .skip(if filtered { 2 } else { 0 })
                            .map(|key| key.primary_key_value().unwrap())
                            .collect::<Vec<_>>();
                        if direction == Direction::Desc {
                            expected.reverse();
                        }
                        assert_eq!(actual, expected);
                    }
                }
            }
        }
    }
}
